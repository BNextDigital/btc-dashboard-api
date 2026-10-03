"""Four-region M2 composite, in USD billions at observation-month FX rates.

Official sources: FRED M2SL (USD billions, SA), ECB BSI (EUR millions,
SA), PBoC financial reports (CNY trillions, NSA), and BOJ MD02 (100m JPY,
monthly average, NSA). Definitions differ, so this is a four-region proxy,
not a harmonised measure of the entire world's money supply.
"""
from __future__ import annotations

import csv
import html
import io
import json
import logging
import math
import os
import re
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from shared.fred_cache import get_series

LOG = logging.getLogger(__name__)
ECB_BASE = "https://data-api.ecb.europa.eu/service/data"
ECB_M2 = "M.U2.Y.V.M20.X.1.U2.2300.Z01.E"
BOJ_CODE = "MAM1NAM2M2MO"
PBOC_INDEX = "https://www.pbc.gov.cn/en/3688247/3688978/3709137/index.html"
REGIONS = {
    "us": {"source": "FRED / Federal Reserve", "series": "M2SL", "native_unit": "USD billions", "currency": "USD", "scale_to_billions": 1, "adjustment": "seasonally adjusted", "basis": "monthly average"},
    "eurozone": {"source": "European Central Bank", "series": f"BSI.{ECB_M2}", "native_unit": "EUR millions", "currency": "EUR", "scale_to_billions": .001, "adjustment": "seasonally and working-day adjusted", "basis": "month-end stock"},
    "china": {"source": "People's Bank of China", "series": "Financial Statistics Report / M2", "native_unit": "CNY trillions", "currency": "CNY", "scale_to_billions": 1000, "adjustment": "not seasonally adjusted", "basis": "month-end stock"},
    "japan": {"source": "Bank of Japan", "series": f"MD02/{BOJ_CODE}", "native_unit": "100 million JPY", "currency": "JPY", "scale_to_billions": .1, "adjustment": "not seasonally adjusted", "basis": "monthly average"},
}
SOURCE_URLS = {
    "us": "https://fred.stlouisfed.org/series/M2SL",
    "eurozone": f"https://data.ecb.europa.eu/data/datasets/BSI/BSI.{ECB_M2}",
    "china": PBOC_INDEX,
    "japan": "https://www.stat-search.boj.or.jp/ssi/mtshtml/mam1nam2m2mo.html",
}
CACHE_VERSION = 1
CACHE_TTL = 12 * 3600
_LOCK = threading.Lock()


def month_shift(period: str, delta: int) -> str:
    year, month = map(int, period.split("-"))
    n = year * 12 + month - 1 + delta
    return f"{n // 12:04d}-{n % 12 + 1:02d}"


def _period(value) -> str:
    text = str(value)
    if re.fullmatch(r"\d{6}", text):
        text = text[:4] + "-" + text[4:]
    period = text[:7]
    datetime.strptime(period, "%Y-%m")
    return period


def _positive(value) -> float:
    if isinstance(value, bool):
        raise ValueError("boolean observation")
    n = float(value)
    if not math.isfinite(n) or n <= 0:
        raise ValueError("observation must be positive and finite")
    return n


def _get(url, **kwargs):
    r = requests.get(url, timeout=20, **kwargs)
    r.raise_for_status()
    r.encoding = "utf-8"
    return r


def parse_ecb_csv(text: str, *, fx=False) -> dict:
    rows = list(csv.DictReader(io.StringIO(text.lstrip("\ufeff"))))
    if not rows:
        raise ValueError("empty ECB response")
    result = {}
    for row in rows:
        if row.get("OBS_STATUS") in {"M", "L"} or not row.get("OBS_VALUE"):
            continue
        period = _period(row["TIME_PERIOD"])
        value = _positive(row["OBS_VALUE"])
        if fx:
            if row.get("CURRENCY_DENOM") != "EUR" or row.get("CURRENCY") not in {"USD", "CNY", "JPY"}:
                raise ValueError("unexpected ECB FX currency")
            if row.get("UNIT_MULT", "0") != "0":
                raise ValueError("unexpected ECB FX scale")
            result.setdefault(period, {})[row["CURRENCY"]] = value
        else:
            if row.get("KEY") != f"BSI.{ECB_M2}" or row.get("UNIT") != "EUR" or row.get("UNIT_MULT") != "6":
                raise ValueError("unexpected ECB M2 series/units")
            result[period] = {"native_value": value}
    if not result:
        raise ValueError("no ECB observations")
    return result


def parse_boj(payload: dict) -> dict:
    if payload.get("STATUS") != 200 or payload.get("NEXTPOSITION"):
        raise ValueError("BOJ error or incomplete response")
    series = next((s for s in payload.get("RESULTSET", []) if s.get("SERIES_CODE") == BOJ_CODE), None)
    if not series or series.get("UNIT", "").lower() != "100 million yen" or series.get("FREQUENCY") != "MONTHLY":
        raise ValueError("unexpected BOJ series/units")
    values = series["VALUES"]
    dates, numbers = values["SURVEY_DATES"], values["VALUES"]
    if len(dates) != len(numbers):
        raise ValueError("BOJ observation lengths differ")
    return {_period(d): {"native_value": _positive(v)} for d, v in zip(dates, numbers) if v is not None}


class _Page(HTMLParser):
    """Extract links and visible text without executing the site's scripts."""
    def __init__(self, text):
        super().__init__(convert_charrefs=True)
        self.links, self.parts, self._hidden = [], [], 0
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag in {"script", "style"}:
            self._hidden += 1
        if tag == "a" and attrs.get("href"):
            self.links.append((attrs["href"], attrs.get("title", "")))
        # PBoC pagination uses onclick/tagname instead of href.
        if attrs.get("tagname"):
            self.links.append((attrs["tagname"], "pagination"))

    def handle_endtag(self, tag):
        if tag in {"script", "style"} and self._hidden:
            self._hidden -= 1

    def handle_data(self, data):
        if not self._hidden:
            self.parts.append(data)

    @property
    def text(self):
        return " ".join(html.unescape(" ".join(self.parts)).split())


MONTHS = {name.lower(): i for i, name in enumerate(
    ("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"), 1)}


def pboc_period(title: str) -> str:
    title = " ".join(html.unescape(title).split())
    m = re.search(r"Financial Statistics Report\s*\(([^)]+)\)", title, re.I)
    if not m:
        raise ValueError("missing PBoC report title")
    label = m[1]
    year = int(re.search(r"\b(20\d{2})\b", label)[1])
    first = label.split()[0].lower()
    month = MONTHS.get(first) or {"q1": 3, "h1": 6, "q1-q3": 9}.get(first)
    if month is None and label == str(year):
        month = 12
    if month is None:
        raise ValueError(f"unknown PBoC reporting period: {label}")
    return f"{year:04d}-{month:02d}"


def parse_pboc_report(text: str, expected_period: str) -> dict:
    visible = _Page(text).text
    if pboc_period(visible) != expected_period:
        raise ValueError("PBoC report period does not match listing")
    match = re.search(r"broad money supply\s*\(M2\).*?RMB\s*([\d,.]+)\s*trillion", visible, re.I)
    if not match:
        raise ValueError("PBoC M2 level in RMB trillions missing")
    return {"native_value": _positive(match[1].replace(",", ""))}


def _pboc_url(base, path):
    url = urljoin(base, path)
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != "www.pbc.gov.cn" or not parsed.path.startswith("/en/3688247/3688978/3709137/"):
        raise ValueError("unexpected PBoC report URL")
    return url


def fetch_pboc(start: str, previous: dict | None = None) -> dict:
    observations = dict(previous or {})
    listing = PBOC_INDEX
    reports = {}
    visited = set()
    # Three listing pages cover > two years (10 reports/page).
    for _ in range(3):
        if listing in visited:
            break
        visited.add(listing)
        page = _Page(_get(listing).text)
        oldest = None
        for path, title in page.links:
            if "Financial Statistics Report (" not in title:
                continue
            period = pboc_period(title)
            oldest = min(oldest or period, period)
            if period >= start:
                reports[period] = _pboc_url(listing, path)
        if oldest and oldest <= start:
            break
        next_pages = sorted({_pboc_url(listing, p) for p, t in page.links if t == "pagination" and re.search(r"-\d+\.html$", p)}, key=lambda u: int(re.search(r"-(\d+)\.html$", u)[1]))
        listing = next((u for u in next_pages if u not in visited), None)
        if listing is None:
            break
    # Recheck the latest two reports for revisions; older successes are durable.
    refresh = set(sorted(reports)[-2:])
    tasks = [(p, u) for p, u in reports.items() if p not in observations or p in refresh]

    def download(item):
        period, url = item
        try:
            observation = parse_pboc_report(_get(url).text, period)
            return period, {**observation, "source_url": url}
        except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
            LOG.warning("PBoC M2 report %s unavailable: %s", period, exc)
            return period, None

    failures = []
    with ThreadPoolExecutor(max_workers=3) as pool:
        for period, observation in pool.map(download, tasks):
            if observation:
                observations[period] = observation
            else:
                failures.append(period)
    for observation in observations.values():
        observation.pop("source_refresh_warning", None)
        if failures:
            observation["source_refresh_warning"] = "PBoC reports unavailable: " + ", ".join(sorted(failures))
    if not observations:
        raise ValueError("no PBoC M2 observations")
    return observations


def _fetch_us(start):
    pairs = get_series("M2SL", n_obs=36)
    if not pairs:
        raise ValueError("FRED M2SL unavailable — check FRED_API_KEY and shared cache")
    return {_period(d): {"native_value": _positive(v)} for d, v in pairs if _period(d) >= start}


def _cached_source(key, fetcher, cache_dir, start):
    path = cache_dir / f"{key}.json"
    previous = {}
    try:
        saved = json.loads(path.read_text())
        if (isinstance(saved, dict) and saved.get("version") == CACHE_VERSION
                and isinstance(saved.get("fetched_at"), (int, float))
                and isinstance(saved.get("observations"), dict)):
            previous = saved
    except (OSError, ValueError, TypeError):
        pass
    if previous and time.time() - previous["fetched_at"] < CACHE_TTL:
        return previous["observations"], {"cache_fallback": False, "fetched_at": previous["fetched_at"]}
    try:
        observations = fetcher(previous.get("observations", {}))
        if not observations:
            raise ValueError("empty source response")
        # Preserve successful historical observations through upstream gaps.
        merged = {**previous.get("observations", {}), **observations}
        observations = {p: v for p, v in merged.items() if p >= start}
        fetched_at = time.time()
        document = {"version": CACHE_VERSION, "fetched_at": fetched_at, "observations": observations}
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(mode="w", dir=cache_dir, delete=False) as f:
                json.dump(document, f, allow_nan=False)
                temp_path = Path(f.name)
            temp_path.replace(path)
        except OSError as exc:
            LOG.warning("M2 cache write failed for %s: %s", key, exc)
        return observations, {"cache_fallback": False, "fetched_at": fetched_at}
    except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
        LOG.warning("M2 source %s failed: %s", key, exc)
        return previous.get("observations", {}), {"cache_fallback": bool(previous), "fetched_at": previous.get("fetched_at"), "error": str(exc)}


def aggregate(series: dict, fx: dict, source_status=None, *, today=None) -> dict:
    today = today or date.today()
    current = today.strftime("%Y-%m")
    status = source_status or {}
    # Require the same *completed* month for all four M2 and FX series.
    common = set(fx)
    for region in REGIONS:
        common &= set(series.get(region, {}))
    common = sorted(p for p in common if p < current and {"USD", "CNY", "JPY"} <= set(fx[p]))
    as_of = common[-1] if common else None
    age = ((today.year * 12 + today.month) - (int(as_of[:4]) * 12 + int(as_of[5:]))) if as_of else None
    usable = as_of is not None and age <= 3
    components, totals = {}, {}

    def convert(region, period):
        meta = REGIONS[region]
        rates = fx[period]
        currency = meta["currency"]
        usd_per_unit = 1 if currency == "USD" else rates["USD"] if currency == "EUR" else rates["USD"] / rates[currency]
        return _positive(series[region][period]["native_value"]) * meta["scale_to_billions"] * _positive(usd_per_unit)

    for period in common:
        totals[period] = sum(convert(r, period) for r in REGIONS)
    for region, meta in REGIONS.items():
        observations = series.get(region, {})
        latest = max((p for p in observations if p < current), default=None)
        observation = observations.get(as_of, {}) if as_of else {}
        components[region] = {k: v for k, v in meta.items() if k != "scale_to_billions"}
        components[region].update({"period": as_of if observation else None, "latest_period": latest, "native_value": observation.get("native_value"), "usd_billions": round(convert(region, as_of), 3) if usable else None, "source_url": observation.get("source_url") or SOURCE_URLS[region], "source_refresh_warning": observation.get("source_refresh_warning"), **status.get(region, {})})
    total = totals.get(as_of) if usable else None

    def growth(delta):
        previous = totals.get(month_shift(as_of, delta)) if usable else None
        return round((total / previous - 1) * 100, 6) if previous is not None else None

    mom, yoy = growth(-1), growth(-12)
    warnings = [f"{k}: {s['error']}" for k, s in status.items() if s.get("error")]
    warnings.extend(c["source_refresh_warning"] for c in components.values() if c.get("source_refresh_warning"))
    if not as_of:
        warnings.append("No common month across all four M2 regions and FX rates")
    elif not usable:
        warnings.append(f"Common observation month {as_of} is stale ({age} months old)")
    if usable and (mom is None or yoy is None):
        warnings.append("Exact previous-month or previous-year composite is unavailable")
    return {
        "global_m2_bil": round(total, 3) if total is not None else None,
        **{f"{r}_m2_bil": components[r]["usd_billions"] for r in REGIONS},
        "as_of": as_of, "last_date": as_of + "-01" if as_of else None,
        "mom_growth_pct": mom, "yoy_growth_pct": yoy, "components": components,
        "fx": {"source": "European Central Bank", "series": "EXR.M.USD+CNY+JPY.EUR.SP00.A", "method": "observation-month average, currencies per EUR", "period": as_of, "rates": fx.get(as_of), **status.get("fx", {})},
        "data_quality": {"status": "unavailable" if not as_of else "stale" if not usable else "degraded" if warnings else "good", "common_period": as_of, "observation_age_months": age, "regions_available": sum(bool(series.get(r)) for r in REGIONS), "regions_expected": 4, "warnings": warnings},
        "calculation_version": "2.0.0",
        "methodology": "Four-region M2 proxy (US, eurozone, China, Japan); mixed seasonal adjustment and stock conventions; monthly-average FX; growth includes FX valuation changes. Other regions are excluded.",
    }


def fetch_global_m2() -> dict:
    start = month_shift(date.today().strftime("%Y-%m"), -25)
    cache_dir = Path(os.getenv("DATA_DIR", "./data")) / "global_m2_sources"
    fetchers = {
        "us": lambda old: _fetch_us(start),
        "eurozone": lambda old: parse_ecb_csv(_get(f"{ECB_BASE}/BSI/{ECB_M2}", params={"format": "csvdata", "startPeriod": start}).text),
        "japan": lambda old: parse_boj(_get("https://www.stat-search.boj.or.jp/api/v1/getDataCode", params={"format": "json", "lang": "en", "db": "MD02", "code": BOJ_CODE, "startDate": start.replace("-", "")}).json()),
        "china": lambda old: fetch_pboc(start, old),
        "fx": lambda old: parse_ecb_csv(_get(f"{ECB_BASE}/EXR/M.USD+CNY+JPY.EUR.SP00.A", params={"format": "csvdata", "startPeriod": start}).text, fx=True),
    }
    # Serialise refreshes within a collector process; independent sources run concurrently.
    with _LOCK, ThreadPoolExecutor(max_workers=5) as pool:
        keys = list(fetchers)
        results = list(pool.map(lambda k: _cached_source(k, fetchers[k], cache_dir, start), keys))
    observations = {k: result[0] for k, result in zip(keys, results)}
    statuses = {k: result[1] for k, result in zip(keys, results)}
    fx = observations.pop("fx")
    return aggregate(observations, fx, statuses)
