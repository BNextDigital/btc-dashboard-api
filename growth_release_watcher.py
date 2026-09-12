"""
growth_release_watcher.py — release-aware Growth / Inflation refresh coordinator.

Runs inside the lightweight snapshot_api process and deliberately avoids
pandas, NumPy, yFinance, requests, and the heavy analytics app.

What it does:
- Uses FRED's own release calendar to learn the next release DATE for each
  high-value growth/inflation family.
- Uses FRED series metadata `last_updated` only inside the small release window
  to detect when the new observations have actually landed on FRED.
- Triggers the disposable `collector.py growth` mode when fresh data arrives.
- Runs one weekday end-of-day refresh for daily/weekly context such as
  breakevens and energy.
- Persists schedule/event state in DATA_DIR so Railway redeploys do not lose
  release awareness or repeatedly fire the same event.

The release DATE comes from FRED dynamically. The release CLOCK TIME is kept
here because FRED release dates do not guarantee the time data becomes
available. These defaults reflect the normal source release times and can be
overridden via GROWTH_RELEASE_TIME_OVERRIDES_JSON.

This module does NOT fetch the observations themselves; the existing Growth
route remains the single source of truth for the actual dashboard payload.
"""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


FRED_API_KEY = os.getenv("FRED_API_KEY", "")
FRED_API_BASE = "https://api.stlouisfed.org/fred"
DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

STATE_PATH = DATA_DIR / "growth_release_watch.json"
ET = ZoneInfo("America/New_York")

WATCH_POLL_SECONDS = max(
    30,
    int(os.getenv("GROWTH_RELEASE_WATCH_POLL_SECONDS", "60")),
)
WINDOW_BEFORE_MINUTES = max(
    0,
    int(os.getenv("GROWTH_RELEASE_WINDOW_BEFORE_MINUTES", "2")),
)
WINDOW_AFTER_MINUTES = max(
    10,
    int(os.getenv("GROWTH_RELEASE_WINDOW_AFTER_MINUTES", "30")),
)
FIRST_RUN_SETTLE_MINUTES = max(
    1,
    int(os.getenv("GROWTH_RELEASE_SETTLE_MINUTES", "3")),
)

DAILY_REFRESH_HOUR_ET = int(
    os.getenv("GROWTH_DAILY_REFRESH_HOUR_ET", "17")
)
DAILY_REFRESH_MINUTE_ET = int(
    os.getenv("GROWTH_DAILY_REFRESH_MINUTE_ET", "30")
)

SCHEDULE_LOOKAHEAD_DAYS = max(
    30,
    int(os.getenv("GROWTH_RELEASE_LOOKAHEAD_DAYS", "75")),
)


DEFAULT_GROUPS: dict[str, dict[str, Any]] = {
    # CPI release also carries rent / OER components used on the Growth tab.
    "cpi": {
        "label": "CPI / Core CPI",
        "anchor": "CPIAUCSL",
        "series": [
            "CPIAUCSL",
            "CPILFESL",
            "CUSR0000SEHA",
            "CUSR0000SEHC",
        ],
        "time_et": "08:30",
    },
    "ppi": {
        "label": "PPI",
        "anchor": "PPIFID",
        "series": ["PPIFID"],
        "time_et": "08:30",
    },
    "pce": {
        "label": "PCE / Core PCE",
        "anchor": "PCEPI",
        "series": ["PCEPI", "PCEPILFE"],
        "time_et": "08:30",
    },
    "employment": {
        "label": "Payrolls / Unemployment / Wages",
        "anchor": "PAYEMS",
        "series": ["PAYEMS", "UNRATE", "CES0500000003"],
        "time_et": "08:30",
    },
    "claims": {
        "label": "Initial / Continuing Claims",
        "anchor": "ICSA",
        "series": ["ICSA", "CCSA"],
        "time_et": "08:30",
    },
    "jolts": {
        "label": "JOLTS",
        "anchor": "JTSJOL",
        "series": ["JTSJOL"],
        "time_et": "10:00",
    },
    "gdp": {
        "label": "GDP",
        "anchor": "A191RL1Q225SBEA",
        "series": ["A191RL1Q225SBEA"],
        "time_et": "08:30",
    },
    "retail": {
        "label": "Retail Sales",
        "anchor": "RSAFS",
        "series": ["RSAFS"],
        "time_et": "08:30",
    },
    "sentiment": {
        "label": "Michigan Sentiment / Inflation Expectations",
        "anchor": "UMCSENT",
        "series": ["UMCSENT", "MICH"],
        "time_et": "10:00",
    },
    
}


def _time_overrides() -> dict[str, str]:
    raw = os.getenv("GROWTH_RELEASE_TIME_OVERRIDES_JSON", "").strip()
    if not raw:
        return {}

    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return {
                str(key): str(value)
                for key, value in parsed.items()
                if isinstance(value, str)
            }
    except Exception as exc:
        print(
            "[growth-watch] invalid "
            f"GROWTH_RELEASE_TIME_OVERRIDES_JSON: {exc}"
        )

    return {}


def _groups() -> dict[str, dict[str, Any]]:
    overrides = _time_overrides()
    result: dict[str, dict[str, Any]] = {}

    for key, config in DEFAULT_GROUPS.items():
        entry = dict(config)
        entry["series"] = list(config["series"])
        if key in overrides:
            entry["time_et"] = overrides[key]
        result[key] = entry

    return result


GROUPS = _groups()


def _load_state() -> dict[str, Any]:
    try:
        if STATE_PATH.exists():
            raw = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return raw
    except Exception as exc:
        print(f"[growth-watch] state read failed: {exc}")

    return {
        "schema_version": 1,
        "schedule_refreshed_date": None,
        "groups": {},
        "events": {},
        "daily_refresh_date": None,
        "last_trigger": None,
    }


def _save_state(state: dict[str, Any]) -> None:
    try:
        tmp = STATE_PATH.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(
                state,
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        tmp.replace(STATE_PATH)
    except Exception as exc:
        print(f"[growth-watch] state write failed: {exc}")


def _parse_iso(value: Any) -> Optional[datetime]:
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(
            str(value).strip().replace("Z", "+00:00")
        )
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None


def _parse_hhmm(value: str) -> dt_time:
    try:
        hour, minute = value.split(":", 1)
        return dt_time(
            hour=int(hour),
            minute=int(minute),
            tzinfo=ET,
        )
    except Exception:
        return dt_time(hour=8, minute=30, tzinfo=ET)


def _event_key(group_key: str, release_date: str) -> str:
    return f"{group_key}:{release_date}"


class GrowthReleaseWatcher:
    def __init__(
        self,
        *,
        run_growth_collector: Callable[[], bool],
        get_growth_snapshot: Callable[[], Any],
        stop_event: threading.Event,
    ) -> None:
        self._run_growth_collector = run_growth_collector
        self._get_growth_snapshot = get_growth_snapshot
        self._stop_event = stop_event

        self._state_lock = threading.Lock()
        self._state = _load_state()

        self._thread: Optional[threading.Thread] = None
        self._last_network_error: Optional[str] = None
        self._watching_group: Optional[str] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name="growth-release-watcher",
        )
        self._thread.start()

    def status(self) -> dict[str, Any]:
        with self._state_lock:
            state = json.loads(json.dumps(self._state))

        now_et = datetime.now(ET)
        upcoming = []

        for group_key, config in GROUPS.items():
            group_state = state.get("groups", {}).get(group_key, {})
            release_date = group_state.get("next_release_date")
            if not release_date:
                continue

            release_at = self._release_datetime(
                release_date,
                config["time_et"],
            )
            event = state.get("events", {}).get(
                _event_key(group_key, release_date),
                {},
            )

            if (
                release_at >= now_et - timedelta(
                    minutes=WINDOW_AFTER_MINUTES
                )
                and not event.get("complete")
            ):
                upcoming.append(
                    {
                        "key": group_key,
                        "label": config["label"],
                        "release_at": release_at.isoformat(),
                        "release_date": release_date,
                        "time_et": config["time_et"],
                        "runs": int(event.get("runs", 0) or 0),
                    }
                )

        upcoming.sort(key=lambda row: row["release_at"])

        return {
            "enabled": bool(FRED_API_KEY),
            "mode": "release_aware",
            "watching": self._watching_group is not None,
            "watching_group": self._watching_group,
            "next_release": upcoming[0] if upcoming else None,
            "upcoming": upcoming[:8],
            "daily_refresh_time_et": (
                f"{DAILY_REFRESH_HOUR_ET:02d}:"
                f"{DAILY_REFRESH_MINUTE_ET:02d}"
            ),
            "daily_refresh_date": state.get(
                "daily_refresh_date"
            ),
            "schedule_refreshed_date": state.get(
                "schedule_refreshed_date"
            ),
            "last_trigger": state.get("last_trigger"),
            "last_network_error": self._last_network_error,
            "poll_seconds_during_release_window": WATCH_POLL_SECONDS,
            "state_path": str(STATE_PATH),
        }

    def _fred_json(
        self,
        endpoint: str,
        params: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        if not FRED_API_KEY:
            return None

        query = {
            **params,
            "api_key": FRED_API_KEY,
            "file_type": "json",
        }

        url = (
            f"{FRED_API_BASE}/{endpoint}?"
            + urlencode(query)
        )
        request = Request(
            url,
            headers={
                "User-Agent": (
                    "BNextDigital-BTC-Dashboard/"
                    "growth-release-watcher"
                )
            },
        )

        try:
            with urlopen(request, timeout=12) as response:
                payload = json.loads(
                    response.read().decode("utf-8")
                )
            self._last_network_error = None
            return payload if isinstance(payload, dict) else None

        except Exception as exc:
            self._last_network_error = (
                f"{type(exc).__name__}: {exc}"
            )
            print(
                "[growth-watch] FRED "
                f"{endpoint} failed: {self._last_network_error}"
            )
            return None

    def _release_id(
        self,
        group_key: str,
        anchor: str,
    ) -> Optional[int]:
        with self._state_lock:
            existing = (
                self._state
                .get("groups", {})
                .get(group_key, {})
                .get("release_id")
            )

        if isinstance(existing, int):
            return existing

        payload = self._fred_json(
            "series/release",
            {"series_id": anchor},
        )
        releases = (
            payload.get("releases", [])
            if isinstance(payload, dict)
            else []
        )

        if not releases:
            return None

        try:
            release_id = int(releases[0]["id"])
        except Exception:
            return None

        with self._state_lock:
            groups = self._state.setdefault("groups", {})
            group_state = groups.setdefault(group_key, {})
            group_state["release_id"] = release_id
            _save_state(self._state)

        return release_id

    def _next_release_date(
        self,
        release_id: int,
        today_et: date,
    ) -> Optional[str]:
        end_date = (
            today_et + timedelta(days=SCHEDULE_LOOKAHEAD_DAYS)
        )

        payload = self._fred_json(
            "release/dates",
            {
                "release_id": release_id,
                "realtime_start": today_et.isoformat(),
                "realtime_end": end_date.isoformat(),
                "include_release_dates_with_no_data": "true",
                "sort_order": "asc",
                "limit": 100,
            },
        )

        rows = (
            payload.get("release_dates", [])
            if isinstance(payload, dict)
            else []
        )

        for row in rows:
            if not isinstance(row, dict):
                continue

            value = row.get("date") or row.get(
                "release_date"
            )
            if not value:
                continue

            try:
                candidate = date.fromisoformat(str(value))
            except Exception:
                continue

            if candidate >= today_et:
                return candidate.isoformat()

        return None

    def _refresh_schedule(self, now_et: datetime) -> None:
        today_str = now_et.date().isoformat()

        with self._state_lock:
            if (
                self._state.get("schedule_refreshed_date")
                == today_str
            ):
                return

        print("[growth-watch] refreshing FRED release calendar")

        any_success = False

        for group_key, config in GROUPS.items():
            release_id = self._release_id(
                group_key,
                config["anchor"],
            )

            if release_id is None:
                continue

            next_date = self._next_release_date(
                release_id,
                now_et.date(),
            )

            if next_date is None:
                continue

            with self._state_lock:
                groups = self._state.setdefault("groups", {})
                group_state = groups.setdefault(
                    group_key,
                    {},
                )
                group_state.update(
                    {
                        "label": config["label"],
                        "anchor": config["anchor"],
                        "release_id": release_id,
                        "next_release_date": next_date,
                        "time_et": config["time_et"],
                    }
                )

            any_success = True

        if any_success:
            with self._state_lock:
                self._state[
                    "schedule_refreshed_date"
                ] = today_str
                _save_state(self._state)

    def _release_datetime(
        self,
        release_date: str,
        time_et: str,
    ) -> datetime:
        release_day = date.fromisoformat(release_date)
        clock = _parse_hhmm(time_et)

        return datetime(
            release_day.year,
            release_day.month,
            release_day.day,
            clock.hour,
            clock.minute,
            tzinfo=ET,
        )

    def _series_last_updated(
        self,
        series_id: str,
    ) -> Optional[datetime]:
        payload = self._fred_json(
            "series",
            {"series_id": series_id},
        )
        rows = (
            payload.get("seriess", [])
            if isinstance(payload, dict)
            else []
        )

        if not rows:
            return None

        return _parse_iso(rows[0].get("last_updated"))

    def _snapshot_updated_at(self) -> datetime:
        try:
            payload = self._get_growth_snapshot()
            if isinstance(payload, dict):
                parsed = _parse_iso(
                    payload.get("updated_at")
                )
                if parsed is not None:
                    return parsed.astimezone(timezone.utc)
        except Exception as exc:
            print(
                "[growth-watch] growth snapshot read "
                f"failed: {exc}"
            )

        return datetime.fromtimestamp(
            0,
            tz=timezone.utc,
        )

    def _check_release_group(
        self,
        group_key: str,
        config: dict[str, Any],
        release_date: str,
        now_et: datetime,
    ) -> None:
        release_at = self._release_datetime(
            release_date,
            config["time_et"],
        )
        window_start = release_at - timedelta(
            minutes=WINDOW_BEFORE_MINUTES
        )
        window_end = release_at + timedelta(
            minutes=WINDOW_AFTER_MINUTES
        )

        if now_et < window_start or now_et > window_end:
            return

        event_key = _event_key(
            group_key,
            release_date,
        )

        with self._state_lock:
            events = self._state.setdefault("events", {})
            event = events.setdefault(
                event_key,
                {
                    "group": group_key,
                    "release_date": release_date,
                    "runs": 0,
                    "complete": False,
                },
            )

            if event.get("complete"):
                return

            last_poll = float(
                event.get("last_poll_unix", 0) or 0
            )

        if time.time() - last_poll < WATCH_POLL_SECONDS:
            return

        with self._state_lock:
            event["last_poll_unix"] = time.time()
            _save_state(self._state)

        self._watching_group = group_key

        snapshot_time = self._snapshot_updated_at()
        updates: dict[str, str] = {}
        changed: list[str] = []

        for series_id in config["series"]:
            updated = self._series_last_updated(series_id)
            if updated is None:
                continue

            updates[series_id] = updated.isoformat()

            if updated.astimezone(timezone.utc) > snapshot_time:
                changed.append(series_id)

        with self._state_lock:
            event["series_last_updated"] = updates
            event["changed_since_snapshot"] = changed
            event["snapshot_time_checked"] = (
                snapshot_time.isoformat()
            )
            runs = int(event.get("runs", 0) or 0)
            _save_state(self._state)

        if not changed:
            if now_et >= window_end:
                with self._state_lock:
                    event["complete"] = True
                    event["completion_reason"] = (
                        "release_window_expired_no_change"
                    )
                    _save_state(self._state)
            return

        all_changed = (
            len(changed) == len(config["series"])
        )

        # On the first pass, allow a brief settle period so grouped
        # releases (e.g. CPI + core + rent/OER) can land together.
        first_run_ready = (
            runs == 0
            and (
                all_changed
                or now_et
                >= release_at
                + timedelta(
                    minutes=FIRST_RUN_SETTLE_MINUTES
                )
            )
        )

        # After one partial refresh, any newly-late series is enough
        # to justify one final collector run.
        followup_ready = runs >= 1

        if not (first_run_ready or followup_ready):
            return

        reason = (
            f"release:{group_key}:"
            + ",".join(changed)
        )

        print(
            "[growth-watch] triggering growth collector "
            f"for {reason}"
        )
        success = self._run_growth_collector()

        if not success:
            return

        with self._state_lock:
            event["runs"] = runs + 1
            event["last_triggered_at"] = (
                datetime.now(timezone.utc).isoformat()
            )
            event["last_changed_series"] = changed

            # Complete after all expected series were seen in the
            # same collector window, or after a second collector pass.
            if all_changed or event["runs"] >= 2:
                event["complete"] = True
                event["completion_reason"] = (
                    "all_series_updated"
                    if all_changed
                    else "max_followup_runs_reached"
                )

            self._state["last_trigger"] = {
                "at": event["last_triggered_at"],
                "reason": reason,
                "success": True,
            }
            _save_state(self._state)

    def _maybe_daily_refresh(
        self,
        now_et: datetime,
    ) -> None:
        # FRED daily/weekly market context does not need weekend work.
        if now_et.weekday() >= 5:
            return

        scheduled = now_et.replace(
            hour=DAILY_REFRESH_HOUR_ET,
            minute=DAILY_REFRESH_MINUTE_ET,
            second=0,
            microsecond=0,
        )

        if now_et < scheduled:
            return

        today_str = now_et.date().isoformat()

        with self._state_lock:
            if (
                self._state.get("daily_refresh_date")
                == today_str
            ):
                return

        print(
            "[growth-watch] running weekday end-of-day "
            "Growth refresh"
        )
        success = self._run_growth_collector()

        if not success:
            return

        with self._state_lock:
            self._state["daily_refresh_date"] = today_str
            self._state["last_trigger"] = {
                "at": datetime.now(
                    timezone.utc
                ).isoformat(),
                "reason": "weekday_eod",
                "success": True,
            }
            _save_state(self._state)

    def _loop(self) -> None:
        if not FRED_API_KEY:
            print(
                "[growth-watch] disabled — FRED_API_KEY "
                "is not configured"
            )
            return

        print(
            "[growth-watch] release-aware watcher started "
            f"(window poll={WATCH_POLL_SECONDS}s)"
        )

        while not self._stop_event.is_set():
            try:
                now_et = datetime.now(ET)
                self._refresh_schedule(now_et)

                with self._state_lock:
                    groups_state = json.loads(
                        json.dumps(
                            self._state.get("groups", {})
                        )
                    )

                self._watching_group = None

                for group_key, config in GROUPS.items():
                    group_state = groups_state.get(
                        group_key,
                        {},
                    )
                    release_date = group_state.get(
                        "next_release_date"
                    )

                    if not release_date:
                        continue

                    self._check_release_group(
                        group_key,
                        config,
                        release_date,
                        now_et,
                    )

                self._maybe_daily_refresh(now_et)

            except Exception as exc:
                print(
                    "[growth-watch] loop error: "
                    f"{type(exc).__name__}: {exc}"
                )

            # The loop itself is cheap; network requests happen only
            # during schedule planning or an active release window.
            if self._stop_event.wait(20):
                return
