# Global M2 calculation (v2)

`/leading/global-m2` remains compatible with the dashboard's display keys,
including `global_m2`, regional display values, growth strings, `last_date`,
alerts, and pattern. Numeric USD-billion fields, `components`, `fx`, `as_of`,
`data_quality`, and `calculation_version` make the calculation inspectable.
The endpoint is also consumed by `/leading/all` through the existing collector.

## Sources and units

| Region | Official source / series | Native unit | Native billions |
|---|---|---|---|
| US | [FRED M2SL](https://fred.stlouisfed.org/series/M2SL) | USD billions | value |
| Eurozone | [ECB BSI.M.U2.Y.V.M20.X.1.U2.2300.Z01.E](https://data.ecb.europa.eu/data/datasets/BSI/BSI.M.U2.Y.V.M20.X.1.U2.2300.Z01.E) | EUR millions | value / 1000 |
| China | [PBoC Financial Statistics Reports](https://www.pbc.gov.cn/en/3688247/3688978/3709137/index.html) | CNY trillions | value * 1000 |
| Japan | [BOJ MD02/MAM1NAM2M2MO](https://www.stat-search.boj.or.jp/ssi/mtshtml/mam1nam2m2mo.html) | 100 million JPY | value * 0.1 |

US uses the existing FRED key and shared persistent cache. Other sources need
no new keys. BOJ uses its [documented API](https://www.stat-search.boj.or.jp/info/api_manual_en.pdf),
including nested `VALUES.SURVEY_DATES` and `VALUES.VALUES` arrays. ECB responses
are CSV; the M2 series key, currency, and unit multiplier are checked.
China report titles distinguish months, Q1, H1, Q1-Q3, and annual reports;
report text must explicitly identify M2 in RMB trillions.

## Alignment and FX

ECB `EXR.M.USD+CNY+JPY.EUR.SP00.A` supplies monthly average currency-per-EUR
rates. USD per EUR is the USD rate; USD per CNY/JPY is the USD rate divided
by the CNY/JPY rate. Each month's four regional native-billion amounts are
converted with that same month's observed rates and summed.

The latest **common completed month** across all four regional series and all
three FX rates determines `as_of`. MoM and YoY compare this aggregate with the
exact preceding calendar month and same month one year earlier. Missing
comparison months yield null growth; no nearest-date approximation is used.
Zero growth is retained as zero.

This is a four-region proxy, not a harmonised world aggregate. US and eurozone
series are seasonally adjusted; China and Japan are not. The sources mix
month-end stocks and monthly averages. Growth in the USD composite includes
FX valuation effects. The retained 8–12 week lead-time label is a heuristic,
not an estimated or guaranteed relationship to BTC.

## Failure handling and storage

Each source is refreshed at most every 12 hours and stored atomically under
`DATA_DIR/global_m2_sources`. Successful observations survive process restarts.
PBoC report history is collected across at most three listing pages (25-month
window) and downloaded by three workers; later refreshes recheck the newest
two reports and retry missing reports. Partial report failures are exposed
as warnings. Independent sources fetch concurrently.

No missing region becomes zero. Without a shared month, the aggregate is
null/unavailable. A common month over three calendar months old is
null/stale. Source failures with usable persisted data, partial report
failures, or missing growth comparison months produce degraded status and
suppress directional alerts. Values, observation dates, units, adjustment,
source URLs, cache fallback state, and source fetch errors remain inspectable.

Corrected daily snapshots use `global_m2_history_v2`. The history route reads
this table, preserving the legacy table without charting its invalid values.
History starts accumulating after the first successful corrected composite;
legacy history is not automatically rescaled or backfilled.

No new dependency or environment variable is required. The initial PBoC
history fetch is slower than subsequent cached refreshes. Production output
updates when the slow collector next refreshes after deployment.

## Validation

Run `python -m unittest discover -s tests -v`.
Tests cover unit validation, BOJ nested arrays, PBoC period parsing and
pagination, aggregate growth, historical FX, missing and stale data,
zero growth, durable cache fallback, formatter compatibility, and v2 history.
