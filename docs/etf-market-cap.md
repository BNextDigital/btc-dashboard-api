# Dated BTC ETF market capitalization

The `/etf-aum/metrics` route keeps its existing response keys, but the card is
explicitly a **market capitalization estimate** for the fixed eight-fund basket:
IBIT, FBTC, ARKB, BITB, HODL, BTCO, EZBC, BRRR. It excludes GBTC/BTC and other
funds. Market price × shares differs from issuer-reported net assets and does
not measure net inflows. The issuer's separate figures illustrate this distinction:
https://www.ishares.com/us/products/333011/ishares-bitcoin-trust-etf

## Inputs and freshness

One five-session Yahoo download supplies unadjusted closes and split actions;
`Ticker.get_shares_full(start, end)` supplies dated share observations. The API
never substitutes undated `fast_info.shares`, `sharesOutstanding`, or
`totalAssets` into this estimate. See the provider API:
https://ranaroussi.github.io/yfinance/reference/api/yfinance.Ticker.get_shares_full.html

All eight latest completed price observations must share a date. The most recent
share observation on or before that date may lag by at most seven calendar days;
that date and lag are exposed per fund. A split between the share date and close
requires an updated share count. Splits in an unfinished session that restate
prior Yahoo closes are reversed before pairing those closes with prior shares.
Current-session prices are excluded until 16:15 America/New_York. A close older
than four calendar days is stale. These are conservative freshness limits, not
an exchange-calendar service; a long closure may show a stale status.

A missing fund invalidates the total. If a complete v2 snapshot exists, the API
returns that dated last-good value as `stale`, suppressing comparisons,
percentile, and alerts. Otherwise the total is null with `unavailable` status.
Yahoo failures are visible in data-quality metadata. Provider availability was
not verified for every fund during implementation; a workspace smoke request
was rate limited. Missing dated share coverage remains unavailable.

## History and rollout

`aum_snapshots_v2` shares the existing database file but isolates poll-dated
legacy rows. The original table is retained and never used for new statistics.
There is one upsert per completed source date, including the current point in
the chart. Weekend polling does not create rows. No history is reconstructed by
multiplying today's share count by old prices.

7/30-day comparisons use calendar dates relative to the source date, selecting
the most recent observation on or before the target within four days. Missing
baselines are `—`; a measured zero remains `+0.0%`. Comparison dates are exposed.
The percentile requires at least 60 observations covering at least 85 days of
the trailing 90-calendar-day window. Ties use midpoint rank. Labels describe the
observed window, not a cycle high or institutional positioning.

The frontend accepts `methodology_version: 2`, shows the date and quality status,
and hides legacy undated totals/statistics during deployment overlap. History
starts rebuilding after deployment; 7/30-day comparisons and the percentile
appear only as sufficient trusted observations accumulate. Backend and frontend
both need deployment; the collector then refreshes the snapshot.

Validation: `python -m unittest discover -s tests -v`. ETF regression coverage
includes partial baskets, intraday exclusions, stale/future shares, split timing,
weekend upserts, legacy isolation, missing baselines, real zero changes, negative
changes, percentile coverage/ties, last-good fallback, and cache invalidation.
