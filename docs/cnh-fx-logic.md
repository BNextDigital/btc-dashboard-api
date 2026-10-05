# CNH source and wind classification

The `usdcnh` shared-cache key uses Yahoo `CNH=X` (offshore USD/CNH).
`CNY=X` is onshore USD/CNY and must not be substituted when CNH is missing.
Source references:

- https://ca.finance.yahoo.com/quote/CNH%3DX/
- https://ca.finance.yahoo.com/quote/CNY%3DX/
- https://www.bis.org/publications/renminbi-turnover-tilts-onshore

Yahoo's indexed quote titles distinguish the symbols. Direct page requests were
rate limited during verification, so automated regressions use distinct fixture
series rather than asserting a live provider quote.

The CNH card preserves the configured level bands but carries direction
separately from alert severity:

| USD/CNH level | Severity | `wind_effect` |
| --- | --- | --- |
| At least 7.40 | extreme | headwind |
| 7.25 to below 7.40 | notable | headwind |
| 6.90 to below 7.25 | none | neutral |
| Below 6.90 | notable | tailwind |

These fixed bands are contextual heuristics. They describe USD pressure within
this pair, not observed capital flows, intervention, or BTC selling. Momentum
and percentile remain display context; neither overrides this level model.

The wind summary consumes `wind_effect`, not the alert label or severity alone.
Missing/invalid CNH data contributes neither headwinds nor tailwinds. A legacy
card without direction also contributes neither side, avoiding the old default
that incorrectly treated every notable alert as weak CNH. Existing DXY, EUR,
JPY, EM, and aggregate summary rules are preserved.

For the regression example 6.6987, the card remains `notable` for strength and
contributes a tailwind. Previously it displayed strength but contributed
`CNH weak` to headwinds. The source correction may change the actual quote,
because the old example came from the onshore feed.

Validation: `python -m unittest discover -s tests -v`. Tests cover the regression,
all level boundaries, missing/invalid inputs, mixed winds, wording changes,
offshore cache mapping, no CNY fallback, and the route/cache response contract.

This change is proposed on a PR branch only. If merged later, the market collector
must refresh `/forex/metrics`; existing persisted snapshots are not rewritten.
