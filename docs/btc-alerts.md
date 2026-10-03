# BTC alert classification

`shared/btc_alerts.py` is the shared classifier and synthesis layer for the
collector (`main.py`), formatters, and lightweight snapshot server.

Complete labels `No alert`, `No alerts`, `None`, blank text, dash placeholders,
`No data`, and `Error` classify as `none`, ignoring any stale saved severity.
Matching ignores casing and whitespace; display text and user values are kept.
Substantive alerts retain an explicitly assigned level, including threshold-based
extreme labels such as futures backwardation. Existing neutral signal handling
is preserved.

Saved overrides are normalized on read, without requiring a migration or edit of
manual history. `/metrics`, `/summary`, `/causal`, and `/dashboard/btc` use the
same corrected metrics. Summary and causal reads rebuild the existing synthesis
rules from the snapshot metrics plus current overrides, so stale summary counts
and causal weights cannot carry forward false alerts. Snapshot data and the
collector cache are not mutated. Bundle revisions include the corrected output.

Run the regression suite with:

```sh
python -m unittest discover -s tests -v
```

The alert tests cover existing override files, new manual overrides, historical
rows, formatter overrides, summary counts, real severity preservation, causal
weights, and bundle ETag changes. A replay of the affected five-signal snapshot
removes only the three `No alert` entries, leaving two real notable signals.
