## Why

Implements roadmap row `silver-incomplete-status-family`: branch every Silver
asset on the status `corpus ingest` can actually return, mirroring the
`skipped` + `incomplete` shape `public_contracts_silver` just landed, but only
where `incomplete` is reachable — a branch added where corpus cannot emit the
status is dead code (see `market_history.py`'s precedent).

The brief (`tmp/brief.md`) narrows the row's own framing: of the nine sibling
datasets, `corpus ingest` can only emit `status: "incomplete"` for
**`structures`** (declared-suffix `PublicationFrontier`, ADR-0064) and
**`killmails`** (year-index `NotYetPublished`, ADR-0028's 2026-09-01
extension). For the other seven — `system-jumps`, `system-kills`,
`market-orders`, `industry-cost-indices`, `sovereignty-map`,
`sovereignty-structures`, `sovereignty-campaigns` — the fetch layer has no
path that reaches it (`hourly-folder(-tar)` with no `member_suffix` declared
maps every non-`skipped` failure to a clean `skip` or a fatal error), so they
get no branch, only a docstring sentence saying why.

## What Changes

- Add an `incomplete` branch to `structures_silver` and `killmails_silver`,
  copying `public_contracts_silver`'s shape: `yield dg.AssetObservation(...,
  metadata={"skip_reason": "upstream_incomplete", ...}); return` before the
  unconditional `verify`.
- Add a `market_history.py`-style docstring sentence to the Silver function of
  each of the seven other datasets, stating that `incomplete` is unreachable
  for their layout and naming the fetch-layer reason (no `member_suffix`
  declared, so `FolderEmptiedByDeclaredSuffix` never fires; a folder-not-found
  or empty-folder 404 maps to `UpstreamAbsent`/`skipped` or a fatal error, not
  to `NotYetPublished`).
- Extend each of the two changed assets' test module
  (`tests/test_structures.py`, `tests/test_killmails.py`) with a case that
  sets `FAKE_INCOMPLETE_DATES` and asserts the partition stays Missing with an
  `AssetObservation` carrying `skip_reason: upstream_incomplete` — no
  `tests/fake_corpus.py` change needed, `_do_ingest`'s `FAKE_INCOMPLETE_DATES`
  check is already dataset-generic.

No compute, parsing or validation moves into Python: both changed assets keep
shelling `corpus ingest` then `corpus verify` and only branch on the `status`
string corpus already returns on stdout.

## Capabilities

### New Capabilities

- `silver-upstream-absence`

### Modified Capabilities

- none. `public-contracts-silver` already states the two-status rule for its
  own dataset and is not restated here; this row's capability is the family
  rule and the reachability discipline that decides which assets carry a
  branch at all.

## Impact

- Modules under `defs/`: `defs/structures.py#structures_silver`,
  `defs/killmails.py#killmails_silver` (behaviour change); `defs/system_jumps.py`,
  `defs/system_kills.py`, `defs/market_orders.py`,
  `defs/industry_cost_indices.py`, `defs/sovereignty_map.py`,
  `defs/sovereignty_structures.py`, `defs/sovereignty_campaigns.py`
  (docstring only).
- Corpus CLI surface shelled / what is recorded: unchanged —
  `corpus ingest --dataset <x> --date <d> --sink-path <p>` then
  `corpus verify --dataset <x> --date <d> --tier silver --sink-path <p>`; the
  two changed assets now also read `status.get("status") == "incomplete"` off
  the ingest call's existing stdout and record an `AssetObservation` instead
  of falling through to `verify`.
- Sensors, schedules, pools: `structures_availability_sensor` and
  `killmails_availability_sensor` (`defs/sensors.py`) keep their existing
  trigger condition and rotating `run_key` (`sensor_util.py`) — a still-missing
  partition after this change is retried on the next tick exactly as it is
  today for `public_contracts_silver`. Both assets keep the `everef_download`
  pool; no memory budget in `deploy/dagster.yaml` moves.
