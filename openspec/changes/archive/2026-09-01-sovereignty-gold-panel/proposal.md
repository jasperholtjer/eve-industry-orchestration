## Why

The sovereignty family's Silver has landed (roadmap row `sovereignty-silver-wiring`)
and is inert: corpus declares five Gold derivatives across the three datasets and
this code location has an asset for none of them, so nothing downstream can be
served. This change implements roadmap row **`sovereignty-gold-panel`**.

The last of the five is the reason the row is one row rather than five. The
assembled panel is the family's first Gold-over-Gold build: it reads the same day's
sibling Gold partitions, a trailing flip window over a fourth sibling and the SDE
snapshot, never Silver. Corpus ADR-0066 decision 8 makes that build order part of
the contract and requires the orchestrator to express it as a real asset
dependency rather than as schedule ordering — which cannot be checked until the
four trees it depends on exist.

## What Changes

- Five day-partitioned Gold assets, one per corpus derivative, each shelling out to
  the corpus Gold build for its own derivative and then the corpus contract
  verification for the Gold tier:
  - `sovereignty_ownership_gold` and `sovereignty_changes_gold` off
    `sovereignty-map`;
  - `sovereignty_adm_gold` off `sovereignty-structures`;
  - `sovereignty_contests_gold` off `sovereignty-campaigns`;
  - `sovereignty_panel_gold` off `sovereignty-map`, which owns the derivative name.
- The panel's build order as a real dependency: `sovereignty_panel_gold` declares
  `deps=` on the four sibling Gold assets and on the SDE snapshot Gold asset. The
  SDE asset is non-partitioned, so that edge carries lineage only — the same shape
  `killmails_consumption_gold` already uses.
- Five Gold availability sensors, one per derivative, each polling corpus for the
  dates that derivative reports ready and requesting those partitions. Readiness is
  the binary's answer, not an inference from the upstream Silver run.
- Per-derivative partition starts read through the existing config resolver. The
  panel's start sits one flip window past its siblings' because the corpus dataset
  configuration says so; no date is written into `defs/`.
- Two gates the assets record and never pre-empt: a permanently absent same-day
  prerequisite makes the build report a skipped day, which is observed rather than
  materialised and does not fail the run; an incomplete flip window is not a skip
  at all — the build succeeds and publishes the two flip counts as NULL. Both
  decisions belong to the binary. Conflating them in Python would publish a wrong
  number rather than a missing one.
- No new concurrency pool. None of the five builds has a measured peak RSS, and
  membership of a memory-bearing pool is by measurement; the five run under the
  global cap alone, as the other narrow Gold builds do.

## Capabilities

### New Capabilities

- `sovereignty-gold`: how a sovereignty Gold derivative is built, offered and
  recorded — per-derivative partition starts, the build-then-verify order, the
  skipped-day and incomplete-window gates, the assembled panel's Gold-over-Gold
  dependency edges, per-derivative readiness sensors, and the concurrency bound
  the builds run under.

### Modified Capabilities

<!-- None. `sovereignty-silver` already states that the panel contributes no
     Silver reach-back; this change adds no requirement to it and changes none. -->

## Impact

- `src/eve_industry_orchestration/defs/sovereignty_map.py` — three Gold assets
  (ownership, changes, panel).
- `src/eve_industry_orchestration/defs/sovereignty_structures.py` — the ADM Gold
  asset.
- `src/eve_industry_orchestration/defs/sovereignty_campaigns.py` — the contests
  Gold asset.
- `src/eve_industry_orchestration/defs/sensors.py` — five Gold readiness sensors.
- `tests/` — Gold coverage per derivative against the fake corpus binary, including
  the skipped-day branch, the panel's dependency set and the unchanged pool set.
- No change to `deploy/dagster.yaml`, `defs/config.py` or the corpus dataset
  configuration. Corpus is read-only from here.
