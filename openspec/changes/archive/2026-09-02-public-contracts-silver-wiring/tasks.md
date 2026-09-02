## 1. `config.py` — Silver-only start resolution

- [x] 1.1 Add a resolver for a dataset that declares no `gold:` block: it returns
      the dataset's `silver.served_start` through the existing
      `_silver_served_start` helper, and raises `PartitionConfigError` when the
      dataset declares neither a Gold derivative nor a coverage floor. Do not
      touch `resolve_partition_starts`'s Gold-derivative path — a dataset with
      derivatives must resolve exactly as it does today. Name the function and
      place it as you judge best; the constraint is that no caller may hardcode a
      date. Verify: new unit tests in `tests/test_config.py` cover a fixture with
      a floor and no `gold:` (resolves), a fixture with neither (raises), and a
      real dataset that has derivatives (unchanged).
- [x] 1.2 Verify no existing dataset regresses: `uv run pytest tests/test_config.py -q`
      passes for every dataset already calling `resolve_partition_starts`.

## 2. `defs/public_contracts.py` — the Silver asset

- [x] 2.1 Consult the `dagster-expert` skill and the `add-dataset-to-orchestration`
      skill before writing the definition. Add `defs/public_contracts.py` with
      `DATASET = "public-contracts"`, a `DailyPartitionsDefinition` whose start
      comes from task 1's resolver, and `public_contracts_silver` on the
      `defs/system_jumps.py#system_jumps_silver` mould: `corpus ingest` then —
      only on success — `corpus verify --tier silver`, `pool="everef_download"`,
      `output_required=False` for the upstream-gap skip branch, and the run-state
      metadata merge (`rows`, `retention_class`, `parquet_sha256`) every
      partitioned asset here does. The module docstring says why the start is
      resolved from the coverage floor rather than from derivatives, and that the
      Gold derivatives are corpus's `public-contracts-gold` and are not this
      row's. Do not touch `defs/public_contracts_live.py`. Verify: `uv run ruff
      check` on the new module passes and it imports cleanly.
- [x] 2.2 Nothing is registered by hand: `src/eve_industry_orchestration/definitions.py`
      loads the code location with `load_from_defs_folder`, so a module placed in
      `defs/` is picked up. Verify: the loaded definitions list
      `public_contracts_silver` — assert it in a test rather than by eye.

## 3. `defs/sensors.py` — availability sensor

- [x] 3.1 Consult the `dagster-expert` skill before writing the sensor. Add
      `public_contracts_availability_sensor` on the
      `market_history_availability_sensor` mould: `corpus everef
      missing-partitions` for `public-contracts` through the shared
      `sensor_util` request path, so it inherits the per-tick fan-out cap, the
      rotating run key and the in-flight guard. Target
      `public_contracts_silver`, match the siblings' minimum interval, and ship
      `DefaultSensorStatus.STOPPED` as every sibling does. It covers the trailing
      edge only — it is not the backfill mechanism, and it must not be widened to
      become one. Verify: a test drives one tick against the fake binary and
      asserts the requested partitions and the cap.

## 4. `tests/fake_corpus.py` and `tests/test_public_contracts.py`

- [x] 4.1 (No change needed.) `tests/fake_corpus.py` required no `public-contracts`
      branch: its `ingest`, `verify`, `everef missing-partitions` and `state
      query` paths are already dataset-generic, and the new tests drive all four
      against it unmodified. The original task read: extend `tests/fake_corpus.py`
      with the `public-contracts` cases:
      `ingest` (a written partition, and the skip status for an absent upstream
      day), `verify --tier silver`, `everef missing-partitions --format json` and
      the `state query --format json` row. Follow the closest existing dataset's
      fixtures. Verify: `uv run pytest -q` still passes for every dataset already
      exercising the fake binary.
- [x] 4.2 Write `tests/test_public_contracts.py` on
      `tests/test_public_contracts_live.py`'s structure: a partition
      materialises and seals `_DONE`; the metadata carries the identifying fields
      plus the run-state facts; a missing run-state row warns and still succeeds;
      an absent upstream day leaves the partition unmaterialised without failing;
      the partition definition starts at the coverage floor the corpus YAML
      declares (read it, do not hardcode 2021-06-17 in the assertion's
      expectation source); and the sensor tick cases from 3.1. Verify: `uv run
      pytest tests/test_public_contracts.py -q` passes.

## 5. `deploy/dagster.yaml` — the backfill's run-planning

- [x] 5.1 Add `public-contracts` to the `everef_download` member list, and add to
      the memory-budget block — not as prose elsewhere — a short paragraph
      recording *why* it is not memory-bearing rather than merely that nobody
      measured it: the ingestor streams one archive at a time, so a day's ~47
      snapshots never coexist and the peak is a small multiple of one
      decompressed archive (~63 MB) — corpus
      `crates/ingestor-public-contracts/src/silver.rs:34-46` states this
      directly. Note that no `/usr/bin/time -v` figure exists (corpus
      `tmp/contracts/measurements-2026-09-01.md` §7.5 states the problem and
      gives no number), that the full backfill is 1 892 partitions and ~8.2 h at
      the politeness limit of 2 and so overlaps the daily schedules, and which of
      the two handlings applies — paused schedules, or a bound of its own. Keep it
      short: this file is already long and the arithmetic table is what readers
      come for. Do not restate these numbers in `CLAUDE.md` or `ROADMAP.md`.
      Verify: `uv run pytest tests/test_concurrency_pools.py -q` passes with the
      pinned pool set unchanged.

## 6. Real run

- [x] 6.1 In a scratch Dagster instance — `DAGSTER_HOME` and `CORPUS_SINK_PATH`
      under `C:\tmp\orchestration-scratch\public-contracts-silver-wiring`, `Y:\`
      read and never written — against the real `corpus` binary, materialise
      `public_contracts_silver` for `2021-06-17` and preview one tick of
      `public_contracts_availability_sensor`. That date is the resolved start and
      is the day corpus itself measured: 28 archives, 14 616 340 rows, 118.1 MiB
      of Silver from 130.1 MiB of source. Report the partition sealed under the
      scratch sink, the recorded `rows` against that figure, and what the tick
      requested. If the binary in the corpus checkout predates the
      public-contracts ingestor, say so and report what you ran instead — a run
      that could not happen is evidence, not a pass.
- [x] 6.2 While 6.1 runs, capture the peak working set of the `corpus` process
      (PowerShell, e.g. sampling `Get-Process corpus | Select-Object PeakWorkingSet64`,
      or `Measure-Command` around it with the process peak read at exit). Report
      it as an **indicative Windows workstation number only** — it is not the
      `/usr/bin/time -v` measurement on the LXC that pool membership requires, and
      it must not be written into `deploy/dagster.yaml` as if it were. It goes in
      the report so the next row starts from something rather than nothing.

## 7. The claims the row falsifies

- [x] 7.1 `openspec/config.yaml`'s **State of the repository** paragraph says
      "The history half of public contracts is blocked in corpus and has no asset
      here." This row makes that false. Rewrite the sentence to what is then
      true: the history tier is wired as a day-partitioned Silver asset with its
      own availability sensor, its start resolved from the Silver coverage floor
      because the dataset declares no Gold derivative, and the Gold half still
      corpus's. Do not restate the memory arithmetic here.
