# eve-industry-orchestration — shared language

The words this repository uses that no other repository needs. The platform
vocabulary — row, fix, Bronze/Silver/Gold, partition, `_DONE`, `_INDEX.json`,
area, consumer — is defined once at [`../CONTEXT.md`](../CONTEXT.md) and is not
repeated here.

This file defines words, never behaviour. What a thing does lives in an ADR, in
`docs/serving-seam.md`, in the Decisions section of `ROADMAP.md`, in
`deploy/dagster.yaml`, or in a test. Where the two disagree, the other one is
right and this file is stale.

## Language

**Asset**
One `@dg.asset` in `src/eve_industry_orchestration/defs/`: the unit Dagster
schedules, backfills and records. A dataset's Silver and each of its Gold
derivatives get one asset each; the live datasets get one non-partitioned asset.
*Avoid*: job (that word is an EVE industry job), step, task, node.

**Shim**
The body of an asset: shell the `corpus` binary through `CorpusResource` and
return a `MaterializeResult`. Target ~20 lines. A shim that grows past a trivial
dispatch is logic in the wrong repository, and the remedy is a corpus
subcommand, never a longer shim.
*Avoid*: wrapper, adapter, glue.

**Partition matrix**
The set of legal partition keys an asset declares — a
`dg.DailyPartitionsDefinition` anchored on a start date resolved by
`defs/config.py`, or SDE's build axis. Dagster owns it; corpus owns what a
partition contains.
*Avoid*: the schedule, the calendar, the date range.

**Silver start / Gold start**
The two distinct anchors of one dataset's matrix, both resolved from the corpus
dataset YAML by `resolve_partition_starts`. **Gold start** is the derivative's
`gold.served_start`; **Silver start** reaches back one look-back window before
the earliest Gold start, clamped at `silver.served_start` where upstream
coverage begins later (ADR-0027). Never a literal in Python.
*Avoid*: begin date, epoch, the first day; and *served start* on its own, which
names only the Gold side.

**Availability sensor / readiness sensor**
The two sensor shapes in `defs/sensors.py`, both over `defs/sensor_util.py`.
**Availability** diffs corpus's `missing` report against the Silver matrix;
**readiness** diffs its `ready` report against the Gold matrix. Neither globs
the NAS.
*Avoid*: watcher, poller, trigger.

**Tick**
One sensor evaluation. A tick **proposes** partitions — it emits one
`RunRequest` per eligible key, oldest first, capped at
`MAX_PARTITIONS_PER_TICK` — and launches nothing itself; the run keys carry a
per-tick token so a still-missing date is re-proposed rather than deduped.
*Avoid*: poll, cycle, scan; and *fires* for what a tick does.

**Pool**
A Dagster concurrency pool named in `deploy/dagster.yaml` and joined by an asset
with `pool=`. Four exist: `heavy`, `market_orders`, `news_embed`,
`everef_download`. A pool gates every launch path — sensor, UI backfill, manual
— so a pooled run is bounded by `min(global cap, pool limit)`. Distinct from the
**global run cap** (`concurrency.runs.max_concurrent_runs`), which is the I/O
default every run taps, pooled or not.
*Avoid*: queue, lane, semaphore, run tag.

**Holder**
An asset that occupies one slot of a pool while it runs, and the unit the memory
budget is stated in: *peak per holder* times *slots*. Membership of a
memory-bearing pool is by measured peak, never by shape.
*Avoid*: member, occupant, consumer (that word is a downstream repository).

**Box budget**
The whole of `deploy/dagster.yaml`'s arithmetic against one machine: the pools
do not know about each other, so their per-holder peaks add, and the worst case
is stated against the LXC's RAM whether or not it is reachable in practice. One
box, one budget.
*Avoid*: the memory limit, the cap, headroom.

**The LXC**
The Proxmox container the deployment runs in — 8 cores, 12 GiB, `pct 211` — with
Dagster storage on its NVMe and the medallion on the NFS-mounted UNAS. Where the
box budget is spent. Provisioning is `homelab_docs`', not this repo's.
*Avoid*: the server, the host, the VM, prod.

**Run-state**
The SQLite `partitions` table the `corpus` binary owns, read through
`corpus state query`. It is what every sensor keys on, and this repo only reads
it. Distinct from the **materialisation log** — Dagster's own event log under
`DAGSTER_HOME`, which records what this orchestrator launched and what it
recorded. Corpus's answer to *does this partition exist* is the run-state; the
materialisation log's answer is only *did we run*.
*Avoid*: the database, the state file, the ledger for either of them.

**Placement**
What this repo owns of storage: selecting roots through `--sink-path` and the
`CORPUS_*` environment, and triggering contract-aware operations. Distinct from
the **contract**, which is corpus's: the `parquet + _INDEX.json + _DONE` shape,
the `year=/month=/day=` layout, the sha256 and the schema. Constructing a path
layout in Python is placement crossing into contract, which is the storage
boundary.
*Avoid*: storage, the sink for placement; *format* or *schema* for the contract.

**Live asset**
A non-partitioned asset over a corpus `*-live` dataset: no Silver tier, no date
matrix, one `current/` partition the binary overwrites. Driven by a fixed-cadence
schedule rather than a sensor, because there is no per-date availability to diff.
*Avoid*: latest asset, realtime, streaming, snapshot (that word is corpus's).

**Fake binary**
`tests/fake_corpus.py`, which mimics the slice of the CLI the tests exercise —
the contract writes and the `missing-partitions` / `ready-dates` / `state query`
JSON — so the suite runs with no Rust build, no sibling checkout and no NAS.
Point `CORPUS_BINARY_PATH` at it. It is a test double for the *boundary*, never a
substitute for the real run.
*Avoid*: mock, stub, dummy corpus.

**Real run**
The one execution a bundle touching an asset, a sensor, a schedule or a resource
method does before review: one partition materialised, or one tick previewed, in
a **scratch instance** against the real `corpus` binary. Its output is evidence
handed to the reviewer, not a materialisation anyone keeps.
*Avoid*: smoke test, integration run, live run.

**Scratch instance**
The throwaway Dagster instance a real run uses: `DAGSTER_HOME` and
`CORPUS_SINK_PATH` under `C:\tmp\orchestration-scratch\<id>`. `Y:\` is read and
never written — a materialise whose sink is `Y:\` is a defect in its own right,
and the default sink is never used.
*Avoid*: local instance, test instance, sandbox.

## Relationships

- One **dataset** has one Silver **asset**, one asset per Gold derivative, and
  one **availability sensor** and **readiness sensor** over them — or, when it is
  a **live asset**, one asset and one schedule.
- An asset declares a **partition matrix** anchored on a **Silver start** or a
  **Gold start**, and may join one **pool** as a **holder**; every pool's peaks
  add into the one **box budget** on **the LXC**.
- A **tick** proposes from the **run-state**; what it launches lands in the
  **materialisation log**. The two are never the same record.
- The **fake binary** gates a test; a **real run** in a **scratch instance**
  gates a row. Neither substitutes for the other.

## Flagged ambiguities

- **"run"** means a Dagster run and an execution of the `corpus` binary.
  Resolved: bare **run** is always the Dagster run — that is what this repo
  launches, caps and records. The binary's execution is a *corpus run*, or is
  said as what it is: an ingest, a Gold build, a live build. The `corpus`
  **run-state** keeps its full name for the same reason.
- **"partition"** means the platform's output folder, the Dagster partition key,
  and the run-state's `partitions.partition_key`. The first two are one to one,
  so bare **partition** is the platform's and stays unqualified. Where the
  strings must be told apart, Dagster's is the **partition key**
  (`2024-01-15`, `300`) and the run-state's is a **run-state key**
  (`date=2024-01-15`, `build=300`, `latest`) — a scheme prefix Dagster's does
  not carry, which is the whole reason `corpus_resource.date_key` exists.
- **"status"** meant both a Dagster run status and the `status` field corpus
  reports on an ingest (`incomplete`, `skipped`). Resolved: the corpus one is
  always an *ingest status*, said with its value; bare **status** is Dagster's.
- **"schedule"** is a `dg.schedule` here, never a cron entry on the box. This
  repo installs no crontab: the cadence a schedule names is the only one.
