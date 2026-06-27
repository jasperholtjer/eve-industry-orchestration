# Serving seam

The serving tier — Postgres `eve` and Neo4j on the DB-VM (`192.168.2.212`) — holds
the query-facing copy of the Gold data. It is deployed and operated outside this
repository. The orchestrator owns only the *when*: it triggers loads after Gold
partitions are produced and records the runs.

## The boundary

The serving tier exposes one idempotent CLI, `eve-serving load`, which reads
`_DONE` Gold partitions and writes both stores. Orchestration owns scheduling;
serving owns how the load works. This repo:

- does not add a scheduler to the serving tier,
- does not reach into Postgres or Neo4j directly,
- reimplements no load logic — it only shells out to `eve-serving load`.

This mirrors the `corpus`-binary seam: the data plane lives elsewhere, and Dagster
is a thin shim that invokes a command and logs the result.

## The SSH trigger

The DB-VM has a PATH wrapper `eve-serving` that sources its own environment (Gold
root, DB DSNs), so a bare invocation over SSH is fully configured:

```bash
ssh serving@192.168.2.212 eve-serving load --dataset <name> [--date YYYY-MM-DD]
```

The Dagster LXC's `corpus` account already holds an authorized SSH key for
`serving@192.168.2.212`, so no credentials live in this repo. `ServingResource`
(`defs/serving_resource.py`) builds that command, streams stdout/stderr into the
run log, fails the asset on a non-zero exit, and parses the loader's trailing
`... loaded: <n> rows` / `... skipped: 0 rows` summary into asset metadata. Host
and user are configurable via `SERVING_HOST` / `SERVING_USER` (resource config),
defaulting to `serving@192.168.2.212`.

## Datasets and flags

| Dataset              | Command                                              |
| -------------------- | ---------------------------------------------------- |
| `sde`                | `eve-serving load --dataset sde --latest`            |
| `market-history`     | `eve-serving load --dataset market-history`          |
| `market-orders-live` | `eve-serving load --dataset market-orders-live`      |
| `market-prices-live` | `eve-serving load --dataset market-prices-live`      |

Every load is idempotent on the partition's `parquet_sha256`: re-running an
unchanged partition prints `skipped` and is a no-op.

## Ordering: SDE is upstream of the market loads

The SDE load is the upstream dependency of all market loads. Market facts enforce
foreign keys to the SDE (types and regions), and a new SDE build does a full-state
rewrite that TRUNCATEs the `market.*` tables and clears their serving load-state.
The market datasets must therefore be (re)loaded *after* the SDE load.

The asset graph models this directly (`defs/serving.py`):

- `serving_load_sde` depends on Gold `sde-snapshot` availability.
- each market load depends on its own Gold dataset's availability **and** on
  `serving_load_sde`.

`serving_load_job` selects all four loads, so a single run executes them in
dependency order — SDE first, then the three markets. On a normal hour every load
is a no-op (`skipped`); after an SDE rebuild the SDE load truncates and
repopulates, which clears the market load-state, so the downstream market loads
re-run and repopulate `market.*` in the same pass. `serving_load_schedule`
(hourly, STOPPED by default) drives the job; `deps=` carry lineage only, the same
split between lineage and triggering as the rest of the repo.
