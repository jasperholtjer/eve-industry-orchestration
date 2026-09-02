# A `CHANGELOG.md` in this repository

## What it would have been

Keep a Changelog, the same file corpus keeps, landing every merged row under
`[Unreleased]`. It would give one chronological read of what this orchestrator
started doing and when — which sensor arrived, which pool limit moved, which
dataset got wired — without reconstructing it from eleven spec files and a
`git log`. It is also the cheapest possible handover: an operator who has been
away a month reads one file rather than diffing the roadmap. And it is what
half the platform already does, so the asymmetry costs a beat of explanation
every time someone notices it.

## Why not

Nothing here is released, so there is no section for an entry to belong to.
Corpus keeps a changelog because it cuts versions and `release.yml` builds the
GitHub release notes from the matching section; this repo ships no artefact, has
no tags, and is deployed by `redeploy.sh` pulling `develop`. A changelog with no
release is a second commit log, maintained by hand, and the entry that matters
most — *what fires when now* — is not a line of prose but the partition
definition, the sensor and the schedule, which are read directly.

The record that has to be right is the one something enforces. `openspec/specs/`
says what this orchestrator guarantees today, `deploy/dagster.yaml` owns the
memory arithmetic, and `roadmap.yaml`'s `status: done` says a row landed. A
changelog duplicates all three without being checked against any of them.

## What would change our mind

This repository cutting versions — a tagged deployment, or an artefact another
repo depends on by version. Then the release notes need a source and the file
earns itself, exactly as it does in corpus. Wanting a readable history is not
enough on its own: `git log --first-parent develop` is that, already accurate.
