"""What could be built in this repository, and nowhere else.

Run from the repo root, or from a worktree under `.worktrees/`:

    uv run --with pyyaml python .agents/skills/candidates/candidates.py

Five blocks, all orchestration-local. OPEN ROWS is `roadmap.yaml` minus what is
done. WORK ITEMS and FUTURE PHASES are the two standing sections of
`ROADMAP.md` — the first is work that was named and not finished, the second is
the catalogue that never made a row. DEFERRED reads the places a decision is
recorded here (`docs/adr/`, `docs/serving-seam.md`, `ROADMAP.md`'s Decisions
section) for what they chose *not* to build — the bucket that exists nowhere
else, because a deferral is recorded where the decision was made and never
collected anywhere. NOT TAKEN is `docs/decisions/not-taken/`, printed last so a
proposal meets the rejections before it is made.

Writes nothing. The platform-wide view over all six repos is the `candidates`
skill at `C:\\Projecten\\eve`; this one never leaves the repository.
"""

from __future__ import annotations

import re
import sys
import textwrap
from pathlib import Path

import yaml

# What a decision record sounds like when it names something it chose not to
# build. Loose on purpose: a false positive costs one line to read, a missed
# deferral is the whole point of the block.
DEFERRED = re.compile(
    r"\bnot built\b|stays? additive|\bdeferred\b|left for later|a later row"
    r"|its own row|for now",
    re.I,
)

# `### 5. Enrich materialisation metadata — done`, either dash.
WORK_ITEM_DONE = re.compile(r"[-\u2014]\s*done\s*$", re.I)


def open_rows(root: Path) -> None:
    print("OPEN ROWS - roadmap.yaml items that are not done")
    doc = yaml.safe_load((root / "roadmap.yaml").read_text(encoding="utf-8")) or {}
    found = 0
    for raw in doc.get("items") or []:
        if raw.get("status", "todo") == "done":
            continue
        found += 1
        deps = raw.get("depends_on") or []
        waits = f"  waits on {', '.join(deps)}" if deps else ""
        print(
            f"\n  {raw['id']}  [{raw.get('status', 'todo')} p{raw.get('priority', '?')}"
            f" {', '.join(raw.get('areas') or []) or 'no area'}]{waits}"
        )
        print(f"  {raw.get('title', '')}")
        for line in textwrap.wrap(" ".join((raw.get("goal") or "").split()), 92)[:6]:
            print(f"      {line}")
    if not found:
        print("  (none - every row on this roadmap is done)")
    print()


def section(text: str, heading: str) -> str:
    """One `## <heading>` section of a markdown file, without its heading."""
    body = text.partition(f"## {heading}")[2]
    return body.partition("\n## ")[0].strip()


def roadmap_md(root: Path) -> None:
    text = (root / "ROADMAP.md").read_text(encoding="utf-8")

    print("WORK ITEMS - named in ROADMAP.md and not marked done")
    found = 0
    for line in section(text, "Work items").splitlines():
        if line.startswith("### ") and not WORK_ITEM_DONE.search(line):
            found += 1
            print(f"  {line[4:]}")
    if not found:
        print("  (none - every work item is marked done)")
    print()

    print("FUTURE PHASES - described in ROADMAP.md, never made a row")
    for line in section(text, "Future phases").splitlines():
        print(f"  {line}")
    print()


def deferrals(root: Path) -> None:
    print("DEFERRED IN THE DECISIONS - what a record chose not to build")
    sources = [
        *sorted((root / "docs" / "adr").glob("*.md")),
        root / "docs" / "serving-seam.md",
        root / "ROADMAP.md",
    ]
    found = 0
    for path in sources:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        # ROADMAP.md is only interesting where it records a decision; the rest of
        # the file is the CLI surface and the work items, already printed above.
        offset = 0
        if path.name == "ROADMAP.md":
            body = section(text, "Decisions")
            offset = text[: text.index(body)].count("\n") if body else 0
            text = body
        for n, line in enumerate(text.splitlines(), 1 + offset):
            if DEFERRED.search(line):
                found += 1
                stem = path.name.split("-")[0][:14]
                print(f"  {stem:<14}:{n:<5} {line.strip()[:130]}")
    if not found:
        print("  (none - one ADR on file, and the Decisions section defers nothing)")
    print()


def not_taken(root: Path) -> None:
    print("NOT TAKEN - declined before; re-propose only against the file, with proof")
    found = 0
    for path in sorted((root / "docs" / "decisions" / "not-taken").glob("*.md")):
        if path.name == "README.md":
            continue
        found += 1
        title, first_lines = rejection(path)
        print(f"\n  {path.relative_to(root).as_posix()}")
        print(f"  {title}")
        reopen = "what would change our mind"
        for label, key in (("why not", "why not"), ("reopen if", reopen)):
            for n, line in enumerate(textwrap.wrap(first_lines.get(key, "-"), 84)[:2]):
                print(f"      {label + ':' if n == 0 else '':<11}{line}")
    if not found:
        print("  (none recorded)")
    print()


def rejection(path: Path) -> tuple[str, dict[str, str]]:
    """The `#` title, and the first paragraph under each `##` heading, unwrapped."""
    title, lead, heading, para = path.stem, {}, None, []

    def flush() -> None:
        if heading and para and heading not in lead:
            lead[heading] = " ".join(para)
        para.clear()

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("# "):
            flush()
            title = line[2:]
        elif line.startswith("## "):
            flush()
            heading = line[3:].lower()
        elif line:
            para.append(line)
        else:
            flush()
    flush()
    return title, lead


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    start = Path.cwd().resolve()
    # The repo root, or the worktree's - both carry roadmap.yaml beside the package.
    root = next(
        (
            d
            for d in (start, *start.parents)
            if (d / "roadmap.yaml").exists()
            and (d / "src" / "eve_industry_orchestration").is_dir()
        ),
        None,
    )
    if root is None:
        print("No orchestration checkout above the cwd.")
        return 1
    print(f"repo: {root}\n")
    open_rows(root)
    roadmap_md(root)
    deferrals(root)
    not_taken(root)
    return 0


if __name__ == "__main__":
    sys.exit(main())
