// The repository's "never" rules as refusals instead of prose. Loads in every
// pi process started in this repo, subagents included.
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { isToolCallEventType } from "@earendil-works/pi-coding-agent";
import { existsSync, readFileSync } from "node:fs";

const Y = String.raw`[Yy]:[\\/]`;
// A write-shaped token followed by a Y:\ path. --silver-path Y:/silver is a read and passes.
// ponytail: cp/mv/rm with Y:\ anywhere in the segment is refused, reads via cp from Y:\ included.
const Y_WRITE = new RegExp(
  String.raw`(--(sink|gold)-path\s*=?\s*|>{1,2}\s*)["']?` + Y + String.raw`|\b(rm|mv|cp|tee|mkdir|touch|rmdir|robocopy|xcopy|Copy-Item|Move-Item|Remove-Item|New-Item|Set-Content|Out-File)\b[^|&;>]*\s["']?` + Y,
);
// CORPUS_SINK_PATH is placement, so Y:\ in it reads for a sensor preview and writes
// for a materialise. Only the materialise is refused, and only where the assignment
// and the command are in one call — an `export` on an earlier turn is the prose's half.
const Y_MATERIALIZE = new RegExp(String.raw`\b(materialize|backfill)\b[\s\S]*` + Y + `|` + Y + String.raw`[\s\S]*\b(materialize|backfill)\b`);
const CORPUS_REPO = /(^|\/)eve-industry-corpus\//;
const BAD_GIT = /\bgit\b[^|&;]*\s(add\s+(-A|--all|\.)(?=\s|$)|commit\s+(-[a-zA-Z]*a[a-zA-Z]*|--all)(?=\s|$)|push(?=\s|$))/;
const norm = (p: string) => p.replace(/\\/g, "/");

// tmp/row.json in the root checkout, written by the row session at intake and
// removed at merge: { "worktree": "C:/Projecten/eve/eve-industry-orchestration/.worktrees/<id>" }
function activeWorktree(): string | undefined {
  const f = "tmp/row.json";
  try {
    return existsSync(f) ? norm(JSON.parse(readFileSync(f, "utf8")).worktree) : undefined;
  } catch {
    return undefined;
  }
}

export default function (pi: ExtensionAPI) {
  pi.on("tool_call", async (event) => {
    if (isToolCallEventType("bash", event)) {
      const c = event.input.command;
      if (Y_WRITE.test(c) || Y_MATERIALIZE.test(c))
        return { block: true, reason: "Y:\\ is read-only production. Materialise into C:\\tmp\\orchestration-scratch\\<id> via CORPUS_SINK_PATH; Y:\\ is only ever a sensor preview's read." };
      if (BAD_GIT.test(c))
        return { block: true, reason: "Commit by pathspec: never git add -A / add . / commit -a, and never push." };
    }
    if (isToolCallEventType("write", event) || isToolCallEventType("edit", event)) {
      const p = norm(event.input.path);
      if (new RegExp("^" + Y).test(p)) return { block: true, reason: "Y:\\ is read-only production." };
      if (CORPUS_REPO.test(p))
        return { block: true, reason: "The corpus repo is read-only from here: it owns the binary, the CLI contract and the dataset YAML. Ask for a corpus row instead." };
      const wt = activeWorktree();
      if (wt && !p.startsWith(wt) && !/docs\/questions\//.test(p) && !/(^|\/)tmp\//.test(p))
        return { block: true, reason: `Row worktree ${wt} is active: edits belong there, not in the root checkout.` };
    }
    if (isToolCallEventType("read", event) && /(^|[\\/])\.env(\..*)?$/.test(event.input.path))
      return { block: true, reason: ".env files are denied." };
  });
}
