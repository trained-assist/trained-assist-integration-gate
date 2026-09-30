#!/usr/bin/env python3
"""Bounded deterministic repo index from tracked files; never calls a model."""
import fnmatch
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

ROOT = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip())
OUT = ROOT / ".repo-context/output"
LIMIT = 8192
EXCLUDE = (".env*", "*.key", "*.pem", "*secrets*", "*credentials*", "package-lock.json", "*.log")


def allowed(name):
    p = Path(name)
    return not any(part in {"node_modules", "output", "pgdata", ".wrangler", "logs", "dist", "coverage"}
                   or any(fnmatch.fnmatch(part.lower(), pattern) for pattern in EXCLUDE)
                   for part in p.parts)


def build():
    OUT.mkdir(parents=True, exist_ok=True)
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    names = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT).decode().split("\0")
    names = sorted(n for n in names if n and allowed(n) and (ROOT / n).is_file() and not (ROOT / n).is_symlink())
    repo = os.environ.get("GITHUB_REPOSITORY", "trained-assist/trained-agent-architecture")
    base = f"https://github.com/{repo}/blob/{sha}/"
    rows = ["# Repository map", "", f"Source: `{repo}@{sha}`", "",
            "Generated structurally, without LLM. This index is incomplete by design; open source before changing behavior.",
            "Code details: `repo-compressed.xml` (Tree-sitter). Manifest: `manifest.json`.", "",
            "## Start here", ""]
    for name in ("README.md", "ARCHITECTURE.md", "IMPLEMENTATION-AND-INTEGRATION-PLAN.md", "ENGINEERING-APPROACH.md", "SANDBOX-PLAN.md", "AGENTS.md"):
        if name in names:
            rows.append(f"- [{name}]({base}{name})")
    rows += ["", "## Tracked source index", "", "Paths and Markdown headings; generated data and sensitive paths excluded.", ""]
    indexed, omitted = [], []
    for name in names:
        path = ROOT / name
        label = name.replace("`", "").replace("\n", " ")
        detail = ""
        if path.suffix.lower() == ".md" and path.stat().st_size <= 200000:
            # Extract only structural headings, never paragraphs / fenced examples.
            headings, fenced = [], False
            for line in path.read_text(errors="replace").splitlines():
                if re.match(r"^\s*(```|~~~)", line):
                    fenced = not fenced
                elif not fenced and re.match(r"^#{1,2} ", line):
                    headings.append(re.sub(r"^#+ ", "", line).replace("`", "")[:100])
            detail = " — " + "; ".join(headings[:3]) if headings else ""
        row = f"- `{label}`{detail}"
        tail = f"\n\nOmitted from short index: {len(names)} paths at most. Full inventory is in manifest.json.\n"
        if len(("\n".join(rows + [row]) + tail).encode()) <= LIMIT:
            rows.append(row)
            indexed.append(name)
        else:
            omitted.append(name)
    rows += ["", f"Omitted from short index: {len(omitted)} paths. Full inventory is in manifest.json.", ""]
    output = "\n".join(rows)
    if len(output.encode()) > LIMIT:
        raise RuntimeError("Short index exceeds budget")
    (OUT / "REPO-MAP.md").write_text(output)
    manifest = {
        "schemaVersion": 1, "generatorVersion": "1.0.0", "repomixVersion": "1.18.1",
        "repository": repo, "sourceSha": sha, "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT)),
        "mapByteLimit": LIMIT, "mapBytes": len(output.encode()), "indexed": indexed, "omittedFromMap": omitted,
        "inventory": [{"path": n, "bytes": (ROOT / n).stat().st_size} for n in names],
        "configSha256": hashlib.sha256((ROOT / ".repo-context/repomix.config.json").read_bytes()).hexdigest(),
        "packedPaths": re.findall(r'<file path="([^"\n]+)"', (OUT / "repo-compressed.xml").read_text()) if (OUT / "repo-compressed.xml").exists() else [],
        "packScope": "repomix.config.json allowlist; Markdown is not AST-compressed",
        "files": {n: {"bytes": p.stat().st_size, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                  for n in ("REPO-MAP.md", "repo-compressed.xml") if (p := OUT / n).is_file()},
    }
    (OUT / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    print(f"Repository context: {sha}, map={len(output.encode())} bytes, indexed={len(indexed)}, omitted={len(omitted)}")


if __name__ == "__main__":
    build()
