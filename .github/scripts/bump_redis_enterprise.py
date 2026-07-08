#!/usr/bin/env python3
"""Bump the RedisTimeSeries 99.99 beta in a checked-out Redis-Enterprise tree.

Strategy: edit only the source-of-truth (`modules/modules_build_files_generator.py`)
to change the redistimeseries 99.99 version, then invoke that generator. The
generator (re)downloads each beta zip from S3, computes SHA256s, and rewrites
`modules/modules_99_99.bzl` + `modules/BUILD.bazel` canonically. This keeps us
in lockstep with whatever OS×arch matrix the generator currently supports —
so a new combo (e.g. rhel8-aarch64 appearing on S3) is picked up automatically.

The caller's job is to:
  1. Run this script.
  2. `git add modules/` + commit + push + open PR.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

# The single line in modules_build_files_generator.py that pins the TS 99.99 beta.
GEN_LINE_RE = re.compile(
    r'(Module\(name="redistimeseries",\s*version="99\.99\.99\.)'
    r'(?P<old>[^"]+)'
    r'(",\s*feature_set="99\.99"\s*\))'
)


def bump_generator(generator_path: Path, new_version: str) -> str:
    """Patch the redistimeseries 99.99 version line. Returns the OLD version."""
    text = generator_path.read_text()
    m = GEN_LINE_RE.search(text)
    if not m:
        raise RuntimeError(
            f"redistimeseries 99.99 line not found in {generator_path}. "
            f"Regex {GEN_LINE_RE.pattern!r} matched nothing."
        )
    old = m.group("old")
    if old == new_version:
        print(f"redistimeseries 99.99 already at {new_version} — no change.", file=sys.stderr)
        return old
    new_text, n = GEN_LINE_RE.subn(rf'\g<1>{new_version}\g<3>', text)
    if n != 1:
        raise RuntimeError(f"Expected exactly one match for the TS 99.99 line, got {n}")
    generator_path.write_text(new_text)
    return old


def run_generator(modules_dir: Path) -> None:
    """Invoke modules_build_files_generator.py from its own directory."""
    cmd = [sys.executable, "modules_build_files_generator.py"]
    print(f"==> running generator in {modules_dir}", file=sys.stderr)
    # Stream output live so the workflow log is useful if something goes wrong.
    subprocess.run(cmd, cwd=str(modules_dir), check=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-root", required=True, help="Path to checked-out Redis-Enterprise tree")
    ap.add_argument("--new-version", required=True, help="New TS beta version (with or without the leading 99.99.99.)")
    ap.add_argument("--summary-out", help="Optional path to write a markdown summary to")
    ap.add_argument("--skip-generator", action="store_true",
                    help="Only update the generator file, don't re-run it (mostly for local debugging)")
    args = ap.parse_args()

    # Accept "99.99.99.<ts>.<run>" or "<ts>.<run>".
    new_version = args.new_version
    if new_version.startswith("99.99.99."):
        new_version = new_version[len("99.99.99."):]

    modules_dir = Path(args.repo_root) / "modules"
    generator = modules_dir / "modules_build_files_generator.py"
    if not generator.is_file():
        raise SystemExit(f"missing: {generator}")

    print(f"==> bumping redistimeseries 99.99 in {generator.name}", file=sys.stderr)
    old_version = bump_generator(generator, new_version)
    print(f"    old: 99.99.99.{old_version}", file=sys.stderr)
    print(f"    new: 99.99.99.{new_version}", file=sys.stderr)

    if not args.skip_generator:
        run_generator(modules_dir)

    if args.summary_out:
        lines = [
            f"**Old beta:** `99.99.99.{old_version}`",
            f"**New beta:** `99.99.99.{new_version}`",
            "",
            "Generated files (`modules/modules_99_99.bzl`, `modules/BUILD.bazel`) were "
            "produced by re-running `modules/modules_build_files_generator.py` against "
            "S3, so the OS×arch matrix is canonical for the current beta.",
        ]
        Path(args.summary_out).write_text("\n".join(lines) + "\n")

    # Surface for the calling shell.
    print(f"OLD_VERSION=99.99.99.{old_version}")
    print(f"NEW_VERSION=99.99.99.{new_version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
