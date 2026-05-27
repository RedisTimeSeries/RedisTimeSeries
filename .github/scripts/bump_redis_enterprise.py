#!/usr/bin/env python3
"""Bump the RedisTimeSeries 99.99 beta version in a checked-out Redis-Enterprise tree.

Rewrites three files under <repo>/modules/:
  - modules_99_99.bzl                  (per-OS http_file entries: name / sha256 / urls)
  - BUILD.bazel                        (per-OS dependency labels)
  - modules_build_files_generator.py   (Module(name="redistimeseries", version=..., feature_set="99.99"))

For each TS http_file entry in modules_99_99.bzl, the script downloads the new
zip from S3 (URL derived from the existing entry by replacing the old version),
computes its SHA256, and rewrites the entry in place.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import urllib.request
from pathlib import Path

S3_BASE = "https://redismodules.s3.amazonaws.com/redistimeseries/beta"

# http_file block: name="timeseries-<os>-<arch>-99.99.99.<ts>-99.99", sha256=..., urls=[...]
HTTP_FILE_RE = re.compile(
    r'http_file\(\s*'
    r'name\s*=\s*"(?P<name>timeseries-(?P<os>[^-]+)-(?P<arch>[^-]+)-99\.99\.99\.(?P<ver>[^"-]+)-99\.99)",\s*'
    r'sha256\s*=\s*"(?P<sha>[0-9a-f]+)",\s*'
    r'urls\s*=\s*\[\s*"(?P<url>[^"]+)",?\s*\],?\s*\),?',
    re.MULTILINE,
)


def download_sha256(url: str) -> str:
    """Stream the zip from S3 and return its SHA256 hex digest."""
    h = hashlib.sha256()
    with urllib.request.urlopen(url, timeout=120) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} fetching {url}")
        while True:
            chunk = resp.read(1 << 20)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def rewrite_modules_99_99(path: Path, new_version: str) -> tuple[str, list[tuple[str, str]]]:
    """Rewrite modules_99_99.bzl. Returns (old_version, [(os_arch, sha256)])."""
    text = path.read_text()
    entries: list[tuple[str, str]] = []  # (os-arch, new_sha)
    old_versions: set[str] = set()

    def repl(m: re.Match[str]) -> str:
        old_ver = m.group("ver")
        old_versions.add(old_ver)
        os_, arch = m.group("os"), m.group("arch")
        # Derive new URL by swapping the version in the existing URL — keeps the
        # OS/arch slug formatting exactly as the existing entry uses it.
        old_url = m.group("url")
        new_url = old_url.replace(old_ver, new_version)
        new_name = f"timeseries-{os_}-{arch}-99.99.99.{new_version}-99.99"
        print(f"  fetch sha256: {new_url}", file=sys.stderr)
        new_sha = download_sha256(new_url)
        entries.append((f"{os_}-{arch}", new_sha))
        return (
            f'http_file(\n'
            f'    name = "{new_name}",\n'
            f'    sha256 = "{new_sha}",\n'
            f'    urls = [\n'
            f'      "{new_url}",\n'
            f'    ],\n'
            f'  )'
        )

    new_text, n = HTTP_FILE_RE.subn(repl, text)
    if n == 0:
        raise RuntimeError(f"No timeseries http_file entries found in {path}")
    if len(old_versions) != 1:
        raise RuntimeError(f"Expected one old TS beta version, found {old_versions}")
    path.write_text(new_text)
    return old_versions.pop(), entries


def rewrite_build_bazel(path: Path, old_version: str, new_version: str) -> int:
    """Replace timeseries-<...>-99.99.99.<old>-99.99 → ...<new>... in BUILD.bazel."""
    text = path.read_text()
    pattern = re.compile(
        rf'(timeseries-[a-zA-Z0-9._]+-[a-zA-Z0-9_]+-99\.99\.99\.){re.escape(old_version)}(-99\.99)'
    )
    new_text, n = pattern.subn(rf'\g<1>{new_version}\g<2>', text)
    if n == 0:
        raise RuntimeError(f"No timeseries 99.99 references to bump in {path}")
    path.write_text(new_text)
    return n


def rewrite_generator(path: Path, old_version: str, new_version: str) -> int:
    """Update the `redistimeseries` 99.99 line in modules_build_files_generator.py.

    The stored value includes the full 99.99.99 prefix
    (e.g. `version="99.99.99.20260301.203246.828"`).
    """
    text = path.read_text()
    pattern = re.compile(
        rf'(Module\(name="redistimeseries",\s*version="99\.99\.99\.)'
        rf'{re.escape(old_version)}'
        rf'(",\s*feature_set="99\.99"\s*\))'
    )
    new_text, n = pattern.subn(rf'\g<1>{new_version}\g<2>', text)
    if n != 1:
        raise RuntimeError(
            f"Expected exactly one redistimeseries 99.99 entry in {path}, found {n}"
        )
    path.write_text(new_text)
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-root", required=True, help="Path to checked-out Redis-Enterprise repo")
    ap.add_argument("--new-version", required=True, help="New TS beta version, e.g. 99.99.99.20260526.205848.1092 (without the leading 99.99.99 if you want — accepts both)")
    ap.add_argument("--summary-out", help="Optional path to write a markdown summary to")
    args = ap.parse_args()

    # Accept either "99.99.99.<ts>.<run>" or just "<ts>.<run>" for convenience.
    new_version = args.new_version
    if new_version.startswith("99.99.99."):
        new_version = new_version[len("99.99.99."):]

    modules_dir = Path(args.repo_root) / "modules"
    bzl = modules_dir / "modules_99_99.bzl"
    build = modules_dir / "BUILD.bazel"
    gen = modules_dir / "modules_build_files_generator.py"
    for p in (bzl, build, gen):
        if not p.is_file():
            raise SystemExit(f"missing: {p}")

    print(f"==> rewriting {bzl}", file=sys.stderr)
    old_version, entries = rewrite_modules_99_99(bzl, new_version)
    print(f"    old beta: 99.99.99.{old_version}", file=sys.stderr)
    print(f"    new beta: 99.99.99.{new_version}", file=sys.stderr)
    print(f"    {len(entries)} entries refreshed", file=sys.stderr)

    print(f"==> rewriting {build}", file=sys.stderr)
    n_build = rewrite_build_bazel(build, old_version, new_version)
    print(f"    {n_build} references replaced", file=sys.stderr)

    print(f"==> rewriting {gen}", file=sys.stderr)
    rewrite_generator(gen, old_version, new_version)

    if args.summary_out:
        lines = [
            f"**Old beta:** `99.99.99.{old_version}`",
            f"**New beta:** `99.99.99.{new_version}`",
            "",
            f"- `modules/modules_99_99.bzl`: refreshed {len(entries)} entries",
            f"- `modules/BUILD.bazel`: replaced {n_build} label references",
            f"- `modules/modules_build_files_generator.py`: updated redistimeseries 99.99 version",
            "",
            "| OS-Arch | SHA256 |",
            "| --- | --- |",
            *[f"| `{k}` | `{v}` |" for k, v in entries],
        ]
        Path(args.summary_out).write_text("\n".join(lines) + "\n")

    # Expose for the calling shell.
    print(f"OLD_VERSION=99.99.99.{old_version}")
    print(f"NEW_VERSION=99.99.99.{new_version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
