#!/usr/bin/env python3

import argparse
import re
import sys
from pathlib import Path


def validate_version(version):
    """Validate version format (MAJOR.MINOR.PATCH)"""
    pattern = r'^\d+\.\d+\.\d+$'
    if not re.match(pattern, version):
        raise ValueError(f"Invalid version format: {version}. Expected format: MAJOR.MINOR.PATCH")
    return version


def parse_version(version):
    """Parse version string into major, minor, patch components"""
    parts = version.split('.')
    return int(parts[0]), int(parts[1]), int(parts[2])


def update_version_file(version_file, major, minor, patch):
    """Update version.h file with new version numbers"""
    if not version_file.exists():
        raise FileNotFoundError(f"Version file not found: {version_file}")

    content = version_file.read_text()

    content = re.sub(
        r'(#define REDISTIMESERIES_VERSION_MAJOR )\d+\n'
        r'(#define REDISTIMESERIES_VERSION_MINOR )\d+\n'
        r'(#define REDISTIMESERIES_VERSION_PATCH )\d+',
        rf'\g<1>{major}\n\g<2>{minor}\n\g<3>{patch}',
        content
    )

    version_file.write_text(content)
    print(f"Updated {version_file} to version {major}.{minor}.{patch}")


def get_current_version(version_file):
    """Extract current version from version.h"""
    if not version_file.exists():
        raise FileNotFoundError(f"Version file not found: {version_file}")

    content = version_file.read_text()

    match = re.search(
        r'#define REDISTIMESERIES_VERSION_MAJOR (\d+)\n'
        r'#define REDISTIMESERIES_VERSION_MINOR (\d+)\n'
        r'#define REDISTIMESERIES_VERSION_PATCH (\d+)',
        content
    )

    if not match:
        raise ValueError("Could not parse version from version.h")

    return f"{match.group(1)}.{match.group(2)}.{match.group(3)}"


def main():
    parser = argparse.ArgumentParser(description='Set RedisTimeSeries version')
    parser.add_argument('version', help='New version in format MAJOR.MINOR.PATCH')
    parser.add_argument('--version-file', default='src/version.h',
                        help='Path to version.h file (default: src/version.h)')

    args = parser.parse_args()

    version_file_path = Path(args.version_file)
    if version_file_path.is_absolute():
        version_file = version_file_path
    else:
        repo_root = Path(__file__).parent.parent
        version_file = repo_root / args.version_file

    try:
        validate_version(args.version)
        major, minor, patch = parse_version(args.version)

        current = get_current_version(version_file)
        print(f"Current version: {current}")
        print(f"New version: {args.version}")

        update_version_file(version_file, major, minor, patch)

        print(f"Successfully bumped version to {args.version}")
        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())

