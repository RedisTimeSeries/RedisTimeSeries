#!/usr/bin/env python3
"""
One-off script to check which tests from all-tests.txt appear in jammy.*.log files.
"""

import glob
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def main():
    # Read all test names from all-tests.txt
    tests_file = os.path.join(SCRIPT_DIR, "all-tests.txt")
    with open(tests_file, "r") as f:
        test_names = [line.strip() for line in f if line.strip()]

    print(f"Total test names in all-tests.txt: {len(test_names)}")
    print("=" * 60)

    # Find all jammy.*.log files
    log_pattern = os.path.join(SCRIPT_DIR, "jammy.*.log")
    log_files = sorted(glob.glob(log_pattern))

    if not log_files:
        print("No jammy.*.log files found.")
        return

    print(f"Found {len(log_files)} log file(s)\n")

    for log_file in log_files:
        log_name = os.path.basename(log_file)

        # Read log file content
        with open(log_file, "r", errors="replace") as f:
            log_content = f.read()

        found = []
        missing = []

        for test_name in test_names:
            if test_name in log_content:
                found.append(test_name)
            else:
                missing.append(test_name)

        print(f"Log file: {log_name}")
        print(f"  Found:   {len(found)}")
        print(f"  Missing: {len(missing)}")

        if missing:
            print("  Missing tests:")
            for name in missing:
                print(f"    - {name}")
        print()

if __name__ == "__main__":
    main()
