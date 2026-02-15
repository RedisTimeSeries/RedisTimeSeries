#!/bin/bash
# Parse RLTest/unittest output to extract test counts
# Expected format: "Ran X tests..." followed by "OK" or "OK (skipped=Z)" or "FAILED (failures=W, errors=V)"
# Usage: parse-test-results.sh <test_output_file> <output_json_file>

set -e

INPUT_FILE="${1}"
OUTPUT_FILE="${2:-test-results.json}"

PASSED=0
FAILED=0
SKIPPED=0
TOTAL=0

if [ -n "$INPUT_FILE" ] && [ -f "$INPUT_FILE" ]; then
    TEST_OUTPUT=$(cat "$INPUT_FILE")
else
    TEST_OUTPUT=$(cat)
fi

TOTAL=$(echo "$TEST_OUTPUT" | grep -iE "ran [0-9]+ test" | grep -oE "[0-9]+" | head -1 | tr -d '\n\r ' || echo "0")
TOTAL=${TOTAL:-0}

if echo "$TEST_OUTPUT" | grep -qiE "^\s*ok\s*$|^\s*ok\s*\(|^ok"; then
    SKIPPED=$(echo "$TEST_OUTPUT" | grep -iE "ok.*skipped" | grep -oE "skipped[=:][ ]*[0-9]+" | grep -oE "[0-9]+" | head -1 | tr -d '\n\r ' || echo "0")
    SKIPPED=${SKIPPED:-0}
    if [ "$TOTAL" != "0" ] && [ -n "$TOTAL" ]; then
        PASSED=$((TOTAL - SKIPPED))
    fi
elif echo "$TEST_OUTPUT" | grep -qiE "^\s*failed\s*$|^\s*failed\s*\(|^failed"; then
    FAILED=$(echo "$TEST_OUTPUT" | grep -iE "failed.*failures" | grep -oE "failures?[=:][ ]*[0-9]+" | grep -oE "[0-9]+" | head -1 | tr -d '\n\r ' || echo "0")
    FAILED=${FAILED:-0}
    ERRORS=$(echo "$TEST_OUTPUT" | grep -iE "failed.*errors" | grep -oE "errors?[=:][ ]*[0-9]+" | grep -oE "[0-9]+" | head -1 | tr -d '\n\r ' || echo "0")
    ERRORS=${ERRORS:-0}
    FAILED=$((FAILED + ERRORS))

    SKIPPED=$(echo "$TEST_OUTPUT" | grep -iE "skipped" | grep -oE "skipped[=:][ ]*[0-9]+" | grep -oE "[0-9]+" | head -1 | tr -d '\n\r ' || echo "0")
    SKIPPED=${SKIPPED:-0}

    if [ "$TOTAL" != "0" ] && [ -n "$TOTAL" ]; then
        PASSED=$((TOTAL - FAILED - SKIPPED))
    fi
fi

# Fallback: count individual test lines if summary parsing failed
# Check SKIPPED=0 to avoid overwriting valid "all tests skipped" results
if [ "$TOTAL" = "0" ] || ([ "$PASSED" = "0" ] && [ "$FAILED" = "0" ] && [ "$SKIPPED" = "0" ]); then
    PASSED_COUNT=$(echo "$TEST_OUTPUT" | grep -cE "(test_\w+.*\bok\b|test_\w+.*\bPASS\b|\[PASS\]|\[OK\])" 2>/dev/null || echo "0")
    PASSED=$(echo "$PASSED_COUNT" | tr -d '\n\r ' | grep -oE '^[0-9]+' | head -1)
    if [ -z "$PASSED" ] || ! [ "$PASSED" -eq "$PASSED" ] 2>/dev/null; then
        PASSED=0
    fi

    FAILED_COUNT=$(echo "$TEST_OUTPUT" | grep -cE "(test_\w+.*\bFAIL\b|test_\w+.*\bERROR\b|\[FAIL\]|\[ERROR\])" 2>/dev/null || echo "0")
    FAILED=$(echo "$FAILED_COUNT" | tr -d '\n\r ' | grep -oE '^[0-9]+' | head -1)
    if [ -z "$FAILED" ] || ! [ "$FAILED" -eq "$FAILED" ] 2>/dev/null; then
        FAILED=0
    fi

    SKIPPED_COUNT=$(echo "$TEST_OUTPUT" | grep -cE "(test_\w+.*\bSKIP\b|\[SKIP\])" 2>/dev/null || echo "0")
    SKIPPED=$(echo "$SKIPPED_COUNT" | tr -d '\n\r ' | grep -oE '^[0-9]+' | head -1)
    if [ -z "$SKIPPED" ] || ! [ "$SKIPPED" -eq "$SKIPPED" ] 2>/dev/null; then
        SKIPPED=0
    fi

    TOTAL=$((PASSED + FAILED + SKIPPED)) 2>/dev/null || TOTAL=0
fi

# Sanitize and validate numeric values
PASSED=$(echo "$PASSED" | tr -d '\n\r ' | head -1)
FAILED=$(echo "$FAILED" | tr -d '\n\r ' | head -1)
SKIPPED=$(echo "$SKIPPED" | tr -d '\n\r ' | head -1)
TOTAL=$(echo "$TOTAL" | tr -d '\n\r ' | head -1)

PASSED=${PASSED:-0}
FAILED=${FAILED:-0}
SKIPPED=${SKIPPED:-0}
if [ -z "$TOTAL" ] || [ "$TOTAL" = "" ]; then
    TOTAL=$((PASSED + FAILED + SKIPPED))
fi

if ! [ "$PASSED" -eq "$PASSED" ] 2>/dev/null; then PASSED=0; fi
if ! [ "$FAILED" -eq "$FAILED" ] 2>/dev/null; then FAILED=0; fi
if ! [ "$SKIPPED" -eq "$SKIPPED" ] 2>/dev/null; then SKIPPED=0; fi
if ! [ "$TOTAL" -eq "$TOTAL" ] 2>/dev/null; then TOTAL=$((PASSED + FAILED + SKIPPED)); fi

OUTPUT_FILE_DIR=$(dirname "$OUTPUT_FILE" 2>/dev/null || echo ".")
mkdir -p "$OUTPUT_FILE_DIR" 2>/dev/null || true

printf '{"passed":%d,"failed":%d,"skipped":%d,"total":%d}\n' "$PASSED" "$FAILED" "$SKIPPED" "$TOTAL" > "$OUTPUT_FILE"

echo "Parsed test results: Passed=$PASSED, Failed=$FAILED, Skipped=$SKIPPED, Total=$TOTAL"

