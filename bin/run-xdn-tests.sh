#!/bin/bash

# Script used to run XDN integration tests with per-method JVM isolation.
# This script is used because each test method that calls
# cluster.start() and cluster.close() needs to run in its own JVM.
# usage example :
#   ./bin/run-xdn-tests.sh                        # Run all Xdn*Test classes
#   ./bin/run-xdn-tests.sh XdnGetReplicaInfoTest  # Run specific test class

set -euo pipefail

CLASSPATH="lib/junit-platform-console-standalone-1.11.1.jar:build/classes:build/test-classes:lib/*"
TEST_CLASSES_DIR="build/test-classes"
OUTPUT_DIR="out/junit5-test-output"
TEST_PATTERN="${1:-Xdn*Test}"
VERBOSE="${VERBOSE:-false}"

mkdir -p "$OUTPUT_DIR"

FAILED=0
PASSED=0
SKIPPED=0

TEST_CLASSES=$(find "$TEST_CLASSES_DIR" -name "${TEST_PATTERN}.class" | grep -v '\$' || true)

if [ -z "$TEST_CLASSES" ]; then
    echo "No test class found matching pattern: $TEST_PATTERN"
    exit 1
fi

for CLASS_FILE in $TEST_CLASSES; do
    # To convert file path
    # e.g.,
    #   CLASS_FILE=build/test-classes/com/example/MyTest.class
    #   TEST_CLASSES_DIR=build/test-classes
    #   CLASS_NAME=com.example.MyTest
    CLASS_NAME=$(echo "$CLASS_FILE" | sed "s|$TEST_CLASSES_DIR/||; s|/|.|g; s|\.class$||")

   # Test method are executed if they match the pattern void test*()
    METHODS=$(javap -cp "build/classes:build/test-classes:lib/*" "$CLASS_NAME" 2>/dev/null | \
        grep -oE 'void test[A-Za-z0-9_]+\(' | sed 's/void //; s/($//' || true)

    if [ -z "$METHODS" ]; then
        echo "No test method found in $CLASS_NAME"
        continue
    fi

    for METHOD in $METHODS; do
        echo "Running: $CLASS_NAME#$METHOD"

        # rm -rf /tmp/gigapaxos /tmp/xdn 2>/dev/null || true

        LOG_FILE="$OUTPUT_DIR/${CLASS_NAME}_${METHOD}.log"

        set +e

        if [ "$VERBOSE" = "true" ]; then
            java -cp "$CLASSPATH" \
                org.junit.platform.console.ConsoleLauncher execute \
                --select-method "$CLASS_NAME#$METHOD" \
                --details=verbose 2>&1 | tee "$LOG_FILE"
        else
            java -cp "$CLASSPATH" \
                org.junit.platform.console.ConsoleLauncher execute \
                --select-method "$CLASS_NAME#$METHOD" \
                --details=verbose \
                > "$LOG_FILE" 2>&1
        fi

        set -e

        # FAILED
        if grep -qE "\[\s*[1-9][0-9]* (tests|containers) failed" "$LOG_FILE"; then
            echo "FAILED: $CLASS_NAME#$METHOD"

            if [ "$VERBOSE" != "true" ]; then
                echo "--- Last 30 lines of log ---"
                tail -30 "$LOG_FILE"
                echo "--- End log ---"
            fi
            ((FAILED++)) || true

        # SKIPPED / ABORTED
        elif grep -qE "\[\s*[1-9][0-9]* (tests|containers) (skipped|aborted)" "$LOG_FILE"; then
            echo "SKIPPED: $CLASS_NAME#$METHOD"
            ((SKIPPED++)) || true

        # PASSED
        elif grep -qE "\[\s*[1-9][0-9]* tests successful" "$LOG_FILE"; then
            echo "PASSED: $CLASS_NAME#$METHOD"
            ((PASSED++)) || true

        # NO TESTS FOUND
        elif grep -q "0 tests found" "$LOG_FILE"; then
            echo "SKIPPED (NO TEST FOUND): $CLASS_NAME#$METHOD"
            ((SKIPPED++)) || true

        # DEAD JVM / CRASH
        else
            echo "FAILED (CRASH/UNKNOWN): $CLASS_NAME#$METHOD"
            echo "JUnit summary not found. JVM might have crashed."
            echo "Check log: $LOG_FILE"
            ((FAILED++)) || true
        fi
    done
done

#rm -rf /tmp/gigapaxos /tmp/xdn 2>/dev/null || true

echo ""
echo "SUMMARY: $PASSED passed, $FAILED failed, $SKIPPED skipped"

[ $FAILED -eq 0 ]
