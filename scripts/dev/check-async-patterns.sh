#!/usr/bin/env bash
# Check for common async anti-patterns in Rust code
# Usage: ./scripts/dev/check-async-patterns.sh
# Requires: gawk (GNU awk) for BEGINFILE support

set -euo pipefail

# shellcheck source=check-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/check-lib.sh"
require_gawk
cd "$PROJECT_ROOT"

echo "Checking async patterns..."
echo ""

VIOLATIONS=0

# =============================================================================
# Check 1: std::sync::Mutex in async code (should use tokio::sync::Mutex)
# =============================================================================
echo "=== Checking for std::sync::Mutex in async code ==="

MUTEX_AWK='
BEGINFILE { in_test_mod = 0; has_async = 0 }
/^[[:space:]]*#\[cfg\(test\)\]/ { in_test_mod = 1 }
/async fn/ { has_async = 1 }
!in_test_mod && has_async && /std::sync::Mutex/ {
    print FILENAME ":" FNR ": " $0
    found++
}
'

MUTEX_MATCHES=$(find crates -name "*.rs" -not -name "build.rs" -not -path "*/tests/*" -exec gawk "$MUTEX_AWK" {} + 2>/dev/null || true)
if [[ -n "$MUTEX_MATCHES" ]]; then
    echo "$MUTEX_MATCHES" | head -10
    echo -e "${RED}FAILED: std::sync::Mutex found in async code${NC}"
    VIOLATIONS=$((VIOLATIONS + 1))
else
    echo -e "${GREEN}PASSED: No std::sync::Mutex in async production code${NC}"
fi
echo ""

# =============================================================================
# Check 2: Blocking operations in async context
# =============================================================================
echo "=== Checking for blocking I/O in async code ==="

mapfile -t BLOCKING_ALLOWED < <(parse_allowed_patterns "blocking_io")

BLOCKING_AWK="${AWK_TEST_TRACKING}"'
!in_test_mod && /std::fs::|std::thread::sleep|std::io::stdin|std::io::stdout/ {
    print FILENAME ":" FNR ": " $0
}
'

BLOCKING_MATCHES=$(find crates -name "*.rs" -not -name "build.rs" -not -path "*/tests/*" -exec gawk "$BLOCKING_AWK" {} + 2>/dev/null || true)
BLOCKING_VIOLATIONS=$(filter_allowed "$BLOCKING_MATCHES" "${BLOCKING_ALLOWED[@]+"${BLOCKING_ALLOWED[@]}"}")

if [[ -z "$BLOCKING_VIOLATIONS" ]]; then
    echo -e "${GREEN}PASSED: No unauthorized blocking I/O in production code${NC}"
else
    echo "$BLOCKING_VIOLATIONS"
    echo -e "${RED}FAILED: Unauthorized blocking I/O found${NC}"
    echo "Add to $CONFIG_FILE [blocking_io.allowed] if this is intentional"
    VIOLATIONS=$((VIOLATIONS + 1))
fi
echo ""

# =============================================================================
# Check 3: Look for .await inside lock guards (potential deadlock)
# =============================================================================
echo "=== Checking for .await while holding locks ==="

LOCK_AWAIT_AWK='
/\.(read|write|lock)\(\)\.await/ {
    lock_line = NR
}
/\.await/ && NR == lock_line {
    count = gsub(/\.await/, ".await")
    if (count > 1) {
        print FILENAME ":" NR ": potential lock held across await"
        found++
    }
}
'

LOCK_MATCHES=$(find crates -name "*.rs" -not -path "*/tests/*" -exec gawk "$LOCK_AWAIT_AWK" {} + 2>/dev/null || true)
if [[ -z "$LOCK_MATCHES" ]]; then
    echo -e "${GREEN}PASSED: No obvious locks held across .await${NC}"
else
    echo "$LOCK_MATCHES"
    echo -e "${RED}FAILED: Locks held across .await (potential deadlock)${NC}"
    VIOLATIONS=$((VIOLATIONS + 1))
fi
echo ""

# =============================================================================
# Check 4: Verify tokio runtime usage
# =============================================================================
echo "=== Checking tokio runtime configuration ==="
if grep -rq '#\[tokio::main\]' crates --include="*.rs"; then
    echo -e "${GREEN}PASSED: Using tokio runtime${NC}"
else
    echo -e "${RED}FAILED: No #[tokio::main] found${NC}"
    VIOLATIONS=$((VIOLATIONS + 1))
fi
echo ""

# =============================================================================
# Check 5: Check for proper cancellation handling
# =============================================================================
echo "=== Checking for cancellation token patterns ==="
if grep -rq 'CancellationToken\|shutdown.*oneshot\|shutdown.*mpsc' crates --include="*.rs"; then
    echo -e "${GREEN}PASSED: Cancellation/shutdown patterns found${NC}"
else
    echo -e "${YELLOW}WARNING: No obvious cancellation handling found${NC}"
fi
echo ""

# =============================================================================
# Summary
# =============================================================================
echo "=== Async Pattern Check Summary ==="
if [[ "$VIOLATIONS" -eq 0 ]]; then
    echo -e "${GREEN}All async pattern checks passed${NC}"
    exit 0
else
    echo -e "${RED}$VIOLATIONS violation(s) found${NC}"
    exit 1
fi
