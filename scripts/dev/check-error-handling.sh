#!/usr/bin/env bash
# Check for .expect(), .unwrap(), and panic!() in production Rust code
# Usage: ./scripts/dev/check-error-handling.sh [--verbose]
# Requires: gawk (GNU awk) for BEGINFILE support

set -euo pipefail

# shellcheck source=check-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/check-lib.sh"
require_gawk
cd "$PROJECT_ROOT"

VERBOSE="${1:-}"
MAX_EXPECT=$(parse_config_value "expect_calls" "max_allowed" "30")

echo "Checking error handling patterns in production code..."
echo ""

AWK_SCRIPT='
BEGINFILE {
    in_test_mod = 0
    in_test_fn = 0
    brace_depth = 0
}

# Track #[cfg(test)] modules
/^[[:space:]]*#\[cfg\(test\)\]/ { in_test_mod = 1 }

# Track #[test] functions (they end when brace depth returns to 0)
/^[[:space:]]*#\[test\]/ { in_test_fn = 1; brace_depth = 0 }

# Track brace depth for test functions
in_test_fn && /{/ { brace_depth += gsub(/{/, "{") }
in_test_fn && /}/ {
    brace_depth -= gsub(/}/, "}")
    if (brace_depth <= 0) in_test_fn = 0
}

# Check for patterns outside test code
!in_test_mod && !in_test_fn {
    if (/\.expect\(/) {
        expect_prod++
    }
    if (/\.unwrap\(\)/) {
        unwrap_prod++
        unwrap_locations[unwrap_prod] = FILENAME ":" FNR ": " $0
    }
    if (/panic!\(/) {
        panic_prod++
        panic_locations[panic_prod] = FILENAME ":" FNR ": " $0
    }
}

# Count test code occurrences
in_test_mod || in_test_fn {
    if (/\.expect\(/) expect_test++
    if (/\.unwrap\(\)/) unwrap_test++
    if (/panic!\(/) panic_test++
}

END {
    print ""
    print "=== Error Handling Pattern Summary ==="
    print ""
    printf "%-12s %12s %12s\n", "Pattern", "Production", "Test Code"
    printf "%-12s %12s %12s\n", "--------", "----------", "---------"
    printf "%-12s %12d %12d\n", ".expect()", expect_prod+0, expect_test+0
    printf "%-12s %12d %12d\n", ".unwrap()", unwrap_prod+0, unwrap_test+0
    printf "%-12s %12d %12d\n", "panic!()", panic_prod+0, panic_test+0
    print ""

    total_violations = unwrap_prod + panic_prod
    if (total_violations > 0) {
        if (unwrap_prod > 0) {
            print ".unwrap() violations:"
            for (i = 1; i <= unwrap_prod; i++) print "  " unwrap_locations[i]
        }
        if (panic_prod > 0) {
            print "panic!() violations:"
            for (i = 1; i <= panic_prod; i++) print "  " panic_locations[i]
        }
        print ""
        print "FAILED: " total_violations " violation(s) found in production code"
        print "(.expect() is allowed as it documents invariants)"
        exit 1
    } else if (expect_prod > max_expect) {
        print "FAILED: .expect() count " expect_prod " exceeds max " max_expect
        print "Update check-config.toml [expect_calls] max_allowed if intentional"
        exit 1
    } else {
        print "PASSED: No violations in production code"
        print "(.expect() count: " expect_prod+0 "/" max_expect " allowed)"
        exit 0
    }
}
'

# Run the check
# Exclude: build.rs (compile-time), tests/ (E2E tests), benches/ (benchmarks),
#          tests.rs (always #[cfg(test)] modules referenced from parent mod.rs)
if [[ "$VERBOSE" == "--verbose" || "$VERBOSE" == "-v" ]]; then
    find crates -name "*.rs" \
        -not -name "build.rs" \
        -not -name "tests.rs" \
        -not -path "*/tests/*" \
        -not -path "*/benches/*" \
        -print0 | xargs -0 gawk -v verbose=1 -v max_expect="$MAX_EXPECT" "$AWK_SCRIPT"
else
    find crates -name "*.rs" \
        -not -name "build.rs" \
        -not -name "tests.rs" \
        -not -path "*/tests/*" \
        -not -path "*/benches/*" \
        -print0 | xargs -0 gawk -v verbose=0 -v max_expect="$MAX_EXPECT" "$AWK_SCRIPT"
fi
