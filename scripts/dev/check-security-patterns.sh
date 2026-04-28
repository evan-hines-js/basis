#!/usr/bin/env bash
# Check for common security anti-patterns in Rust code
# Usage: ./scripts/dev/check-security-patterns.sh
# Requires: gawk (GNU awk) for BEGINFILE support

set -euo pipefail

# shellcheck source=check-lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/check-lib.sh"
require_gawk
cd "$PROJECT_ROOT"

echo "Running security pattern checks..."
echo ""

VIOLATIONS=0

# =============================================================================
# Check 1: Hardcoded secrets patterns
# =============================================================================
echo "=== Checking for potential hardcoded secrets ==="

mapfile -t SECRET_IGNORE < <(parse_array_patterns "hardcoded_secrets.ignore_patterns" "patterns")

SECRETS_AWK="${AWK_TEST_TRACKING}"'
!in_test_mod && /(password|secret|api_key|apikey|credential)[[:space:]]*=[[:space:]]*"[^"]+"/ {
    print FILENAME ":" FNR ": " $0
}
'

SECRETS_MATCHES=$(find crates -name "*.rs" -not -path "*/tests/*" -exec gawk "$SECRETS_AWK" {} + 2>/dev/null || true)
SECRETS_VIOLATIONS=$(filter_allowed "$SECRETS_MATCHES" "${SECRET_IGNORE[@]+"${SECRET_IGNORE[@]}"}")

if [[ -z "$SECRETS_VIOLATIONS" ]]; then
    echo -e "${GREEN}PASSED: No obvious hardcoded secrets${NC}"
else
    echo "$SECRETS_VIOLATIONS" | head -10
    echo -e "${RED}FAILED: Potential hardcoded secrets found${NC}"
    VIOLATIONS=$((VIOLATIONS + 1))
fi
echo ""

# =============================================================================
# Check 2: Weak crypto algorithms
# =============================================================================
echo "=== Checking for weak cryptographic algorithms ==="

mapfile -t WEAK_CRYPTO_ALLOWED < <(parse_allowed_patterns "weak_crypto")

WEAK_CRYPTO_AWK="${AWK_TEST_TRACKING}"'
!in_test_mod && /\b(md5|sha1|sha-1|des|3des|rc4|arcfour|blowfish)\b/ {
    # Skip comments
    if (/^[[:space:]]*(\/\/|\/\*|\*)/) next
    print FILENAME ":" FNR ": " $0
}
'

WEAK_MATCHES=$(find crates -name "*.rs" -not -path "*/tests/*" -exec gawk "$WEAK_CRYPTO_AWK" {} + 2>/dev/null || true)
WEAK_VIOLATIONS=$(filter_allowed "$WEAK_MATCHES" "${WEAK_CRYPTO_ALLOWED[@]+"${WEAK_CRYPTO_ALLOWED[@]}"}")

if [[ -z "$WEAK_VIOLATIONS" ]]; then
    echo -e "${GREEN}PASSED: No weak cryptographic algorithms${NC}"
else
    echo "$WEAK_VIOLATIONS" | head -10
    echo -e "${RED}FAILED: Weak cryptographic algorithms found${NC}"
    VIOLATIONS=$((VIOLATIONS + 1))
fi
echo ""

# =============================================================================
# Check 3: Verify FIPS crypto backend
# =============================================================================
echo "=== Checking FIPS crypto configuration ==="
if grep -q 'features.*=.*\["aws-lc-rs"' Cargo.toml && grep -q 'aws-lc-rs' Cargo.toml; then
    echo -e "${GREEN}PASSED: aws-lc-rs FIPS backend configured${NC}"
else
    echo -e "${RED}FAILED: aws-lc-rs FIPS backend not properly configured${NC}"
    VIOLATIONS=$((VIOLATIONS + 1))
fi
echo ""

# =============================================================================
# Check 4: Unsafe blocks
# =============================================================================
echo "=== Checking for unsafe blocks ==="

mapfile -t UNSAFE_ALLOWED < <(parse_allowed_patterns "unsafe_blocks")

UNSAFE_AWK="${AWK_TEST_TRACKING}"'
!in_test_mod && /unsafe[[:space:]]*\{/ {
    print FILENAME ":" FNR ": " $0
}
'

UNSAFE_MATCHES=$(find crates -name "*.rs" -not -path "*/tests/*" -exec gawk "$UNSAFE_AWK" {} + 2>/dev/null || true)
UNSAFE_VIOLATIONS=$(filter_allowed "$UNSAFE_MATCHES" "${UNSAFE_ALLOWED[@]+"${UNSAFE_ALLOWED[@]}"}")

if [[ -z "$UNSAFE_VIOLATIONS" ]]; then
    echo -e "${GREEN}PASSED: No unauthorized unsafe blocks in production code${NC}"
else
    echo "$UNSAFE_VIOLATIONS" | head -10
    echo -e "${RED}FAILED: Unauthorized unsafe blocks found${NC}"
    echo "Add to $CONFIG_FILE [unsafe_blocks.allowed] if this is intentional"
    VIOLATIONS=$((VIOLATIONS + 1))
fi
echo ""

# =============================================================================
# Check 5: SQL/Command injection patterns
# =============================================================================
echo "=== Checking for potential injection vulnerabilities ==="

INJECTION_AWK="${AWK_TEST_TRACKING}"'
!in_test_mod && /execute\s*\(\s*&format!/ {
    print FILENAME ":" FNR ": " $0
}
'

INJECTION_MATCHES=$(find crates -name "*.rs" -not -path "*/tests/*" -exec gawk "$INJECTION_AWK" {} + 2>/dev/null || true)
if [[ -z "$INJECTION_MATCHES" ]]; then
    echo -e "${GREEN}PASSED: No obvious injection patterns${NC}"
else
    echo "$INJECTION_MATCHES" | head -10
    echo -e "${RED}FAILED: Potential injection vulnerability patterns found${NC}"
    VIOLATIONS=$((VIOLATIONS + 1))
fi
echo ""

# =============================================================================
# Check 6: Verify TLS configuration
# =============================================================================
echo "=== Checking TLS configuration ==="
HAS_RUSTLS=$(grep -rE 'rustls-tls|tls-rustls' Cargo.toml crates/*/Cargo.toml 2>/dev/null || true)
HAS_NATIVE=$(grep -rE 'native-tls' Cargo.toml crates/*/Cargo.toml 2>/dev/null || true)
if [[ -n "$HAS_RUSTLS" ]] && [[ -z "$HAS_NATIVE" ]]; then
    echo -e "${GREEN}PASSED: Using rustls-tls (not native-tls)${NC}"
elif [[ -n "$HAS_NATIVE" ]]; then
    echo -e "${RED}FAILED: native-tls found - use rustls-tls instead${NC}"
    echo "$HAS_NATIVE"
    VIOLATIONS=$((VIOLATIONS + 1))
else
    echo -e "${YELLOW}WARNING: No TLS configuration found${NC}"
fi
echo ""

# =============================================================================
# Summary
# =============================================================================
echo "=== Security Check Summary ==="
if [[ "$VIOLATIONS" -eq 0 ]]; then
    echo -e "${GREEN}All security checks passed${NC}"
    exit 0
else
    echo -e "${RED}$VIOLATIONS security violation(s) found${NC}"
    exit 1
fi
