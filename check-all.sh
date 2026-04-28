#!/usr/bin/env bash
# Run all code quality checks
# Usage: ./scripts/check-all.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

FAILED=0

run_check() {
    local name="$1"
    local script="$2"

    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}Running: $name${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""

    if "$SCRIPT_DIR/$script"; then
        echo ""
        echo -e "${GREEN}✓ $name passed${NC}"
    else
        echo ""
        echo -e "${RED}✗ $name failed${NC}"
        FAILED=$((FAILED + 1))
    fi
}

echo -e "${BLUE}"
echo "╔════════════════════════════════════════════════════════════╗"
echo "║              Lattice Code Quality Checks                   ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

run_check "Error Handling Patterns" "check-error-handling.sh"
run_check "Security Patterns" "check-security-patterns.sh"
run_check "Async Patterns" "check-async-patterns.sh"

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Summary${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

if [[ "$FAILED" -eq 0 ]]; then
    echo -e "${GREEN}All checks passed!${NC}"
    exit 0
else
    echo -e "${RED}$FAILED check(s) failed${NC}"
    exit 1
fi
