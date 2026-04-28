#!/usr/bin/env bash
# Common library for check scripts
# Source this file: source "$(dirname "${BASH_SOURCE[0]}")/check-lib.sh"

# Require gawk
require_gawk() {
    if ! command -v gawk &> /dev/null; then
        echo "Error: gawk is required but not installed."
        echo "Install with: apt-get install gawk (Ubuntu) or brew install gawk (macOS)"
        exit 1
    fi
}

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONFIG_FILE="$SCRIPT_DIR/check-config.toml"

# Parse allowed patterns from [section.allowed] in config
# Usage: mapfile -t PATTERNS < <(parse_allowed_patterns "blocking_io")
parse_allowed_patterns() {
    local section=$1
    if [[ ! -f "$CONFIG_FILE" ]]; then
        return
    fi
    gawk -v section="[$section.allowed]" '
        $0 == section { in_section=1; next }
        /^\[/ { in_section=0 }
        in_section && /^"[^"]+"/ {
            gsub(/^"|".*$/, "", $1)
            print $1
        }
    ' "$CONFIG_FILE"
}

# Parse array from config (for patterns = [...])
# Usage: mapfile -t PATTERNS < <(parse_array_patterns "hardcoded_secrets.ignore_patterns" "patterns")
parse_array_patterns() {
    local section=$1
    local key=$2
    if [[ ! -f "$CONFIG_FILE" ]]; then
        return
    fi
    gawk -v section="[$section]" -v key="$key" '
        $0 == section { in_section=1; next }
        /^\[/ { in_section=0 }
        in_section && $1 == key { in_array=1; next }
        in_array && /\]/ { in_array=0 }
        in_array && /^[[:space:]]*"[^"]+"/ {
            gsub(/^[[:space:]]*"|".*$/, "")
            print
        }
    ' "$CONFIG_FILE"
}

# Parse a single value from config
# Usage: MAX=$(parse_config_value "expect_calls" "max_allowed" "30")
parse_config_value() {
    local section=$1
    local key=$2
    local default=$3
    if [[ ! -f "$CONFIG_FILE" ]]; then
        echo "$default"
        return
    fi
    local value
    value=$(gawk -v section="[$section]" -v key="$key" '
        $0 == section { in_section=1; next }
        /^\[/ { in_section=0 }
        in_section && $1 == key { gsub(/[^0-9]/, "", $3); print $3 }
    ' "$CONFIG_FILE")
    if [[ -n "$value" ]]; then
        echo "$value"
    else
        echo "$default"
    fi
}

# Check if a match is in the allowed list
# Usage: if is_allowed "$match" "${PATTERNS[@]}"; then ...
is_allowed() {
    local match=$1
    shift
    local -a patterns=("$@")
    for pattern in "${patterns[@]}"; do
        if [[ "$match" == *"$pattern"* ]]; then
            return 0
        fi
    done
    return 1
}

# Filter matches against allowed patterns, returning only violations
# Usage: VIOLATIONS=$(filter_allowed "$MATCHES" "${PATTERNS[@]}")
filter_allowed() {
    local matches=$1
    shift
    local -a patterns=("$@")
    local violations=""

    if [[ -z "$matches" ]]; then
        return
    fi

    while IFS= read -r match; do
        if [[ -n "$match" ]] && ! is_allowed "$match" "${patterns[@]+"${patterns[@]}"}"; then
            violations="${violations}${match}"$'\n'
        fi
    done <<< "$matches"

    echo -n "$violations"
}

# AWK preamble for tracking test modules
AWK_TEST_TRACKING='
BEGINFILE { in_test_mod = 0 }
/^[[:space:]]*#\[cfg\(test\)\]/ { in_test_mod = 1 }
'
