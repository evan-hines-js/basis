# Shared PKI fallback for basis dev scripts.
#
# rsync.sh syncs the whole tree to every other host — so the rsync'd
# checkout reliably has working certs at $REPO_ROOT/deploy/ansible/pki/.
# We use those as the fallback when the BASIS_TLS_* env vars are unset
# OR set to a stale path (e.g. an operator's .basis.credentials still
# pointing at /root/deploy/... from before the repo moved under
# ~/basis/). Without this fallback, the only signal is a generic
# "No such file" from the tonic TLS loader, which usually sends people
# on a long detour.
#
# Source this with `. "$REPO_ROOT/scripts/lib/pki.sh"` after
# $REPO_ROOT is defined. Then call `resolve_pki BASIS_TLS_CA ca.crt`
# (etc.) to materialise each env var, with fallback to
# $REPO_ROOT/deploy/ansible/pki/<filename>.

PKI_DEFAULT_DIR="$REPO_ROOT/deploy/ansible/pki"

resolve_pki() {
    local var="$1" filename="$2" current
    current="${!var:-}"
    if [[ -n "$current" && -f "$current" ]]; then
        return 0
    fi
    local fallback="$PKI_DEFAULT_DIR/$filename"
    if [[ -f "$fallback" ]]; then
        if [[ -n "$current" ]]; then
            echo "  warn: $var=$current does not exist; falling back to $fallback" >&2
        fi
        printf -v "$var" '%s' "$fallback"
        export "${var?}"
        return 0
    fi
    echo "FAIL: $var unset (or stale) and no fallback at $fallback" >&2
    exit 2
}
