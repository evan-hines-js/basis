#!/usr/bin/env bash
# Sets up an nginx TCP proxy so docker-compose services (Vault, Keycloak,
# registry mirrors) and optionally the K8s API server are reachable from
# other machines on the LAN.
#
# Usage: sudo ./setup-k8s-proxy.sh [K8S_API_PORT]
#
# The K8s API proxy is optional:
#   - Pass the port as $1 or set K8S_API_PORT env var
#   - If neither is set, skips the K8s API proxy
#
# Proxied services (always):
#   - Vault:          <lan-ip>:8200 -> 127.0.0.1:8200
#   - Keycloak:       <lan-ip>:8080 -> 127.0.0.1:8080
#   - Docker mirror:  <lan-ip>:5555 -> 127.0.0.1:5555
#   - GHCR mirror:    <lan-ip>:5556 -> 127.0.0.1:5556
#
# Proxied services (when K8s port provided):
#   - K8s API:        <lan-ip>:6443 -> 127.0.0.1:<K8S_API_PORT>

set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
    echo "Run as root: sudo $0"
    exit 1
fi

K8S_PORT="${1:-${K8S_API_PORT:-}}"

echo "==> Installing nginx..."
apt-get update -qq
apt-get install -y -qq nginx libnginx-mod-stream

echo "==> Configuring nginx stream proxy..."
cat > /etc/nginx/k8s-stream-proxy.conf <<CONF
stream {
CONF

# K8s API proxy (optional)
if [ -n "$K8S_PORT" ]; then
    cat >> /etc/nginx/k8s-stream-proxy.conf <<CONF
    # K8s API server
    upstream k8s_api {
        server 127.0.0.1:${K8S_PORT};
    }
    server {
        listen 0.0.0.0:6443;
        proxy_pass k8s_api;
    }

CONF
    echo "    K8s API proxy: 6443 -> 127.0.0.1:${K8S_PORT}"
else
    echo "    K8s API proxy: skipped (no port provided)"
fi

# Docker-compose services (always proxied)
cat >> /etc/nginx/k8s-stream-proxy.conf <<'CONF'
    # Vault (docker-compose)
    upstream vault {
        server 127.0.0.1:8200;
    }
    server {
        listen 0.0.0.0:8200;
        proxy_pass vault;
    }

    # Keycloak (docker-compose)
    upstream keycloak {
        server 127.0.0.1:8080;
    }
    server {
        listen 0.0.0.0:8080;
        proxy_pass keycloak;
    }

    # Docker Hub registry mirror (docker-compose)
    upstream docker_mirror {
        server 127.0.0.1:5555;
    }
    server {
        listen 0.0.0.0:5555;
        proxy_pass docker_mirror;
    }

    # GHCR registry mirror (docker-compose)
    upstream ghcr_mirror {
        server 127.0.0.1:5556;
    }
    server {
        listen 0.0.0.0:5556;
        proxy_pass ghcr_mirror;
    }
}
CONF

if ! grep -q 'k8s-stream-proxy.conf' /etc/nginx/nginx.conf; then
    echo 'include /etc/nginx/k8s-stream-proxy.conf;' >> /etc/nginx/nginx.conf
fi

rm -f /etc/nginx/conf.d/k8s-proxy-stream.conf
rm -f /etc/nginx/sites-enabled/default

echo "==> Testing nginx config..."
nginx -t

echo "==> Restarting nginx..."
systemctl restart nginx
systemctl enable nginx

LAN_IP=$(hostname -I | awk '{print $1}')

# Update kubeconfig if K8s proxy is active
if [ -n "$K8S_PORT" ]; then
    REAL_USER="${SUDO_USER:-$USER}"
    echo "==> Updating kubeconfig for external access..."
    sudo -u "$REAL_USER" kubectl config set-cluster management \
        --server="https://${LAN_IP}:6443" \
        --insecure-skip-tls-verify=true
fi

echo
echo "==> Done! Proxied services on ${LAN_IP}:"
if [ -n "$K8S_PORT" ]; then
    echo "    K8s API:       https://${LAN_IP}:6443 -> 127.0.0.1:${K8S_PORT}"
fi
echo "    Vault:         http://${LAN_IP}:8200"
echo "    Keycloak:      http://${LAN_IP}:8080"
echo "    Docker mirror: http://${LAN_IP}:5555"
echo "    GHCR mirror:   http://${LAN_IP}:5556"
