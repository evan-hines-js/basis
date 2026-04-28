#!/usr/bin/env bash
# Setup script for running Lattice E2E tests on a fresh Ubuntu server.
# Installs: Docker, Rust, Go, protobuf, kind, kubectl, helm, clang, cmake,
#           and all other build dependencies needed by test-docker.sh.
#
# Usage: sudo ./setup-ubuntu.sh
#   Then log out and back in (for docker group), or run: newgrp docker

set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
    echo "Run as root: sudo $0"
    exit 1
fi

REAL_USER="${SUDO_USER:-$USER}"
REAL_HOME=$(eval echo "~$REAL_USER")

echo "=== Installing system packages ==="
apt-get update
apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    clang \
    libclang-dev \
    cmake \
    protobuf-compiler \
    libprotobuf-dev \
    perl \
    git \
    curl \
    wget \
    jq \
    unzip \
    ca-certificates \
    gnupg \
    lsb-release

# ---- Docker ----
echo "=== Installing Docker ==="
if ! command -v docker &>/dev/null; then
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
        https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
        > /etc/apt/sources.list.d/docker.list
    apt-get update
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin
    systemctl enable --now docker
else
    echo "Docker already installed: $(docker --version)"
fi
usermod -aG docker "$REAL_USER"

# Configure Docker DNS and insecure registries for local testing
mkdir -p /etc/docker
cat > /etc/docker/daemon.json <<'EOF'
{
  "dns": ["8.8.8.8", "1.1.1.1"],
  "insecure-registries": ["10.0.0.131:5555", "10.0.0.131:5556", "10.0.0.131:5557"]
}
EOF
systemctl restart docker

# ---- Rust ----
echo "=== Installing Rust ==="
if [ ! -d "$REAL_HOME/.cargo" ]; then
    sudo -u "$REAL_USER" bash -c 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable'
else
    echo "Rust already installed"
    sudo -u "$REAL_USER" bash -c 'source "$HOME/.cargo/env" && rustup update stable'
fi

# ---- Go ----
echo "=== Installing Go ==="
GO_VERSION="1.25.3"
if ! command -v go &>/dev/null || ! go version | grep -q "$GO_VERSION"; then
    ARCH=$(dpkg --print-architecture)
    wget -q "https://go.dev/dl/go${GO_VERSION}.linux-${ARCH}.tar.gz" -O /tmp/go.tar.gz
    rm -rf /usr/local/go
    tar -C /usr/local -xzf /tmp/go.tar.gz
    rm /tmp/go.tar.gz
    # Add to path for all users
    cat > /etc/profile.d/go.sh <<'EOF'
export PATH="/usr/local/go/bin:$PATH"
EOF
else
    echo "Go already installed: $(go version)"
fi
export PATH="/usr/local/go/bin:$PATH"

# ---- kubectl ----
echo "=== Installing kubectl ==="
if ! command -v kubectl &>/dev/null; then
    ARCH=$(dpkg --print-architecture)
    curl -fsSL "https://dl.k8s.io/release/$(curl -fsSL https://dl.k8s.io/release/stable.txt)/bin/linux/${ARCH}/kubectl" \
        -o /usr/local/bin/kubectl
    chmod +x /usr/local/bin/kubectl
else
    echo "kubectl already installed: $(kubectl version --client --short 2>/dev/null || kubectl version --client)"
fi

# ---- Helm ----
echo "=== Installing Helm ==="
if ! command -v helm &>/dev/null; then
    curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
else
    echo "Helm already installed: $(helm version --short)"
fi

# ---- kind ----
echo "=== Installing kind ==="
if ! command -v kind &>/dev/null; then
    ARCH=$(dpkg --print-architecture)
    KIND_VERSION=$(curl -fsSL https://api.github.com/repos/kubernetes-sigs/kind/releases/latest | jq -r .tag_name)
    curl -fsSL "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-${ARCH}" -o /usr/local/bin/kind
    chmod +x /usr/local/bin/kind
else
    echo "kind already installed: $(kind version)"
fi

# ---- clusterctl ----
echo "=== Installing clusterctl ==="
if ! command -v clusterctl &>/dev/null; then
    ARCH=$(dpkg --print-architecture)
    CLUSTERCTL_VERSION=$(curl -fsSL https://api.github.com/repos/kubernetes-sigs/cluster-api/releases/latest | jq -r .tag_name)
    curl -fsSL "https://github.com/kubernetes-sigs/cluster-api/releases/download/${CLUSTERCTL_VERSION}/clusterctl-linux-${ARCH}" \
        -o /usr/local/bin/clusterctl
    chmod +x /usr/local/bin/clusterctl
else
    echo "clusterctl already installed: $(clusterctl version -o short 2>/dev/null || echo 'installed')"
fi

curl -fsSL https://claude.ai/install.sh | bash
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc && source ~/.bashrc

# ---- DNS resolver for e2e.internal zone ----
echo "=== Configuring DNS resolver for e2e.internal ==="
PIHOLE_IP="${LATTICE_PIHOLE_RESOLVER:-10.0.0.131:5353}"
mkdir -p /etc/systemd/resolved.conf.d
cat > /etc/systemd/resolved.conf.d/pihole.conf <<EOF
[Resolve]
DNS=${PIHOLE_IP}
DNSOverTLS=no
Domains=~e2e.internal
EOF
systemctl restart systemd-resolved
echo "DNS: e2e.internal queries routed to ${PIHOLE_IP}"

# ---- Summary ----
echo
echo "=== Setup complete ==="
echo
echo "Installed:"
docker --version
echo "Rust: $(sudo -u "$REAL_USER" bash -c 'source "$HOME/.cargo/env" && rustc --version')"
echo "Go: $(/usr/local/go/bin/go version)"
kubectl version --client 2>/dev/null || true
helm version --short 2>/dev/null || true
kind version 2>/dev/null || true
clusterctl version 2>/dev/null || true
claude --version || true
echo
echo ">>> Log out and back in (or run 'newgrp docker') for docker group to take effect."
echo ">>> Then run: ./scripts/dev/test-docker.sh"
