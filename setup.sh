#!/usr/bin/env bash
# One-liner bootstrap for qbt-flow:
#   curl -fsSL https://raw.githubusercontent.com/smit-p/qbt-flow/main/setup.sh | bash
set -euo pipefail

REPO="https://github.com/smit-p/qbt-flow.git"
INSTALL_DIR="${QBT_FLOW_DIR:-$HOME/qbt-flow}"

echo ""
echo "  ╔═══════════════════════════════════════╗"
echo "  ║         qbt-flow installer            ║"
echo "  ╚═══════════════════════════════════════╝"
echo ""

# ---- Check Python ----
if ! command -v python3 &>/dev/null; then
    echo "ERROR: python3 not found. Install Python 3.8+ and try again."
    exit 1
fi

# ---- Clone or update ----
if [[ -d "$INSTALL_DIR/.git" ]]; then
    echo "→ Updating existing install at $INSTALL_DIR ..."
    git -C "$INSTALL_DIR" pull --ff-only
else
    echo "→ Cloning qbt-flow to $INSTALL_DIR ..."
    git clone "$REPO" "$INSTALL_DIR"
fi

cd "$INSTALL_DIR"

# ---- Config ----
if [[ ! -f config.env ]]; then
    cp config.env.example config.env
    chmod 600 config.env
    echo ""
    echo "  ┌──────────────────────────────────────────────────────────┐"
    echo "  │  config.env created — edit it with your settings:       │"
    echo "  │                                                          │"
    echo "  │    nano $INSTALL_DIR/config.env"
    echo "  │                                                          │"
    echo "  │  Then install the systemd service:                       │"
    echo "  │                                                          │"
    echo "  │    cd $INSTALL_DIR && ./install.sh"
    echo "  │                                                          │"
    echo "  └──────────────────────────────────────────────────────────┘"
    echo ""
else
    echo "→ config.env already exists, installing service..."
    echo ""
    exec bash "$INSTALL_DIR/install.sh"
fi
