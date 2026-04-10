#!/usr/bin/env bash
# install.sh — sets up qbt-flow and registers a systemd service
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CURRENT_USER="${SUDO_USER:-$USER}"
SERVICE_NAME="qbt-flow"
SYSTEMD_DIR="/etc/systemd/system"

echo "=== qbt-flow Installer ==="
echo "Script dir : $SCRIPT_DIR"
echo "Running as : $CURRENT_USER"
echo ""

# ---- 1. Check Python ----
if ! command -v python3 &>/dev/null; then
    echo "ERROR: python3 not found. Install Python 3.8+ and try again."
    exit 1
fi
PY_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo "[1/3] Python $PY_VERSION found."

# ---- 2. Set up config ----
if [[ ! -f "$SCRIPT_DIR/config.env" ]]; then
    echo ""
    echo "[2/3] config.env not found — copying example..."
    cp "$SCRIPT_DIR/config.env.example" "$SCRIPT_DIR/config.env"
    chmod 600 "$SCRIPT_DIR/config.env"
    echo ""
    echo "  *** ACTION REQUIRED ***"
    echo "  Edit $SCRIPT_DIR/config.env with your media server and qBittorrent settings."
    echo "  Then re-run this script."
    exit 0
else
    echo "[2/3] config.env already exists."
fi

# ---- 3. Install systemd service ----
if [[ $EUID -ne 0 ]]; then
    echo ""
    echo "[3/3] Installing systemd service requires root. Re-running with sudo..."
    exec sudo bash "$0" "$@"
fi

echo "[3/3] Installing systemd service..."

cat > "$SYSTEMD_DIR/${SERVICE_NAME}.service" <<EOF
[Unit]
Description=qbt-flow — dynamic qBittorrent bandwidth manager
After=network.target

[Service]
Type=simple
User=${CURRENT_USER}
WorkingDirectory=${SCRIPT_DIR}
ExecStart=/usr/bin/python3 ${SCRIPT_DIR}/qbt_flow.py
Restart=on-failure
RestartSec=30
StandardOutput=null
StandardError=null

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now "$SERVICE_NAME"

echo ""
echo "=== Setup complete ==="
echo "Service status: systemctl status $SERVICE_NAME"
echo "Follow logs:    tail -f $SCRIPT_DIR/throttle.log"
echo "Stop:           sudo systemctl stop $SERVICE_NAME"
echo "Uninstall:      sudo systemctl disable --now $SERVICE_NAME && sudo rm $SYSTEMD_DIR/${SERVICE_NAME}.service"
