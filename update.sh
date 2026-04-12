#!/usr/bin/env bash
# Update qbt-flow to the latest version.
#
# One-liner (from anywhere):
#   curl -fsSL https://raw.githubusercontent.com/smit-p/qbt-flow/main/update.sh | bash
#
# Or from the install directory:
#   ./update.sh
set -euo pipefail

# ── Resolve install directory ────────────────────────────────────────────────
# If run from the repo, use that. Otherwise check common locations.
if [[ -f "${BASH_SOURCE[0]:-}" ]] && [[ -d "$(dirname "${BASH_SOURCE[0]}")/.git" ]]; then
    DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
elif [[ -d "${QBT_FLOW_DIR:-}" ]]; then
    DIR="$QBT_FLOW_DIR"
elif [[ -d "$HOME/qbt-flow/.git" ]]; then
    DIR="$HOME/qbt-flow"
else
    echo "ERROR: Could not find qbt-flow install directory."
    echo "Set QBT_FLOW_DIR or run from the install directory."
    exit 1
fi

cd "$DIR"

# ── Colors ───────────────────────────────────────────────────────────────────
BOLD='\033[1m'  GREEN='\033[32m'  YELLOW='\033[33m'  DIM='\033[2m'  RESET='\033[0m'
info() { echo -e "  ${GREEN}→${RESET} $*"; }
warn() { echo -e "  ${YELLOW}!${RESET} $*"; }

echo ""
echo -e "  ${BOLD}qbt-flow updater${RESET}"
echo ""

# ── Pull latest ──────────────────────────────────────────────────────────────
BEFORE=$(git rev-parse HEAD)
info "Pulling latest changes..."
git pull --ff-only -q 2>/dev/null || {
    warn "Fast-forward pull failed. You may have local changes."
    warn "Run: cd $DIR && git stash && git pull && git stash pop"
    exit 1
}
AFTER=$(git rev-parse HEAD)

if [[ "$BEFORE" == "$AFTER" ]]; then
    info "Already up to date."
else
    info "Updated: ${DIM}${BEFORE:0:7} → ${AFTER:0:7}${RESET}"
    echo ""
    echo -e "  ${DIM}$(git log --oneline "${BEFORE}..${AFTER}")${RESET}"
fi

# ── Restart service if running ───────────────────────────────────────────────
echo ""
if systemctl is-active --quiet qbt-flow 2>/dev/null; then
    info "Restarting qbt-flow service..."
    sudo systemctl restart qbt-flow
    info "Service restarted."
else
    warn "qbt-flow service is not running (skipping restart)."
fi

echo ""
echo -e "  ${BOLD}${GREEN}✓ Update complete!${RESET}"
echo ""
