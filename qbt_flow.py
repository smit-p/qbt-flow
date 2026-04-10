#!/usr/bin/env python3
"""
qbt_flow.py
Dynamic bandwidth manager for qBittorrent instances.
Throttles download/upload limits based on active Plex streams and prioritises
a racing instance during a configurable time window.

Configuration is loaded from a config.env file in the same directory, or
from environment variables. Copy config.env.example to config.env to get started.
"""

import argparse
import os
import re
import threading
import logging
import logging.handlers
import sys
import signal
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError

# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

_SCRIPT_DIR = Path(__file__).parent
_ENV_FILE = _SCRIPT_DIR / "config.env"

def _load_env():
    if _ENV_FILE.exists():
        with open(_ENV_FILE) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                line = re.sub(r"^export\s+", "", line)
                if "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())

_load_env()

def _env(key, default=""):
    return os.environ.get(key, default)

def _env_int(key, default):
    try:
        return int(os.environ.get(key, default))
    except (ValueError, TypeError):
        return default

def _env_float(key, default):
    try:
        return float(os.environ.get(key, default))
    except (ValueError, TypeError):
        return default

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PLEX_URL   = _env("PLEX_URL", "http://localhost:32400")
PLEX_TOKEN = _env("PLEX_TOKEN")

# qBittorrent instances as "host:port:user:pass[:scheme]" comma-separated pairs
# e.g. QBT_INSTANCES=localhost:8080:admin:password,localhost:8443:admin:password:https
def _parse_qbt_instances():
    raw = _env("QBT_INSTANCES", "localhost:8080:admin:adminadmin")
    instances = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":", 4)
        if len(parts) >= 4:
            host, port, user, password = parts[0], parts[1], parts[2], parts[3]
            scheme = parts[4] if len(parts) == 5 else "http"
            if scheme not in ("http", "https"):
                print(f"WARNING: invalid scheme {scheme!r} in QBT_INSTANCES entry: {entry!r}", file=sys.stderr)
                scheme = "http"
            instances.append((host, int(port), user, password, scheme))
        else:
            print(f"WARNING: invalid QBT_INSTANCES entry: {entry!r} (expected host:port:user:pass[:scheme])", file=sys.stderr)
    return instances

QBT_INSTANCES = _parse_qbt_instances()

# Your line speed in bits per second (download)
TOTAL_BANDWIDTH_BPS   = _env_float("TOTAL_BANDWIDTH_BPS", 1_000_000_000)  # 1 Gbps
# Upload line speed — defaults to TOTAL_BANDWIDTH_BPS if not set
TOTAL_UPLOAD_BPS      = _env_float("TOTAL_UPLOAD_BPS", TOTAL_BANDWIDTH_BPS)

# Fraction of remaining bandwidth (after Plex) to give qBittorrent.
QBT_HEADROOM_FRACTION = _env_float("QBT_HEADROOM_FRACTION", 0.8)
QBT_UPLOAD_FRACTION   = _env_float("QBT_UPLOAD_FRACTION",   0.9)

# Whether to split bandwidth evenly across qBittorrent instances.
# If true, each instance gets (total / N). If false, each gets the full amount.
QBT_SPLIT_BETWEEN_INSTANCES = _env("QBT_SPLIT_BETWEEN_INSTANCES", "true").lower() in ("true", "1", "yes")

# Hard floor: never throttle qbt below this (bytes/sec)
MIN_QBT_DL_BYTES = _env_int("MIN_QBT_DL_BYTES", 10 * 1024 * 1024)   # 10 MB/s
MIN_QBT_UL_BYTES = _env_int("MIN_QBT_UL_BYTES",  5 * 1024 * 1024)   #  5 MB/s

# When no streams are active, remove all limits.
NORMAL_DL_BYTES = 0  # unlimited
NORMAL_UL_BYTES = 0  # unlimited

# Safety buffer added on top of reported Plex bitrate
PLEX_OVERHEAD_FACTOR = _env_float("PLEX_OVERHEAD_FACTOR", 1.25)

POLL_INTERVAL   = _env_int("POLL_INTERVAL",   15)   # seconds between Plex checks
REQUEST_TIMEOUT = _env_int("REQUEST_TIMEOUT", 10)

# Behavior when Plex is unreachable: "unlimited" (remove limits), "keep" (keep last limits)
PLEX_UNREACHABLE_ACTION = _env("PLEX_UNREACHABLE_ACTION", "keep")

# ---------------------------------------------------------------------------
# Racing window — during this time window the racing instance gets priority
# and the non-racing (media) instance is hard-capped.
# ---------------------------------------------------------------------------
RACING_WINDOW_ENABLED = _env("RACING_WINDOW_ENABLED", "false").lower() in ("true", "1", "yes")
RACING_WINDOW_START   = _env_int("RACING_WINDOW_START", 0)    # hour (0-23), e.g. 0 = midnight
RACING_WINDOW_END     = _env_int("RACING_WINDOW_END",   7)    # hour (0-23), e.g. 7 = 7 AM
RACING_INSTANCE_PORT  = _env_int("RACING_INSTANCE_PORT", 39001)

# Hard caps for the NON-racing instance during the racing window (bytes/sec).
# These override the normal calculated limits for that instance only.
RACING_NON_RACING_DL_LIMIT = _env_int("RACING_NON_RACING_DL_LIMIT", 5 * 1024 * 1024)   # 5 MB/s
RACING_NON_RACING_UL_LIMIT = _env_int("RACING_NON_RACING_UL_LIMIT", 5 * 1024 * 1024)   # 5 MB/s

LOG_FILE  = _env("LOG_FILE", str(_SCRIPT_DIR / "throttle.log"))
LOG_LEVEL = getattr(logging, _env("LOG_LEVEL", "INFO").upper(), logging.INFO)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.handlers.RotatingFileHandler(
            LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3
        ),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("qbt_flow")

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

last_dl_limit = None
last_ul_limit = None

stop_event = threading.Event()

def handle_signal(signum, frame):
    log.info("Received signal %d, shutting down", signum)
    stop_event.set()

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

# --dry-run support (set via CLI arg)
DRY_RUN = False

# ---------------------------------------------------------------------------
# Plex helpers
# ---------------------------------------------------------------------------

def get_plex_sessions():
    """
    Returns (session_count, total_bitrate_bps).
    Sums the 'bitrate' attribute from each active Session element.
    Returns (-1, 0) on error.
    """
    url = f"{PLEX_URL}/status/sessions"
    req = Request(url, headers={"X-Plex-Token": PLEX_TOKEN, "Accept": "application/xml"})
    try:
        with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            body = resp.read()
        root = ET.fromstring(body)
        sessions = root.findall(".//Video") + root.findall(".//Track")
        total_kbps = 0
        count = 0
        for session in sessions:
            player = session.find("Player")
            state = player.attrib.get("state", "playing") if player is not None else session.attrib.get("state", "playing")
            if state in ("paused", "stopped"):
                continue
            count += 1
            # Plex reports bitrate in kbps on the Session element.
            # Fall back to the Media element if not present at top level.
            bitrate = session.attrib.get("bitrate")
            if not bitrate:
                media = session.find("Media")
                if media is not None:
                    bitrate = media.attrib.get("bitrate")
            if bitrate:
                total_kbps += int(bitrate)
        total_bps = total_kbps * 1000
        return count, total_bps
    except (URLError, HTTPError, ET.ParseError, ValueError) as e:
        log.warning("Plex session check failed: %s", e)
        return -1, 0

# ---------------------------------------------------------------------------
# qBittorrent helpers
# ---------------------------------------------------------------------------

class QbtClient:
    def __init__(self, host, port, username, password, scheme="http"):
        self.base = f"{scheme}://{host}:{port}"
        self.username = username
        self.password = password
        self.cookie = None

    def _post(self, path, data):
        payload = urlencode(data).encode()
        req = Request(
            f"{self.base}{path}",
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        if self.cookie:
            req.add_header("Cookie", self.cookie)
        try:
            with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                return resp.status == 200
        except (URLError, HTTPError) as e:
            log.warning("qbt POST %s failed: %s", path, e)
            return False

    def login(self):
        payload = urlencode({"username": self.username, "password": self.password}).encode()
        req = Request(
            f"{self.base}/api/v2/auth/login",
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        try:
            with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                set_cookie = resp.headers.get("Set-Cookie", "")
                for part in set_cookie.split(";"):
                    part = part.strip()
                    if part.startswith("SID="):
                        self.cookie = part
                        return True
            log.warning("qbt login at %s: no SID in response", self.base)
            return False
        except (URLError, HTTPError) as e:
            log.warning("qbt login failed at %s: %s", self.base, e)
            return False

    def set_speed_limits(self, dl_bytes, ul_bytes):
        ok1 = self._post("/api/v2/transfer/setDownloadLimit", {"limit": dl_bytes})
        ok2 = self._post("/api/v2/transfer/setUploadLimit", {"limit": ul_bytes})
        if not (ok1 and ok2) and self.cookie:
            # Auth may have expired — re-login and retry once
            log.debug("Retrying after re-login on %s", self.base)
            self.cookie = None
            if self.login():
                ok1 = self._post("/api/v2/transfer/setDownloadLimit", {"limit": dl_bytes})
                ok2 = self._post("/api/v2/transfer/setUploadLimit", {"limit": ul_bytes})
        return ok1 and ok2

    def ensure_logged_in(self):
        if not self.cookie:
            return self.login()
        return True


clients = [QbtClient(h, p, u, pw, s) for h, p, u, pw, s in QBT_INSTANCES]


def _is_racing_window():
    """Return True if the current local time falls within the racing window."""
    if not RACING_WINDOW_ENABLED:
        return False
    hour = datetime.now().hour
    if RACING_WINDOW_START <= RACING_WINDOW_END:
        return RACING_WINDOW_START <= hour < RACING_WINDOW_END
    else:
        # Wraps midnight, e.g. start=22, end=7
        return hour >= RACING_WINDOW_START or hour < RACING_WINDOW_END


def apply_limits(dl_bytes, ul_bytes, label, detail="", force=False):
    global last_dl_limit, last_ul_limit

    racing_active = _is_racing_window()

    # Skip if limits haven't changed meaningfully (within 1% tolerance)
    # But always re-apply when racing state might have changed.
    if not force and last_dl_limit is not None and last_ul_limit is not None:
        dl_diff = abs(dl_bytes - last_dl_limit) / max(last_dl_limit, 1)
        ul_diff = abs(ul_bytes - last_ul_limit) / max(last_ul_limit, 1)
        if dl_diff < 0.01 and ul_diff < 0.01 and not racing_active:
            log.debug("Limits unchanged within tolerance, skipping API call")
            return

    # Compute per-instance limits
    num_instances = len(clients)

    for client in clients:
        client_port = int(client.base.rsplit(":", 1)[-1])

        if racing_active and num_instances > 1:
            # During racing window: cap the media instance, give the rest to racing
            if client_port == RACING_INSTANCE_PORT:
                # Racing instance gets total minus the non-racing cap
                if dl_bytes == 0:
                    c_dl, c_ul = 0, 0  # unlimited
                else:
                    c_dl = max(dl_bytes - RACING_NON_RACING_DL_LIMIT, MIN_QBT_DL_BYTES)
                    c_ul = max(ul_bytes - RACING_NON_RACING_UL_LIMIT, MIN_QBT_UL_BYTES)
            else:
                # Non-racing (media) instance gets hard cap
                c_dl = RACING_NON_RACING_DL_LIMIT
                c_ul = RACING_NON_RACING_UL_LIMIT
        elif QBT_SPLIT_BETWEEN_INSTANCES and num_instances > 1 and dl_bytes > 0:
            c_dl = max(dl_bytes // num_instances, MIN_QBT_DL_BYTES)
            c_ul = max(ul_bytes // num_instances, MIN_QBT_UL_BYTES)
        else:
            c_dl, c_ul = dl_bytes, ul_bytes

        extra = ""
        if racing_active and num_instances > 1:
            is_racer = client_port == RACING_INSTANCE_PORT
            extra = " [RACING]" if is_racer else " [CAPPED]"

        if DRY_RUN:
            dl_str = f"{c_dl / (1024 * 1024):.1f} MB/s" if c_dl else "unlimited"
            ul_str = f"{c_ul / (1024 * 1024):.1f} MB/s" if c_ul else "unlimited"
            log.info("[DRY-RUN] [%s] %s%s: dl=%s ul=%s%s", label, client.base, extra, dl_str, ul_str,
                     f" ({detail})" if detail else "")
            continue
        if not client.ensure_logged_in():
            log.error("Cannot login to qbt at %s — skipping", client.base)
            continue
        ok = client.set_speed_limits(c_dl, c_ul)
        if ok:
            dl_str = f"{c_dl / (1024 * 1024):.1f} MB/s" if c_dl else "unlimited"
            ul_str = f"{c_ul / (1024 * 1024):.1f} MB/s" if c_ul else "unlimited"
            log.info("[%s] %s%s: dl=%s ul=%s%s", label, client.base, extra, dl_str, ul_str,
                     f" ({detail})" if detail else "")
        else:
            client.cookie = None
            log.warning("Failed to set limits on %s, will retry next cycle", client.base)

    last_dl_limit = dl_bytes
    last_ul_limit = ul_bytes

# ---------------------------------------------------------------------------
# Limit calculation
# ---------------------------------------------------------------------------

def calculate_limits(session_count, plex_bps):
    """
    Given active Plex session count and their total reported bitrate,
    return (dl_bytes_per_sec, ul_bytes_per_sec) for qBittorrent.
    """
    # Apply overhead factor to account for burst buffering etc.
    plex_reserved_bps = plex_bps * PLEX_OVERHEAD_FACTOR

    remaining_dl_bps = max(0, TOTAL_BANDWIDTH_BPS - plex_reserved_bps)
    remaining_ul_bps = max(0, TOTAL_UPLOAD_BPS - plex_reserved_bps)

    dl_bps = remaining_dl_bps * QBT_HEADROOM_FRACTION
    ul_bps = remaining_ul_bps * QBT_UPLOAD_FRACTION

    # Convert to bytes/sec and enforce floor
    dl_bytes = max(int(dl_bps / 8), MIN_QBT_DL_BYTES)
    ul_bytes = max(int(ul_bps / 8), MIN_QBT_UL_BYTES)

    plex_mbps = plex_bps / 1_000_000
    detail = (f"{session_count} stream(s), "
              f"Plex using ~{plex_mbps:.0f} Mbps, "
              f"remaining DL {remaining_dl_bps / 1_000_000:.0f} Mbps / UL {remaining_ul_bps / 1_000_000:.0f} Mbps")

    return dl_bytes, ul_bytes, detail

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def _validate_config():
    errors = []
    if not PLEX_TOKEN:
        errors.append("PLEX_TOKEN is not set")
    if not QBT_INSTANCES:
        errors.append("No valid QBT_INSTANCES configured")
    if errors:
        for e in errors:
            log.error(e)
        sys.exit(1)


def main():
    global DRY_RUN

    parser = argparse.ArgumentParser(description="Plex-aware qBittorrent bandwidth throttle")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log calculated limits without applying them to qBittorrent")
    args = parser.parse_args()
    DRY_RUN = args.dry_run

    _validate_config()

    log.info("qbt_flow starting (poll=%ds, DL=%.0f Mbps, UL=%.0f Mbps%s)",
             POLL_INTERVAL, TOTAL_BANDWIDTH_BPS / 1_000_000,
             TOTAL_UPLOAD_BPS / 1_000_000,
             ", dry-run" if DRY_RUN else "")
    if RACING_WINDOW_ENABLED:
        log.info("Racing window enabled: %02d:00–%02d:00, racing port=%d, "
                 "media cap DL=%.1f MB/s UL=%.1f MB/s",
                 RACING_WINDOW_START, RACING_WINDOW_END, RACING_INSTANCE_PORT,
                 RACING_NON_RACING_DL_LIMIT / (1024 * 1024),
                 RACING_NON_RACING_UL_LIMIT / (1024 * 1024))

    try:
        while not stop_event.is_set():
            session_count, plex_bps = get_plex_sessions()

            if session_count < 0:
                if PLEX_UNREACHABLE_ACTION == "keep":
                    log.info("Plex unreachable — keeping last limits")
                else:
                    log.info("Plex unreachable — setting unlimited")
                    apply_limits(NORMAL_DL_BYTES, NORMAL_UL_BYTES, "NORMAL")
            elif session_count == 0:
                apply_limits(NORMAL_DL_BYTES, NORMAL_UL_BYTES, "NORMAL")
            else:
                dl_bytes, ul_bytes, detail = calculate_limits(session_count, plex_bps)
                log.debug("Plex poll: %s", detail)
                apply_limits(dl_bytes, ul_bytes, "THROTTLE", detail)

            stop_event.wait(POLL_INTERVAL)
    finally:
        # Remove throttles on exit so qBittorrent isn't left limited
        if not DRY_RUN:
            log.info("Cleaning up — removing qbt speed limits")
            apply_limits(0, 0, "SHUTDOWN", force=True)
        log.info("qbt_flow stopped")

if __name__ == "__main__":
    main()
