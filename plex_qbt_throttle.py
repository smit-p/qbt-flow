#!/usr/bin/env python3
"""
plex_qbt_throttle.py
Dynamically throttles qBittorrent speed limits based on active Plex streams.
Sums actual stream bitrates from Plex session data, then allocates a
configurable fraction of remaining bandwidth to qBittorrent.

Configuration is loaded from a config.env file in the same directory, or
from environment variables. Copy config.env.example to config.env to get started.
"""

import os
import re
import time
import logging
import sys
import signal
import xml.etree.ElementTree as ET
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

# qBittorrent instances as "host:port:user:pass" comma-separated pairs
# e.g. QBT_INSTANCES=localhost:8080:admin:password,localhost:8081:admin:password
def _parse_qbt_instances():
    raw = _env("QBT_INSTANCES", "localhost:8080:admin:adminadmin")
    instances = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":", 3)
        if len(parts) == 4:
            host, port, user, password = parts
            instances.append((host, int(port), user, password))
        else:
            print(f"WARNING: invalid QBT_INSTANCES entry: {entry!r} (expected host:port:user:pass)", file=sys.stderr)
    return instances

QBT_INSTANCES = _parse_qbt_instances()

# Your line speed in bits per second
TOTAL_BANDWIDTH_BPS   = _env_float("TOTAL_BANDWIDTH_BPS", 1_000_000_000)  # 1 Gbps

# Fraction of remaining bandwidth (after Plex) to give qBittorrent.
QBT_HEADROOM_FRACTION = _env_float("QBT_HEADROOM_FRACTION", 0.8)
QBT_UPLOAD_FRACTION   = _env_float("QBT_UPLOAD_FRACTION",   0.9)

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

LOG_FILE  = _env("LOG_FILE", str(_SCRIPT_DIR / "throttle.log"))
LOG_LEVEL = logging.INFO

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("plex_qbt_throttle")

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

last_dl_limit = None
last_ul_limit = None

running = True

def handle_signal(signum, frame):
    global running
    log.info("Received signal %d, shutting down", signum)
    running = False

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

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
    def __init__(self, host, port, username, password):
        self.base = f"http://{host}:{port}"
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
        return ok1 and ok2

    def ensure_logged_in(self):
        if not self.cookie:
            return self.login()
        return True


clients = [QbtClient(h, p, u, pw) for h, p, u, pw in QBT_INSTANCES]


def apply_limits(dl_bytes, ul_bytes, label, detail=""):
    global last_dl_limit, last_ul_limit

    # Skip if limits haven't changed meaningfully (within 5% tolerance)
    if last_dl_limit is not None and last_ul_limit is not None:
        dl_diff = abs(dl_bytes - last_dl_limit) / max(last_dl_limit, 1)
        ul_diff = abs(ul_bytes - last_ul_limit) / max(last_ul_limit, 1)
        if dl_diff < 0.01 and ul_diff < 0.01:
            log.debug("Limits unchanged within tolerance, skipping API call")
            return

    for client in clients:
        if not client.ensure_logged_in():
            log.error("Cannot login to qbt at %s — skipping", client.base)
            continue
        ok = client.set_speed_limits(dl_bytes, ul_bytes)
        if ok:
            dl_str = f"{dl_bytes // (1024 * 1024)} MB/s" if dl_bytes else "unlimited"
            ul_str = f"{ul_bytes // (1024 * 1024)} MB/s" if ul_bytes else "unlimited"
            log.info("[%s] %s: dl=%s ul=%s%s", label, client.base, dl_str, ul_str,
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

    remaining_bps = max(0, TOTAL_BANDWIDTH_BPS - plex_reserved_bps)

    dl_bps = remaining_bps * QBT_HEADROOM_FRACTION
    ul_bps = remaining_bps * QBT_UPLOAD_FRACTION

    # Convert to bytes/sec and enforce floor
    dl_bytes = max(int(dl_bps / 8), MIN_QBT_DL_BYTES)
    ul_bytes = max(int(ul_bps / 8), MIN_QBT_UL_BYTES)

    plex_mbps = plex_bps / 1_000_000
    detail = (f"{session_count} stream(s), "
              f"Plex using ~{plex_mbps:.0f} Mbps, "
              f"remaining {remaining_bps / 1_000_000:.0f} Mbps")

    return dl_bytes, ul_bytes, detail

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    log.info("plex_qbt_throttle starting (poll=%ds, total_bw=%.0f Mbps)",
             POLL_INTERVAL, TOTAL_BANDWIDTH_BPS / 1_000_000)

    while running:
        session_count, plex_bps = get_plex_sessions()

        if session_count < 0:
            log.info("Plex unreachable, setting unlimited")
            apply_limits(NORMAL_DL_BYTES, NORMAL_UL_BYTES, "NORMAL")
        elif session_count == 0:
            apply_limits(NORMAL_DL_BYTES, NORMAL_UL_BYTES, "NORMAL")
        else:
            dl_bytes, ul_bytes, detail = calculate_limits(session_count, plex_bps)
            log.debug("Plex poll: %s", detail)
            apply_limits(dl_bytes, ul_bytes, "THROTTLE", detail)

        time.sleep(POLL_INTERVAL)

    log.info("plex_qbt_throttle stopped")

if __name__ == "__main__":
    main()
