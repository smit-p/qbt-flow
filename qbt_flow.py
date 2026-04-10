#!/usr/bin/env python3
"""
qbt_flow.py
Dynamic bandwidth manager for qBittorrent instances.
Throttles download/upload limits based on active media-server streams
(Plex, Jellyfin, or Emby) and prioritises a racing instance during a
configurable time window.

Features:
- Gradual ramp-up when streams stop (configurable steps)
- Exponential backoff on media-server failures
- Optional status/metrics HTTP endpoint
- Racing-window bandwidth priority

Configuration is loaded from a config.env file in the same directory, or
from environment variables. Copy config.env.example to config.env to get started.
"""

import argparse
import json
import os
import re
import signal
import sys
import threading
import time
import logging
import logging.handlers
import xml.etree.ElementTree as ET
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
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

# Per-server configuration — set URL + token for each server to poll.
# Streams from all configured servers are aggregated.
PLEX_URL       = _env("PLEX_URL", "")
PLEX_TOKEN     = _env("PLEX_TOKEN", "")
JELLYFIN_URL   = _env("JELLYFIN_URL", "")
JELLYFIN_TOKEN = _env("JELLYFIN_TOKEN", "")
EMBY_URL       = _env("EMBY_URL", "")
EMBY_TOKEN     = _env("EMBY_TOKEN", "")

# Legacy compat: MEDIA_SERVER_TYPE + MEDIA_SERVER_URL + MEDIA_SERVER_TOKEN
_ms_type  = _env("MEDIA_SERVER_TYPE", "").lower()
_ms_url   = _env("MEDIA_SERVER_URL", "")
_ms_token = _env("MEDIA_SERVER_TOKEN", "")
if _ms_url and _ms_token:
    _target = _ms_type or "plex"
    if _target == "plex" and not PLEX_TOKEN:
        PLEX_URL, PLEX_TOKEN = _ms_url, _ms_token
    elif _target == "jellyfin" and not JELLYFIN_TOKEN:
        JELLYFIN_URL, JELLYFIN_TOKEN = _ms_url, _ms_token
    elif _target == "emby" and not EMBY_TOKEN:
        EMBY_URL, EMBY_TOKEN = _ms_url, _ms_token

# Default Plex URL when nothing else is configured
if not PLEX_URL and not JELLYFIN_URL and not EMBY_URL:
    PLEX_URL = "http://localhost:32400"

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

POLL_INTERVAL   = _env_int("POLL_INTERVAL",   15)   # seconds between checks
REQUEST_TIMEOUT = _env_int("REQUEST_TIMEOUT", 10)

# Behavior when media server is unreachable: "unlimited" or "keep" (last limits)
PLEX_UNREACHABLE_ACTION = _env("PLEX_UNREACHABLE_ACTION", "keep")

# Gradual ramp-up when streams stop: number of cycles (including final
# unlimited) before removing all limits.  0 = instant.  Each step doubles.
RAMP_UP_STEPS = _env_int("RAMP_UP_STEPS", 3)

# Max exponential-backoff interval (seconds) when the media server is unreachable
BACKOFF_MAX_INTERVAL = _env_int("BACKOFF_MAX_INTERVAL", 300)

# Status / metrics HTTP endpoint (0 = disabled)
STATUS_PORT = _env_int("STATUS_PORT", 0)

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
RACING_NON_RACING_DL_LIMIT = _env_int("RACING_NON_RACING_DL_LIMIT", 1 * 1024 * 1024)   # 1 MB/s
RACING_NON_RACING_UL_LIMIT = _env_int("RACING_NON_RACING_UL_LIMIT", 1 * 1024 * 1024)   # 1 MB/s

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
last_racing_active = None

_start_time = 0.0
_status = {
    "streams": 0,
    "dl_limit": 0,
    "ul_limit": 0,
    "racing_active": False,
    "label": "STARTING",
    "media_servers": [],
}

stop_event = threading.Event()

def handle_signal(signum, frame):
    log.info("Received signal %d, shutting down", signum)
    stop_event.set()

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

# --dry-run support (set via CLI arg)
DRY_RUN = False


def _fmt_speed(bytes_per_sec):
    """Format a speed value for logging: 0 → 'unlimited', else '12.3 MB/s'."""
    return f"{bytes_per_sec / (1024 * 1024):.1f} MB/s" if bytes_per_sec else "unlimited"


# ---------------------------------------------------------------------------
# Exponential backoff
# ---------------------------------------------------------------------------

class BackoffTracker:
    """Exponential backoff with configurable ceiling."""

    def __init__(self, max_interval=300):
        self.max_interval = max_interval
        self.failures = 0
        self._next_retry = 0.0

    def should_skip(self):
        return self.failures > 0 and time.monotonic() < self._next_retry

    def record_failure(self):
        self.failures += 1
        delay = min(2 ** self.failures, self.max_interval)
        self._next_retry = time.monotonic() + delay
        return delay

    def record_success(self):
        self.failures = 0
        self._next_retry = 0.0

    def current_delay(self):
        if self.failures == 0:
            return 0
        return min(2 ** self.failures, self.max_interval)

# ---------------------------------------------------------------------------
# Plex helpers
# ---------------------------------------------------------------------------

def get_plex_sessions(url=None, token=None):
    """
    Returns (session_count, total_bitrate_bps).
    Sums the 'bitrate' attribute from each active Session element.
    Returns (-1, 0) on error.
    """
    url = url or PLEX_URL
    token = token or PLEX_TOKEN
    req_url = f"{url}/status/sessions"
    req = Request(req_url, headers={"X-Plex-Token": token, "Accept": "application/xml"})
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
# Jellyfin / Emby helpers
# ---------------------------------------------------------------------------

def _get_jellyfin_emby_sessions(url=None, token=None, path_prefix=""):
    """
    Shared Jellyfin / Emby session fetcher.
    Returns (session_count, total_bitrate_bps).  (-1, 0) on error.
    """
    url = url or JELLYFIN_URL or EMBY_URL
    token = token or JELLYFIN_TOKEN or EMBY_TOKEN
    req_url = f"{url}{path_prefix}/Sessions"
    req = Request(req_url, headers={
        "X-Emby-Token": token,
        "Accept": "application/json",
    })
    try:
        with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            data = json.loads(resp.read())
        count = 0
        total_bps = 0
        for session in data:
            now_playing = session.get("NowPlayingItem")
            if not now_playing:
                continue
            play_state = session.get("PlayState", {})
            if play_state.get("IsPaused", False):
                continue
            count += 1
            total_bps += now_playing.get("Bitrate", 0)
        return count, total_bps
    except (URLError, HTTPError, json.JSONDecodeError, ValueError, KeyError) as e:
        log.warning("Media server session check failed: %s", e)
        return -1, 0


def get_jellyfin_sessions(url=None, token=None):
    """Jellyfin session fetcher."""
    return _get_jellyfin_emby_sessions(url, token)


def get_emby_sessions(url=None, token=None):
    """Emby session fetcher."""
    return _get_jellyfin_emby_sessions(url, token, "/emby")


# ---------------------------------------------------------------------------
# Multi-server aggregation
# ---------------------------------------------------------------------------

_configured_servers = []
if PLEX_URL and PLEX_TOKEN:
    _configured_servers.append(("plex", PLEX_URL, PLEX_TOKEN, get_plex_sessions))
if JELLYFIN_URL and JELLYFIN_TOKEN:
    _configured_servers.append(("jellyfin", JELLYFIN_URL, JELLYFIN_TOKEN, get_jellyfin_sessions))
if EMBY_URL and EMBY_TOKEN:
    _configured_servers.append(("emby", EMBY_URL, EMBY_TOKEN, get_emby_sessions))

_status["media_servers"] = [name for name, _, _, _ in _configured_servers]

_server_backoffs = {}


def get_sessions():
    """Poll all configured media servers and aggregate active streams."""
    total_count = 0
    total_bps = 0
    any_ok = False

    for name, url, token, fetch_fn in _configured_servers:
        bt = _server_backoffs.setdefault(name, BackoffTracker(BACKOFF_MAX_INTERVAL))
        if bt.should_skip():
            log.debug("%s backoff active, skipping", name)
            continue

        count, bps = fetch_fn(url, token)
        if count < 0:
            delay = bt.record_failure()
            log.info("%s unreachable (backoff %ds)", name, delay)
        else:
            bt.record_success()
            total_count += count
            total_bps += bps
            any_ok = True

    if not any_ok:
        return -1, 0
    return total_count, total_bps


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
    global last_dl_limit, last_ul_limit, last_racing_active

    racing_active = _is_racing_window()

    # Skip if limits haven't changed meaningfully (within 1% tolerance)
    # But always re-apply when racing state has changed.
    if not force and last_dl_limit is not None and last_ul_limit is not None:
        dl_diff = abs(dl_bytes - last_dl_limit) / max(last_dl_limit, 1)
        ul_diff = abs(ul_bytes - last_ul_limit) / max(last_ul_limit, 1)
        if dl_diff < 0.01 and ul_diff < 0.01 and racing_active == last_racing_active:
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
            log.info("[DRY-RUN] [%s] %s%s: dl=%s ul=%s%s", label, client.base, extra,
                     _fmt_speed(c_dl), _fmt_speed(c_ul),
                     f" ({detail})" if detail else "")
            continue
        if not client.ensure_logged_in():
            log.error("Cannot login to qbt at %s — skipping", client.base)
            continue
        ok = client.set_speed_limits(c_dl, c_ul)
        if ok:
            log.info("[%s] %s%s: dl=%s ul=%s%s", label, client.base, extra,
                     _fmt_speed(c_dl), _fmt_speed(c_ul),
                     f" ({detail})" if detail else "")
        else:
            client.cookie = None
            log.warning("Failed to set limits on %s, will retry next cycle", client.base)

    last_dl_limit = dl_bytes
    last_ul_limit = ul_bytes
    last_racing_active = racing_active

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
              f"using ~{plex_mbps:.0f} Mbps, "
              f"remaining DL {remaining_dl_bps / 1_000_000:.0f} Mbps / UL {remaining_ul_bps / 1_000_000:.0f} Mbps")

    return dl_bytes, ul_bytes, detail

# ---------------------------------------------------------------------------
# Status / metrics endpoint
# ---------------------------------------------------------------------------

class _StatusHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/status", "/"):
            _status["uptime_seconds"] = int(time.monotonic() - _start_time)
            body = json.dumps(_status, indent=2).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/metrics":
            _status["uptime_seconds"] = int(time.monotonic() - _start_time)
            lines = [
                f'qbt_flow_streams {_status["streams"]}',
                f'qbt_flow_dl_limit_bytes {_status["dl_limit"]}',
                f'qbt_flow_ul_limit_bytes {_status["ul_limit"]}',
                f'qbt_flow_racing_active {1 if _status["racing_active"] else 0}',
                f'qbt_flow_uptime_seconds {_status["uptime_seconds"]}',
            ]
            body = "\n".join(lines).encode() + b"\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # suppress default access log


def _start_status_server():
    if not STATUS_PORT:
        return None
    try:
        server = HTTPServer(("", STATUS_PORT), _StatusHandler)
    except OSError as e:
        log.warning("Cannot start status server on port %d: %s", STATUS_PORT, e)
        return None
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    log.info("Status endpoint listening on port %d", STATUS_PORT)
    return server

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def _validate_config():
    errors = []
    if not _configured_servers:
        errors.append("No media server configured \u2014 set PLEX_URL+PLEX_TOKEN, "
                      "JELLYFIN_URL+JELLYFIN_TOKEN, or EMBY_URL+EMBY_TOKEN")
    if not QBT_INSTANCES:
        errors.append("No valid QBT_INSTANCES configured")
    if errors:
        for e in errors:
            log.error(e)
        sys.exit(1)


def main():
    global DRY_RUN, _start_time

    parser = argparse.ArgumentParser(description="Dynamic qBittorrent bandwidth manager")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log calculated limits without applying them to qBittorrent")
    args = parser.parse_args()
    DRY_RUN = args.dry_run

    _validate_config()

    _start_time = time.monotonic()

    server_names = "+".join(name for name, _, _, _ in _configured_servers)
    log.info("qbt_flow starting (poll=%ds, DL=%.0f Mbps, UL=%.0f Mbps, servers=%s%s)",
             POLL_INTERVAL, TOTAL_BANDWIDTH_BPS / 1_000_000,
             TOTAL_UPLOAD_BPS / 1_000_000, server_names,
             ", dry-run" if DRY_RUN else "")
    if RACING_WINDOW_ENABLED:
        log.info("Racing window enabled: %02d:00–%02d:00, racing port=%d, "
                 "media cap DL=%.1f MB/s UL=%.1f MB/s",
                 RACING_WINDOW_START, RACING_WINDOW_END, RACING_INSTANCE_PORT,
                 RACING_NON_RACING_DL_LIMIT / (1024 * 1024),
                 RACING_NON_RACING_UL_LIMIT / (1024 * 1024))

    _start_status_server()

    prev_session_count = 0
    ramp_remaining = 0
    ramp_dl = 0
    ramp_ul = 0

    try:
        while not stop_event.is_set():
            session_count, server_bps = get_sessions()

            if session_count < 0:
                _status["label"] = "UNREACHABLE"
                if PLEX_UNREACHABLE_ACTION != "keep":
                    apply_limits(NORMAL_DL_BYTES, NORMAL_UL_BYTES, "NORMAL")
            else:
                _status["streams"] = session_count

                if session_count == 0:
                    # Start ramp-up if transitioning from active streams
                    if prev_session_count > 0 and RAMP_UP_STEPS > 0 and last_dl_limit:
                        ramp_remaining = RAMP_UP_STEPS
                        ramp_dl = last_dl_limit
                        ramp_ul = last_ul_limit or 0

                    if ramp_remaining > 0:
                        ramp_remaining -= 1
                        if ramp_remaining == 0:
                            apply_limits(NORMAL_DL_BYTES, NORMAL_UL_BYTES, "NORMAL")
                        else:
                            ramp_dl *= 2
                            ramp_ul *= 2
                            max_bw = int(TOTAL_BANDWIDTH_BPS / 8)
                            if ramp_dl >= max_bw:
                                apply_limits(NORMAL_DL_BYTES, NORMAL_UL_BYTES, "NORMAL")
                                ramp_remaining = 0
                            else:
                                step = RAMP_UP_STEPS - ramp_remaining
                                apply_limits(ramp_dl, ramp_ul, "RAMP-UP",
                                             f"step {step}/{RAMP_UP_STEPS}")
                    else:
                        apply_limits(NORMAL_DL_BYTES, NORMAL_UL_BYTES, "NORMAL")
                else:
                    ramp_remaining = 0
                    dl_bytes, ul_bytes, detail = calculate_limits(session_count, server_bps)
                    log.debug("Media poll: %s", detail)
                    apply_limits(dl_bytes, ul_bytes, "THROTTLE", detail)

                prev_session_count = session_count

            # Update status
            _status["dl_limit"] = last_dl_limit or 0
            _status["ul_limit"] = last_ul_limit or 0
            _status["racing_active"] = _is_racing_window()
            if ramp_remaining > 0:
                _status["label"] = "RAMP-UP"
            elif session_count > 0:
                _status["label"] = "THROTTLE"
            elif session_count == 0:
                _status["label"] = "NORMAL"

            stop_event.wait(POLL_INTERVAL)
    finally:
        # Remove throttles on exit so qBittorrent isn't left limited
        if not DRY_RUN:
            log.info("Cleaning up — removing qbt speed limits")
            apply_limits(0, 0, "SHUTDOWN", force=True)
        log.info("qbt_flow stopped")

if __name__ == "__main__":
    main()
