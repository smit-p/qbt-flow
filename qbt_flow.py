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

# Speed-suffix multipliers (case-insensitive lookup)
_SPEED_MULTIPLIERS = {
    # bits per second (SI / base-1000 — what ISPs advertise)
    "bps": 1, "kbps": 1_000, "mbps": 1_000_000, "gbps": 1_000_000_000,
    # bytes per second (binary / base-1024 — what apps display)
    "b/s": 1, "kb/s": 1024, "mb/s": 1024**2, "gb/s": 1024**3,
}
_SPEED_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([a-zA-Z/]+)\s*$")

def _parse_speed(raw):
    """Parse a human-readable speed value into a number.

    Accepts plain numbers (returned as-is) or numbers with a suffix:
      Bits/sec:  Kbps, Mbps, Gbps   (e.g. "1Gbps" → 1 000 000 000)
      Bytes/sec: KB/s, MB/s, GB/s   (e.g. "10MB/s" → 10 485 760)
    """
    if isinstance(raw, (int, float)):
        return raw
    raw = str(raw).strip()
    if not raw:
        return 0
    m = _SPEED_RE.match(raw)
    if m:
        number = float(m.group(1))
        suffix = m.group(2).lower()
        mult = _SPEED_MULTIPLIERS.get(suffix)
        if mult is not None:
            return number * mult
    # Plain number or unrecognised suffix — fall through to float()
    try:
        return float(raw)
    except ValueError:
        return 0

def _env_speed(key, default):
    """Read a speed env-var, returning *default* if unset."""
    val = os.environ.get(key)
    if val is not None:
        return _parse_speed(val)
    return default

# Size-suffix multipliers (for file sizes — base-1024)
_SIZE_MULTIPLIERS = {
    "b": 1, "kb": 1024, "mb": 1024**2, "gb": 1024**3,
}
_SIZE_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([a-zA-Z]*)\s*$")

def _parse_size(raw):
    """Parse a human-readable file size: 5MB, 500KB, 1GB, or plain bytes."""
    if isinstance(raw, (int, float)):
        return int(raw)
    raw = str(raw).strip()
    if not raw:
        return 0
    m = _SIZE_RE.match(raw)
    if m:
        number = float(m.group(1))
        suffix = m.group(2).lower()
        if suffix:
            mult = _SIZE_MULTIPLIERS.get(suffix)
            if mult is not None:
                return int(number * mult)
    try:
        return int(float(raw))
    except ValueError:
        return 0

def _env_size(key, default):
    """Read a file-size env-var, returning *default* if unset."""
    val = os.environ.get(key)
    if val is not None:
        return _parse_size(val)
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

# Default Plex URL when nothing else is configured
if not PLEX_URL and not JELLYFIN_URL and not EMBY_URL:
    PLEX_URL = "http://localhost:32400"

# qBittorrent instances as "host:port:user:pass[:scheme]" comma-separated pairs
# e.g. QBT_INSTANCES=localhost:8080:admin:password,localhost:8443:admin:password:https
def _parse_qbt_instances():
    raw = _env("QBT_INSTANCES", "")
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

# Your line speed (download).  Accepts plain numbers (bits/sec) or suffixes:
#   1Gbps, 500Mbps, 100Mbps, etc.
TOTAL_BANDWIDTH_BPS   = _env_speed("TOTAL_BANDWIDTH", 1_000_000_000)  # 1 Gbps
# Upload line speed — defaults to TOTAL_BANDWIDTH if not set
TOTAL_UPLOAD_BPS      = _env_speed("TOTAL_UPLOAD", TOTAL_BANDWIDTH_BPS)

# Fraction of remaining bandwidth (after media streams) to give qBittorrent.
# Download defaults to 1.0 (no throttling) because media streams use upload,
# not download.  Lower this only if you have a specific reason (e.g. bufferbloat
# without router-level QoS).
QBT_HEADROOM_FRACTION = _env_float("QBT_HEADROOM_FRACTION", 1.0)
QBT_UPLOAD_FRACTION   = _env_float("QBT_UPLOAD_FRACTION",   0.9)

# Whether to split bandwidth evenly across qBittorrent instances.
# If true, each instance gets (total / N). If false, each gets the full amount.
QBT_SPLIT_BETWEEN_INSTANCES = _env("QBT_SPLIT_BETWEEN_INSTANCES", "true").lower() in ("true", "1", "yes")

# Dynamic split: query each qBt instance for active torrent counts and give the
# full budget only to instances that are actively downloading/seeding.  Idle
# instances receive the hard floor (MIN_QBT_DL / MIN_QBT_UL) as a holding cap.
# On the next poll cycle the split re-evaluates automatically.
# Requires QBT_SPLIT_BETWEEN_INSTANCES=true and more than one instance.
QBT_DYNAMIC_SPLIT = _env("QBT_DYNAMIC_SPLIT", "false").lower() in ("true", "1", "yes")

# Minimum speed a torrent must exceed to be counted as "active" during the
# dynamic split.  Torrents that are stalled (stalledDL / stalledUP state) are
# always excluded regardless of this threshold.  Set to 0 to only exclude
# stalled-state torrents.
QBT_ACTIVE_DL_THRESHOLD = int(_env_speed("QBT_ACTIVE_DL_THRESHOLD", 0))  # bytes/sec
QBT_ACTIVE_UL_THRESHOLD = int(_env_speed("QBT_ACTIVE_UL_THRESHOLD", 0))  # bytes/sec

# Hard floor: never throttle qbt below this.  Accepts plain numbers (bytes/sec)
# or suffixes: 10MB/s, 5MB/s, etc.
MIN_QBT_DL_BYTES = int(_env_speed("MIN_QBT_DL", 10 * 1024 * 1024))   # 10 MB/s
MIN_QBT_UL_BYTES = int(_env_speed("MIN_QBT_UL",  5 * 1024 * 1024))   #  5 MB/s

# When no streams are active, remove all limits.
NORMAL_DL_BYTES = 0  # unlimited
NORMAL_UL_BYTES = 0  # unlimited

# Safety buffer added on top of reported stream bitrates
STREAM_OVERHEAD_FACTOR = _env_float("STREAM_OVERHEAD_FACTOR", 1.25)

POLL_INTERVAL   = _env_int("POLL_INTERVAL",   15)   # seconds between checks
REQUEST_TIMEOUT = _env_int("REQUEST_TIMEOUT", 10)

# Behavior when media server is unreachable: "unlimited" or "keep" (last limits)
UNREACHABLE_ACTION = _env("UNREACHABLE_ACTION", "keep")

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

# Hard caps for the NON-racing instance during the racing window.
# Accepts plain numbers (bytes/sec) or suffixes: 1MB/s, 512KB/s, etc.
RACING_NON_RACING_DL_LIMIT = int(_env_speed("RACING_NON_RACING_DL_LIMIT", 1 * 1024 * 1024))   # 1 MB/s
RACING_NON_RACING_UL_LIMIT = int(_env_speed("RACING_NON_RACING_UL_LIMIT", 1 * 1024 * 1024))   # 1 MB/s

LOG_FILE       = _env("LOG_FILE", str(_SCRIPT_DIR / "throttle.log"))
LOG_LEVEL      = getattr(logging, _env("LOG_LEVEL", "INFO").upper(), logging.INFO)
LOG_MAX_SIZE   = _env_size("LOG_MAX_SIZE", 5 * 1024 * 1024)        # 5 MB default
LOG_BACKUP_COUNT = _env_int("LOG_BACKUP_COUNT", 3)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.handlers.RotatingFileHandler(
            LOG_FILE, maxBytes=LOG_MAX_SIZE, backupCount=LOG_BACKUP_COUNT
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
last_detail = None
last_activity: dict = {}   # id(client) -> (dl_count: int, ul_count: int)

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
                title = session.attrib.get("title", "unknown")
                log.debug("Plex: skipping %s session '%s'", state, title)
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
            title = session.attrib.get("title", "unknown")
            log.debug("Plex: active stream '%s' @ %s kbps", title, bitrate or "unknown")
        total_bps = total_kbps * 1000
        log.debug("Plex: %d active stream(s), total %d kbps", count, total_kbps)
        return count, total_bps
    except (URLError, HTTPError, ET.ParseError, ValueError) as e:
        log.warning("Plex session check failed (%s): %s", url, e)
        return -1, 0

# ---------------------------------------------------------------------------
# Jellyfin / Emby helpers
# ---------------------------------------------------------------------------

def _get_jellyfin_emby_sessions(url=None, token=None, path_prefix="", server_type="media"):
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
                item_name = now_playing.get("Name", "unknown")
                log.debug("%s: skipping paused session '%s'", server_type, item_name)
                continue
            count += 1
            # Bitrate may be on NowPlayingItem, TranscodingInfo, or MediaSources
            bitrate = now_playing.get("Bitrate", 0)
            if not bitrate:
                bitrate = session.get("TranscodingInfo", {}).get("Bitrate", 0)
            if not bitrate:
                for ms in now_playing.get("MediaSources", []):
                    bitrate = ms.get("Bitrate", 0)
                    if bitrate:
                        break
            total_bps += bitrate
            item_name = now_playing.get("Name", "unknown")
            log.debug("%s: active stream '%s' @ %.1f Mbps", server_type, item_name, bitrate / 1_000_000)
        log.debug("%s: %d active stream(s), total %.1f Mbps", server_type, count, total_bps / 1_000_000)
        return count, total_bps
    except (URLError, HTTPError, json.JSONDecodeError, ValueError, KeyError) as e:
        log.warning("%s session check failed (%s): %s", server_type, url, e)
        return -1, 0


def get_jellyfin_sessions(url=None, token=None):
    """Jellyfin session fetcher."""
    return _get_jellyfin_emby_sessions(url, token, server_type="Jellyfin")


def get_emby_sessions(url=None, token=None):
    """Emby session fetcher."""
    return _get_jellyfin_emby_sessions(url, token, "/emby", server_type="Emby")


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
            was_ok = bt.failures == 0
            delay = bt.record_failure()
            if was_ok:
                log.warning("%s became unreachable, backing off %ds", name, delay)
            else:
                log.info("%s still unreachable (attempt #%d, backoff %ds)", name, bt.failures, delay)
        else:
            if bt.failures > 0:
                log.info("%s recovered after %d failed attempt(s)", name, bt.failures)
            bt.record_success()
            total_count += count
            total_bps += bps
            any_ok = True
            log.debug("%s: %d stream(s), %.1f Mbps", name, count, bps / 1_000_000)

    if not any_ok:
        log.warning("All media servers unreachable")
        return -1, 0
    log.debug("Aggregated: %d stream(s), %.1f Mbps total", total_count, total_bps / 1_000_000)
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
            log.warning("qbt %s POST %s failed: %s", self.base, path, e)
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
                        log.debug("qbt login successful at %s", self.base)
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
            log.info("qbt %s: auth expired, re-authenticating", self.base)
            self.cookie = None
            if self.login():
                ok1 = self._post("/api/v2/transfer/setDownloadLimit", {"limit": dl_bytes})
                ok2 = self._post("/api/v2/transfer/setUploadLimit", {"limit": ul_bytes})
                if ok1 and ok2:
                    log.info("qbt %s: retry succeeded after re-login", self.base)
                else:
                    log.warning("qbt %s: retry failed after re-login", self.base)
            else:
                log.error("qbt %s: re-login failed, giving up for this cycle", self.base)
        return ok1 and ok2

    def ensure_logged_in(self):
        if not self.cookie:
            return self.login()
        return True

    def _get_json(self, path):
        """Authenticated GET request returning parsed JSON, or None on failure."""
        url = f"{self.base}{path}"
        req = Request(url)
        if self.cookie:
            req.add_header("Cookie", self.cookie)
        try:
            with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                return json.loads(resp.read().decode())
        except HTTPError as e:
            if e.code == 403 and self.cookie:
                # Auth expired — re-login and retry once
                self.cookie = None
                if self.login():
                    req2 = Request(url)
                    req2.add_header("Cookie", self.cookie)
                    try:
                        with urlopen(req2, timeout=REQUEST_TIMEOUT) as resp:
                            return json.loads(resp.read().decode())
                    except (URLError, HTTPError, ValueError) as e2:
                        log.warning("qbt %s GET %s retry failed: %s", self.base, path, e2)
            else:
                log.warning("qbt %s GET %s failed: %s", self.base, path, e)
            return None
        except (URLError, ValueError) as e:
            log.warning("qbt %s GET %s failed: %s", self.base, path, e)
            return None

    def get_torrent_activity(self, dl_threshold=0, ul_threshold=0):
        """
        Returns (dl_count, ul_count): number of torrents that are genuinely
        active above the given speed thresholds and not in a stalled state.
        Stalled states (stalledDL / stalledUP) are always excluded.
        Returns None if the query fails — callers should treat a failed
        instance as active (fail-open).
        """
        dl_data = self._get_json("/api/v2/torrents/info?filter=downloading")
        ul_data = self._get_json("/api/v2/torrents/info?filter=seeding")
        if dl_data is None or ul_data is None:
            return None
        dl_count = sum(
            1 for t in dl_data
            if t.get("state") != "stalledDL"
            and t.get("dlspeed", 0) > dl_threshold
        )
        ul_count = sum(
            1 for t in ul_data
            if t.get("state") != "stalledUP"
            and t.get("upspeed", 0) > ul_threshold
        )
        return dl_count, ul_count


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
    global last_dl_limit, last_ul_limit, last_racing_active, last_detail, last_activity

    racing_active = _is_racing_window()

    # Log racing window transitions
    if last_racing_active is not None and racing_active != last_racing_active:
        if racing_active:
            log.info("Entering racing window (port %d gets priority)", RACING_INSTANCE_PORT)
        else:
            log.info("Exiting racing window, resuming normal split")

    num_instances = len(clients)
    dynamic_split_active = (
        QBT_SPLIT_BETWEEN_INSTANCES
        and QBT_DYNAMIC_SPLIT
        and num_instances > 1
        and not racing_active   # racing window has its own allocation logic
        and not DRY_RUN         # don't make extra API calls in dry-run
    )

    # Pre-query per-instance torrent activity so we know which instances are
    # actively downloading/seeding *before* the tolerance check — activity
    # changes must always trigger a re-apply even if the total budget is same.
    if dynamic_split_active:
        raw_activity = {}
        for client in clients:
            counts = client.get_torrent_activity(
                    QBT_ACTIVE_DL_THRESHOLD, QBT_ACTIVE_UL_THRESHOLD)
            if counts is None:
                # Query failed — treat as 1 active in each direction so the
                # instance gets an equal share (fail-open, not penalised).
                raw_activity[id(client)] = (1, 1)
            else:
                raw_activity[id(client)] = counts  # (dl_cnt, ul_cnt)

        # Sum of torrent counts across active instances (excludes idle zeros).
        dl_active_total = sum(cnt for cnt, _ in raw_activity.values() if cnt > 0)
        ul_active_total = sum(cnt for _, cnt in raw_activity.values() if cnt > 0)

        # If no instance is active in a direction, treat all as having 1 active
        # torrent so the budget is split equally (no locking at the floor).
        if dl_active_total == 0:
            raw_activity = {k: (1, ul) for k, (_, ul) in raw_activity.items()}
            dl_active_total = num_instances
        if ul_active_total == 0:
            raw_activity = {k: (dl, 1) for k, (dl, _) in raw_activity.items()}
            ul_active_total = num_instances
        activity = raw_activity
    else:
        activity = {id(c): (1, 1) for c in clients}
        dl_active_total = num_instances
        ul_active_total = num_instances

    # Skip if limits haven't changed meaningfully (within 1% tolerance)
    # But always re-apply when label/detail/activity changes.
    if not force and last_dl_limit is not None and last_ul_limit is not None:
        dl_diff = abs(dl_bytes - last_dl_limit) / max(last_dl_limit, 1)
        ul_diff = abs(ul_bytes - last_ul_limit) / max(last_ul_limit, 1)
        detail_changed = detail != last_detail
        activity_changed = QBT_DYNAMIC_SPLIT and activity != last_activity
        if (dl_diff < 0.01 and ul_diff < 0.01
                and racing_active == last_racing_active
                and not detail_changed and not activity_changed):
            log.debug("Limits unchanged (dl\u00b1%.1f%% ul\u00b1%.1f%%), skipping",
                      dl_diff * 100, ul_diff * 100)
            return

    for client in clients:
        client_port = int(client.base.rsplit(":", 1)[-1])
        dl_cnt, ul_cnt = activity[id(client)]
        is_dl_active = dl_cnt > 0
        is_ul_active = ul_cnt > 0

        if racing_active and num_instances > 1:
            # During racing window: cap the media instance, give the rest to racing
            if client_port == RACING_INSTANCE_PORT:
                # Racing instance gets total minus the non-racing cap.
                # DL and UL are handled independently: 0 means unlimited for
                # that direction, not a signal to skip the UL budget.
                c_dl = 0 if dl_bytes == 0 else max(dl_bytes - RACING_NON_RACING_DL_LIMIT, MIN_QBT_DL_BYTES)
                c_ul = 0 if ul_bytes == 0 else max(ul_bytes - RACING_NON_RACING_UL_LIMIT, MIN_QBT_UL_BYTES)
            else:
                # Non-racing (media) instance gets hard cap
                c_dl = RACING_NON_RACING_DL_LIMIT
                c_ul = RACING_NON_RACING_UL_LIMIT
        elif QBT_SPLIT_BETWEEN_INSTANCES and num_instances > 1:
            if dynamic_split_active:
                # Proportional split: each instance's share is weighted by its
                # active torrent count.  Idle instances (count=0) get the MIN
                # floor; active instances get (my_count / total_active_count).
                if dl_bytes == 0:
                    c_dl = 0
                elif is_dl_active:
                    c_dl = max(int(dl_bytes * dl_cnt / dl_active_total), MIN_QBT_DL_BYTES)
                else:
                    c_dl = MIN_QBT_DL_BYTES

                if ul_bytes == 0:
                    c_ul = 0
                elif is_ul_active:
                    c_ul = max(int(ul_bytes * ul_cnt / ul_active_total), MIN_QBT_UL_BYTES)
                else:
                    c_ul = MIN_QBT_UL_BYTES
            else:
                # Static equal split. 0 (unlimited) stays unlimited;
                # positive budgets are divided evenly between instances.
                c_dl = 0 if dl_bytes == 0 else max(dl_bytes // num_instances, MIN_QBT_DL_BYTES)
                c_ul = 0 if ul_bytes == 0 else max(ul_bytes // num_instances, MIN_QBT_UL_BYTES)
        else:
            c_dl, c_ul = dl_bytes, ul_bytes

        extra = ""
        if racing_active and num_instances > 1:
            is_racer = client_port == RACING_INSTANCE_PORT
            extra = " [RACING]" if is_racer else " [CAPPED]"
        elif dynamic_split_active:
            if not is_dl_active and not is_ul_active:
                extra = " [IDLE]"
            elif not is_dl_active:
                extra = " [IDLE-DL]"
            elif not is_ul_active:
                extra = " [IDLE-UL]"

        if DRY_RUN:
            log.info("[DRY-RUN] [%s] %s%s: dl=%s ul=%s%s", label, client.base, extra,
                     _fmt_speed(c_dl), _fmt_speed(c_ul),
                     f" ({detail})" if detail else "")
            continue
        if not client.ensure_logged_in():
            log.error("qbt %s: login failed, skipping this cycle", client.base)
            continue
        ok = client.set_speed_limits(c_dl, c_ul)
        if ok:
            log.info("[%s] %s%s: dl=%s ul=%s%s", label, client.base, extra,
                     _fmt_speed(c_dl), _fmt_speed(c_ul),
                     f" ({detail})" if detail else "")
        else:
            client.cookie = None
            log.error("qbt %s: failed to set limits, will retry next cycle", client.base)

    last_dl_limit = dl_bytes
    last_ul_limit = ul_bytes
    last_racing_active = racing_active
    last_detail = detail
    last_activity = activity

# ---------------------------------------------------------------------------
# Limit calculation
# ---------------------------------------------------------------------------

def calculate_limits(session_count, stream_bps):
    """
    Given active stream count and their total reported bitrate,
    return (dl_bytes_per_sec, ul_bytes_per_sec) for qBittorrent.
    """
    # Apply overhead factor to account for burst buffering etc.
    reserved_bps = stream_bps * STREAM_OVERHEAD_FACTOR

    remaining_dl_bps = max(0, TOTAL_BANDWIDTH_BPS - reserved_bps)
    remaining_ul_bps = max(0, TOTAL_UPLOAD_BPS - reserved_bps)

    ul_bps = remaining_ul_bps * QBT_UPLOAD_FRACTION

    # QBT_HEADROOM_FRACTION >= 1.0 means "don't throttle downloads" — use 0 (unlimited).
    # Values < 1.0 apply a proportional cap on remaining download bandwidth.
    if QBT_HEADROOM_FRACTION >= 1.0:
        dl_bytes = NORMAL_DL_BYTES  # 0 = unlimited
    else:
        dl_bps = remaining_dl_bps * QBT_HEADROOM_FRACTION
        dl_bytes = max(int(dl_bps / 8), MIN_QBT_DL_BYTES)
    ul_bytes = max(int(ul_bps / 8), MIN_QBT_UL_BYTES)

    log.debug("Calc: streams=%.1f Mbps, reserved=%.1f Mbps (x%.2f), "
              "remaining DL=%.1f/UL=%.1f Mbps, result DL=%s UL=%s",
              stream_bps / 1_000_000, reserved_bps / 1_000_000,
              STREAM_OVERHEAD_FACTOR, remaining_dl_bps / 1_000_000,
              remaining_ul_bps / 1_000_000, _fmt_speed(dl_bytes), _fmt_speed(ul_bytes))

    stream_mbps = stream_bps / 1_000_000
    detail = (f"{session_count} stream(s), "
              f"using ~{stream_mbps:.0f} Mbps, "
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
        errors.append("No media server configured — set PLEX_URL+PLEX_TOKEN, "
                      "JELLYFIN_URL+JELLYFIN_TOKEN, or EMBY_URL+EMBY_TOKEN")
    if not QBT_INSTANCES:
        errors.append("No valid QBT_INSTANCES configured")
    if UNREACHABLE_ACTION not in ("keep", "unlimited"):
        errors.append(f"Invalid UNREACHABLE_ACTION={UNREACHABLE_ACTION!r} "
                      f"(must be 'keep' or 'unlimited')")
    if QBT_HEADROOM_FRACTION < 0 or QBT_HEADROOM_FRACTION > 1:
        errors.append(f"QBT_HEADROOM_FRACTION={QBT_HEADROOM_FRACTION} out of range [0, 1]")
    if QBT_UPLOAD_FRACTION < 0 or QBT_UPLOAD_FRACTION > 1:
        errors.append(f"QBT_UPLOAD_FRACTION={QBT_UPLOAD_FRACTION} out of range [0, 1]")
    if errors:
        for e in errors:
            log.error("Config error: %s", e)
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
    instance_list = ", ".join(f"{s}://{h}:{p}" for h, p, _, _, s in QBT_INSTANCES)
    log.info("qbt-flow starting%s", " (DRY-RUN)" if DRY_RUN else "")
    log.info("  Media servers : %s", server_names)
    log.info("  qBt instances : %s", instance_list)
    log.info("  Line speed    : DL %.0f Mbps / UL %.0f Mbps",
             TOTAL_BANDWIDTH_BPS / 1_000_000, TOTAL_UPLOAD_BPS / 1_000_000)
    log.info("  Poll interval : %ds", POLL_INTERVAL)
    log.info("  DL fraction=%.2f  UL fraction=%.2f  overhead=%.2fx  "
             "min DL=%s  min UL=%s",
             QBT_HEADROOM_FRACTION, QBT_UPLOAD_FRACTION,
             STREAM_OVERHEAD_FACTOR, _fmt_speed(MIN_QBT_DL_BYTES),
             _fmt_speed(MIN_QBT_UL_BYTES))
    log.info("  Unreachable=%s  ramp_steps=%d  split=%s%s",
             UNREACHABLE_ACTION, RAMP_UP_STEPS, QBT_SPLIT_BETWEEN_INSTANCES,
             " (dynamic)" if QBT_DYNAMIC_SPLIT else "")
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
                if _status["label"] != "UNREACHABLE":
                    log.warning("Entering UNREACHABLE state (action=%s)", UNREACHABLE_ACTION)
                _status["label"] = "UNREACHABLE"
                if UNREACHABLE_ACTION != "keep":
                    apply_limits(NORMAL_DL_BYTES, NORMAL_UL_BYTES, "NORMAL")
            else:
                if _status["label"] == "UNREACHABLE":
                    log.info("Media server(s) reachable again")
                _status["streams"] = session_count

                if session_count == 0:
                    # Start ramp-up if transitioning from active streams.
                    # Use last_ul_limit for the guard — last_dl_limit can be 0
                    # (unlimited) when QBT_HEADROOM_FRACTION >= 1.0, making it
                    # falsy even when throttling was actively applied.
                    if prev_session_count > 0 and RAMP_UP_STEPS > 0 and last_ul_limit:
                        ramp_remaining = RAMP_UP_STEPS
                        ramp_dl = last_dl_limit
                        ramp_ul = last_ul_limit or 0
                        log.info("All streams stopped, ramping up over %d steps", RAMP_UP_STEPS)

                    if ramp_remaining > 0:
                        ramp_remaining -= 1
                        if ramp_remaining == 0:
                            log.info("Ramp-up complete, removing limits")
                            apply_limits(NORMAL_DL_BYTES, NORMAL_UL_BYTES, "NORMAL")
                        else:
                            ramp_dl *= 2
                            ramp_ul *= 2
                            max_bw = int(TOTAL_BANDWIDTH_BPS / 8)
                            max_ul = int(TOTAL_UPLOAD_BPS / 8)
                            if (ramp_dl and ramp_dl >= max_bw) or ramp_ul >= max_ul:
                                log.info("Ramp-up reached line speed, removing limits")
                                apply_limits(NORMAL_DL_BYTES, NORMAL_UL_BYTES, "NORMAL")
                                ramp_remaining = 0
                            else:
                                step = RAMP_UP_STEPS - ramp_remaining
                                apply_limits(ramp_dl, ramp_ul, "RAMP-UP",
                                             f"step {step}/{RAMP_UP_STEPS}")
                    else:
                        apply_limits(NORMAL_DL_BYTES, NORMAL_UL_BYTES, "NORMAL")
                else:
                    if prev_session_count == 0:
                        log.info("%d stream(s) detected, entering throttle mode", session_count)
                    elif session_count != prev_session_count:
                        log.info("Stream count changed: %d → %d", prev_session_count, session_count)
                    ramp_remaining = 0
                    dl_bytes, ul_bytes, detail = calculate_limits(session_count, server_bps)
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
