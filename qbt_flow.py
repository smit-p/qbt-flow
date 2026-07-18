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
import base64
import ipaddress
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
from urllib.parse import urlencode, urlparse, parse_qs
from urllib.error import URLError, HTTPError

__version__ = "1.5.0"

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
    # With /s suffix (e.g. "10MB/s") or bare suffix (e.g. "10MB" / "10MBps")
    "b/s": 1, "kb/s": 1024, "mb/s": 1024**2, "gb/s": 1024**3,
    "b": 1, "kb": 1024, "mb": 1024**2, "gb": 1024**3,
}
_SPEED_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([a-zA-Z/]+)\s*$")
# Suffixes that denote *bytes*/sec (as opposed to bits/sec).  Used to normalise
# byte-denominated values to bits when a caller needs bits (see `as_bits`).
_BYTE_SPEED_SUFFIXES = frozenset({"b/s", "kb/s", "mb/s", "gb/s", "b", "kb", "mb", "gb"})

def _parse_speed(raw, as_bits=False):
    """Parse a human-readable speed value into a number.

    Accepts plain numbers (returned as-is) or numbers with a suffix:
      Bits/sec:  Kbps, Mbps, Gbps   (e.g. "1Gbps" → 1 000 000 000)
      Bytes/sec: KB/s, MB/s, GB/s   (e.g. "10MB/s" → 10 485 760)

    By default the return unit follows the suffix: bits for bit-suffixes,
    bytes for byte-suffixes, and plain numbers pass through unchanged.  This is
    fine for byte-denominated settings (MIN_QBT_*, thresholds, racing caps).

    Pass ``as_bits=True`` for bandwidth *totals* (TOTAL_BANDWIDTH / TOTAL_UPLOAD),
    which are consumed as bits/sec: a byte-suffixed value (e.g. "125MB/s") is
    then multiplied by 8 so it means the same as its bit equivalent ("1Gbps").
    Without this, "125MB/s" would be read as ~131 Mbit and silently give ~8x
    less bandwidth than the docs claim it's equivalent to.
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
            value = number * mult
            if as_bits and suffix in _BYTE_SPEED_SUFFIXES:
                value *= 8  # bytes/sec → bits/sec
            return value
    # Plain number or unrecognised suffix — fall through to float()
    try:
        return float(raw)
    except ValueError:
        return 0

def _env_speed(key, default, as_bits=False):
    """Read a speed env-var, returning *default* if unset."""
    val = os.environ.get(key)
    if val is not None:
        return _parse_speed(val, as_bits=as_bits)
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
# Tautulli — aggregates Plex activity and reports per-session bandwidth and
# LAN/WAN location directly.  TAUTULLI_TOKEN is the Tautulli API key.
TAUTULLI_URL   = _env("TAUTULLI_URL", "")
TAUTULLI_TOKEN = _env("TAUTULLI_TOKEN", "")

# Default Plex URL when nothing else is configured
if not (PLEX_URL or JELLYFIN_URL or EMBY_URL or TAUTULLI_URL):
    PLEX_URL = "http://localhost:32400"

# Torrent-client instances as "host:port:user:pass[:scheme]" comma-separated
# pairs, e.g. host:8080:admin:password,host:8443:admin:password:https
def _parse_instances(raw, var_name):
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
                print(f"WARNING: invalid scheme {scheme!r} in {var_name} entry: {entry!r}", file=sys.stderr)
                scheme = "http"
            instances.append((host, int(port), user, password, scheme))
        else:
            print(f"WARNING: invalid {var_name} entry: {entry!r} (expected host:port:user:pass[:scheme])", file=sys.stderr)
    return instances

QBT_INSTANCES = _parse_instances(_env("QBT_INSTANCES", ""), "QBT_INSTANCES")
# Transmission instances (same host:port:user:pass[:scheme] format).
TRANSMISSION_INSTANCES = _parse_instances(_env("TRANSMISSION_INSTANCES", ""), "TRANSMISSION_INSTANCES")

# Your line speed (download).  Accepts plain numbers (bits/sec) or suffixes:
#   1Gbps, 500Mbps, 100Mbps (bits) or 125MB/s (bytes — normalised to bits).
# These totals are consumed as bits/sec, so parse with as_bits=True.
TOTAL_BANDWIDTH_BPS   = _env_speed("TOTAL_BANDWIDTH", 1_000_000_000, as_bits=True)  # 1 Gbps
# Upload line speed — defaults to TOTAL_BANDWIDTH if not set
TOTAL_UPLOAD_BPS      = _env_speed("TOTAL_UPLOAD", TOTAL_BANDWIDTH_BPS, as_bits=True)

# Fraction of remaining bandwidth (after media streams) to give qBittorrent.
# Download defaults to 1.0 (no throttling) because media streams use upload,
# not download.  Lower this only if you have a specific reason (e.g. bufferbloat
# without router-level QoS).
QBT_HEADROOM_FRACTION = _env_float("QBT_HEADROOM_FRACTION", 1.0)
QBT_UPLOAD_FRACTION   = _env_float("QBT_UPLOAD_FRACTION",   0.9)

# Whether to split bandwidth evenly across qBittorrent instances.
# If true, each instance gets (total / N). If false, each gets the full amount.
QBT_SPLIT_BETWEEN_INSTANCES = _env("QBT_SPLIT_BETWEEN_INSTANCES", "true").lower() in ("true", "1", "yes")

# When true, if an instance has qBittorrent's *alternative* speed limits toggled
# on, qbt-flow leaves that instance's limits untouched for the cycle.  This lets
# you manually switch an instance into a hard-throttled state (via the qBt UI or
# scheduler) without qbt-flow overwriting the values you set.  Default off to
# preserve existing behaviour.
QBT_RESPECT_ALT_LIMITS = _env("QBT_RESPECT_ALT_LIMITS", "false").lower() in ("true", "1", "yes")

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
# Warn if the env var was explicitly set but couldn't be parsed (unrecognised
# suffix).  The logger isn't configured yet at this point in module load, so
# collect the warnings and emit them once `log` exists (see the Logging block).
_config_warnings = []
for _thr_key, _thr_val in (("QBT_ACTIVE_DL_THRESHOLD", QBT_ACTIVE_DL_THRESHOLD),
                            ("QBT_ACTIVE_UL_THRESHOLD", QBT_ACTIVE_UL_THRESHOLD)):
    _raw = os.environ.get(_thr_key, "")
    if _raw and _raw.strip() not in ("0", "") and _thr_val == 0:
        _config_warnings.append(
            "Config warning: %s=%r could not be parsed — treating as 0 "
            "(unrecognised suffix?). Use formats like '500KB', '1MB/s', '500000'."
            % (_thr_key, _raw)
        )
del _thr_key, _thr_val, _raw

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
# Poll interval while streams are active (or ramping down).  Defaults to
# POLL_INTERVAL so behaviour is unchanged; set it lower (e.g. 5) to react
# faster to stream count changes without hammering servers when idle.
POLL_INTERVAL_ACTIVE = _env_int("POLL_INTERVAL_ACTIVE", POLL_INTERVAL)
REQUEST_TIMEOUT = _env_int("REQUEST_TIMEOUT", 10)

# Ignore streams playing to a LAN/local client — those consume no WAN upload,
# so there's no need to throttle torrents for them.  Detected via the client IP
# (private / loopback / link-local ranges) and, for Plex, the Player 'local'
# flag.  Default off to preserve existing behaviour.
IGNORE_LAN_STREAMS = _env("IGNORE_LAN_STREAMS", "false").lower() in ("true", "1", "yes")

# Optional shared secret for the /webhook endpoint.  When set, requests must
# supply it via ?token=... or an X-Webhook-Token header.  Empty = no auth.
WEBHOOK_TOKEN = _env("WEBHOOK_TOKEN", "")

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

# Always log to stdout; add a rotating file handler unless LOG_FILE is empty
# (set LOG_FILE= to log to stdout only, e.g. in Docker where the platform
# captures container logs).
_log_handlers = [logging.StreamHandler(sys.stdout)]
if LOG_FILE:
    _log_handlers.insert(0, logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=LOG_MAX_SIZE, backupCount=LOG_BACKUP_COUNT
    ))
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=_log_handlers,
)
log = logging.getLogger("qbt_flow")

# Flush any config warnings collected before the logger was ready.
for _msg in _config_warnings:
    log.warning("%s", _msg)
del _config_warnings

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

last_dl_limit = None
last_ul_limit = None
last_racing_active = None
last_detail = None
last_activity: dict = {}   # id(client) -> (dl_count: int, ul_count: int)
last_alt_skipped = False   # True if any instance was skipped for active alt limits last cycle
last_apply_failed = False  # True if any instance failed login/set last cycle (retry must not be skipped)

_start_time = 0.0
_status = {
    "version": __version__,
    "streams": 0,
    "stream_bandwidth_bps": 0,
    "dl_limit": 0,
    "ul_limit": 0,
    "racing_active": False,
    "label": "STARTING",
    "media_servers": [],
    "torrent_clients": 0,
    "last_webhook": 0,
}

stop_event = threading.Event()
# Set to wake the main loop early (webhook received, or shutdown).  The loop
# waits on this instead of sleeping a fixed interval, so a webhook triggers an
# immediate re-poll.
wake_event = threading.Event()

def handle_signal(signum, frame):
    log.info("Received signal %d, shutting down", signum)
    stop_event.set()
    wake_event.set()  # break the poll wait immediately

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

# --dry-run support (set via CLI arg)
DRY_RUN = False


def _fmt_speed(bytes_per_sec):
    """Format a speed value for logging: 0 → 'unlimited', else '12.3 MB/s'."""
    return f"{bytes_per_sec / (1024 * 1024):.1f} MB/s" if bytes_per_sec else "unlimited"


# Networks considered "local" — RFC1918 + loopback + link-local + IPv6 ULA.
# Deliberately narrower than ipaddress.is_private, which also flags CGNAT
# (100.64/10) and documentation ranges we'd want treated as remote (WAN).
_LAN_NETWORKS = tuple(ipaddress.ip_network(n) for n in (
    "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",  # RFC1918
    "127.0.0.0/8", "169.254.0.0/16",                   # loopback, link-local
    "fc00::/7", "fe80::/10", "::1/128",                # IPv6 ULA, link-local, loopback
))


def _client_is_lan(addr):
    """
    True if *addr* is on the local network (RFC1918 / loopback / link-local /
    IPv6 ULA) — a client whose stream consumes no WAN upload.  Accepts a bare
    IP or a "host:port" / "[v6]:port" form (as returned by Jellyfin/Emby's
    RemoteEndPoint or Plex's Player address).  Unparseable input → False (i.e.
    treated as remote, the safe default: we'd rather throttle than under-throttle).
    """
    if not addr:
        return False
    addr = str(addr).strip()
    if addr.startswith("["):          # [v6]:port or [v6]
        addr = addr[1:].split("]", 1)[0]
    elif addr.count(":") == 1:        # ipv4:port
        addr = addr.split(":", 1)[0]
    try:
        ip = ipaddress.ip_address(addr)
    except ValueError:
        return False
    return any(ip in net for net in _LAN_NETWORKS)


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
            if IGNORE_LAN_STREAMS and player is not None and (
                    player.attrib.get("local") == "1"
                    or _client_is_lan(player.attrib.get("address", ""))):
                log.debug("Plex: skipping LAN session '%s'", session.attrib.get("title", "unknown"))
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
            if IGNORE_LAN_STREAMS and _client_is_lan(session.get("RemoteEndPoint", "")):
                log.debug("%s: skipping LAN session '%s'",
                          server_type, now_playing.get("Name", "unknown"))
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
# Tautulli helpers
# ---------------------------------------------------------------------------

def get_tautulli_sessions(url=None, token=None):
    """
    Tautulli session fetcher via its get_activity API.  Tautulli reports the
    real per-session bandwidth (kbps) and the stream location (lan/wan), which
    it derives from Plex — handy for IGNORE_LAN_STREAMS.
    Returns (session_count, total_bitrate_bps).  (-1, 0) on error.
    """
    url = url or TAUTULLI_URL
    token = token or TAUTULLI_TOKEN
    req_url = f"{url}/api/v2?apikey={token}&cmd=get_activity"
    req = Request(req_url, headers={"Accept": "application/json"})
    try:
        with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            payload = json.loads(resp.read())
        data = payload.get("response", {}).get("data") or {}
        sessions = data.get("sessions", [])
        count = 0
        total_kbps = 0
        for session in sessions:
            state = session.get("state", "playing")
            if state in ("paused", "stopped", "buffering"):
                continue
            if IGNORE_LAN_STREAMS and str(session.get("location", "")).lower() == "lan":
                log.debug("Tautulli: skipping LAN session '%s'", session.get("full_title", "unknown"))
                continue
            count += 1
            try:
                total_kbps += int(float(session.get("bandwidth") or 0))
            except (TypeError, ValueError):
                pass
        total_bps = total_kbps * 1000
        log.debug("Tautulli: %d active stream(s), total %d kbps", count, total_kbps)
        return count, total_bps
    except (URLError, HTTPError, json.JSONDecodeError, ValueError, KeyError) as e:
        log.warning("Tautulli session check failed (%s): %s", url, e)
        return -1, 0


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
if TAUTULLI_URL and TAUTULLI_TOKEN:
    _configured_servers.append(("tautulli", TAUTULLI_URL, TAUTULLI_TOKEN, get_tautulli_sessions))

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
        all_in_backoff = all(
            _server_backoffs.get(name, BackoffTracker()).should_skip()
            for name, _, _, _ in _configured_servers
        )
        if all_in_backoff:
            log.debug("All media servers in backoff, treating as unreachable")
        else:
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
                    if part.startswith("SID=") or part.startswith("QBT_SID_"):
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

    def is_alt_limits_active(self):
        """
        Return True if qBittorrent's alternative speed limits are currently
        enabled for this instance.  Used to leave the user's manually-toggled
        alt limits untouched when QBT_RESPECT_ALT_LIMITS is on.

        The endpoint returns the bare text "1" (enabled) or "0" (disabled),
        both of which parse cleanly as JSON.  On any query failure we return
        False (fail-open) so limits are still applied rather than silently
        skipped.
        """
        mode = self._get_json("/api/v2/transfer/speedLimitsMode")
        return mode == 1

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


# ---------------------------------------------------------------------------
# Transmission client (RPC) — same interface as QbtClient so apply_limits and
# the dynamic-split logic work with either client type interchangeably.
# ---------------------------------------------------------------------------

class TransmissionClient:
    def __init__(self, host, port, username, password, scheme="http"):
        self.base = f"{scheme}://{host}:{port}"
        self.rpc_url = f"{self.base}/transmission/rpc"
        self.username = username
        self.password = password
        self.session_id = None
        # `cookie` is set to None by apply_limits on failure; keep the attribute
        # for interface parity with QbtClient (Transmission uses session_id).
        self.cookie = "transmission"

    def _auth_header(self):
        if not self.username:
            return {}
        token = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        return {"Authorization": f"Basic {token}"}

    def _rpc(self, method, arguments=None, _retry=True):
        """Perform a Transmission RPC call, handling the 409 session-id handshake."""
        body = json.dumps({"method": method, "arguments": arguments or {}}).encode()
        headers = {"Content-Type": "application/json"}
        headers.update(self._auth_header())
        if self.session_id:
            headers["X-Transmission-Session-Id"] = self.session_id
        req = Request(self.rpc_url, data=body, headers=headers)
        try:
            with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                return json.loads(resp.read().decode())
        except HTTPError as e:
            if e.code == 409 and _retry:
                # Transmission hands back a fresh session id in the 409 headers.
                self.session_id = e.headers.get("X-Transmission-Session-Id")
                if self.session_id:
                    return self._rpc(method, arguments, _retry=False)
            log.warning("transmission %s RPC %s failed: %s", self.base, method, e)
            return None
        except (URLError, ValueError) as e:
            log.warning("transmission %s RPC %s failed: %s", self.base, method, e)
            return None

    def login(self):
        # A no-arg session-get both validates credentials and primes session_id.
        resp = self._rpc("session-get")
        ok = bool(resp) and resp.get("result") == "success"
        if ok:
            log.debug("transmission login successful at %s", self.base)
        else:
            log.warning("transmission login failed at %s", self.base)
        return ok

    def ensure_logged_in(self):
        if not self.session_id:
            return self.login()
        return True

    def set_speed_limits(self, dl_bytes, ul_bytes):
        # Transmission speed limits are in KB/s; 0 bytes means "unlimited"
        # (disable the limit rather than set it to 0).
        args = {}
        if dl_bytes and dl_bytes > 0:
            args["speed-limit-down"] = max(1, int(dl_bytes // 1024))
            args["speed-limit-down-enabled"] = True
        else:
            args["speed-limit-down-enabled"] = False
        if ul_bytes and ul_bytes > 0:
            args["speed-limit-up"] = max(1, int(ul_bytes // 1024))
            args["speed-limit-up-enabled"] = True
        else:
            args["speed-limit-up-enabled"] = False
        resp = self._rpc("session-set", args)
        return bool(resp) and resp.get("result") == "success"

    def is_alt_limits_active(self):
        """True if Transmission's alternative ('turtle') speed limits are on."""
        resp = self._rpc("session-get")
        if not resp or resp.get("result") != "success":
            return False
        return bool(resp.get("arguments", {}).get("alt-speed-enabled", False))

    def get_torrent_activity(self, dl_threshold=0, ul_threshold=0):
        """
        Returns (dl_count, ul_count) of torrents actively transferring above the
        thresholds.  Transmission status codes: 4 = downloading, 6 = seeding.
        Returns None on query failure (callers treat that as active, fail-open).
        """
        resp = self._rpc("torrent-get", {"fields": ["status", "rateDownload", "rateUpload"]})
        if not resp or resp.get("result") != "success":
            return None
        torrents = resp.get("arguments", {}).get("torrents", [])
        dl_count = sum(
            1 for t in torrents
            if t.get("status") == 4 and t.get("rateDownload", 0) > dl_threshold
        )
        ul_count = sum(
            1 for t in torrents
            if t.get("status") == 6 and t.get("rateUpload", 0) > ul_threshold
        )
        return dl_count, ul_count


clients = (
    [QbtClient(h, p, u, pw, s) for h, p, u, pw, s in QBT_INSTANCES]
    + [TransmissionClient(h, p, u, pw, s) for h, p, u, pw, s in TRANSMISSION_INSTANCES]
)


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
    global last_dl_limit, last_ul_limit, last_racing_active, last_detail, last_activity, \
        last_alt_skipped, last_apply_failed

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
    # If an instance was skipped last cycle (active alt limits) or failed
    # (login/set error), never short-circuit: the promised retry / re-push must
    # actually happen, otherwise the instance would stay on stale limits until
    # the budget happened to drift by >1%.
    if not force and last_dl_limit is not None and last_ul_limit is not None \
            and not last_alt_skipped and not last_apply_failed:
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

    alt_skipped = False
    apply_failed = False
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
            apply_failed = True
            continue
        if QBT_RESPECT_ALT_LIMITS and client.is_alt_limits_active():
            log.info("[%s] %s%s: alternative speed limits active, leaving limits untouched",
                     label, client.base, extra)
            alt_skipped = True
            continue
        ok = client.set_speed_limits(c_dl, c_ul)
        if ok:
            log.info("[%s] %s%s: dl=%s ul=%s%s", label, client.base, extra,
                     _fmt_speed(c_dl), _fmt_speed(c_ul),
                     f" ({detail})" if detail else "")
        else:
            client.cookie = None
            apply_failed = True
            log.error("qbt %s: failed to set limits, will retry next cycle", client.base)

    last_dl_limit = dl_bytes
    last_ul_limit = ul_bytes
    last_racing_active = racing_active
    last_detail = detail
    last_activity = activity
    last_alt_skipped = alt_skipped
    last_apply_failed = apply_failed

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
            _label = _status["label"]
            lines = [
                '# HELP qbt_flow_up Whether qbt-flow is running.',
                '# TYPE qbt_flow_up gauge',
                'qbt_flow_up 1',
                '# HELP qbt_flow_build_info Build/version info.',
                '# TYPE qbt_flow_build_info gauge',
                f'qbt_flow_build_info{{version="{__version__}"}} 1',
                '# HELP qbt_flow_streams Active media streams being reserved for.',
                '# TYPE qbt_flow_streams gauge',
                f'qbt_flow_streams {_status["streams"]}',
                '# HELP qbt_flow_stream_bandwidth_bps Reported bitrate of active streams (bits/sec).',
                '# TYPE qbt_flow_stream_bandwidth_bps gauge',
                f'qbt_flow_stream_bandwidth_bps {_status["stream_bandwidth_bps"]}',
                '# HELP qbt_flow_dl_limit_bytes Current per-cycle download limit (0 = unlimited).',
                '# TYPE qbt_flow_dl_limit_bytes gauge',
                f'qbt_flow_dl_limit_bytes {_status["dl_limit"]}',
                '# HELP qbt_flow_ul_limit_bytes Current per-cycle upload limit (0 = unlimited).',
                '# TYPE qbt_flow_ul_limit_bytes gauge',
                f'qbt_flow_ul_limit_bytes {_status["ul_limit"]}',
                '# HELP qbt_flow_racing_active Whether the racing window is active.',
                '# TYPE qbt_flow_racing_active gauge',
                f'qbt_flow_racing_active {1 if _status["racing_active"] else 0}',
                '# HELP qbt_flow_torrent_clients Number of configured torrent clients.',
                '# TYPE qbt_flow_torrent_clients gauge',
                f'qbt_flow_torrent_clients {_status["torrent_clients"]}',
                '# HELP qbt_flow_state_info Current state label.',
                '# TYPE qbt_flow_state_info gauge',
                f'qbt_flow_state_info{{label="{_label}"}} 1',
                '# HELP qbt_flow_last_webhook_timestamp_seconds Unix time of the last webhook received.',
                '# TYPE qbt_flow_last_webhook_timestamp_seconds gauge',
                f'qbt_flow_last_webhook_timestamp_seconds {_status["last_webhook"]}',
                '# HELP qbt_flow_uptime_seconds Seconds since start.',
                '# TYPE qbt_flow_uptime_seconds counter',
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

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path in ("/webhook", "/notify"):
            if WEBHOOK_TOKEN:
                supplied = (parse_qs(parsed.query).get("token", [""])[0]
                            or self.headers.get("X-Webhook-Token", ""))
                if supplied != WEBHOOK_TOKEN:
                    self.send_response(403)
                    self.end_headers()
                    return
            # Drain the request body so the sender doesn't see a broken pipe.
            # The content is irrelevant — any webhook means "re-poll now".
            try:
                length = int(self.headers.get("Content-Length", 0) or 0)
                if length:
                    self.rfile.read(length)
            except (ValueError, OSError):
                pass
            _status["last_webhook"] = int(time.time())
            wake_event.set()
            log.debug("Webhook received, triggering immediate re-poll")
            self.send_response(204)
            self.end_headers()
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
    log.info("Status endpoint listening on port %d (GET /status, /metrics; POST /webhook)",
             STATUS_PORT)
    return server

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def _validate_config():
    errors = []
    if not _configured_servers:
        errors.append("No media server configured — set PLEX_URL+PLEX_TOKEN, "
                      "JELLYFIN_URL+JELLYFIN_TOKEN, EMBY_URL+EMBY_TOKEN, or "
                      "TAUTULLI_URL+TAUTULLI_TOKEN")
    if not QBT_INSTANCES and not TRANSMISSION_INSTANCES:
        errors.append("No torrent client configured — set QBT_INSTANCES "
                      "and/or TRANSMISSION_INSTANCES")
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
    _status["torrent_clients"] = len(clients)

    server_names = "+".join(name for name, _, _, _ in _configured_servers)
    instance_list = ", ".join(
        f"{s}://{h}:{p}" for h, p, _, _, s in (QBT_INSTANCES + TRANSMISSION_INSTANCES))
    log.info("qbt-flow %s starting%s", __version__, " (DRY-RUN)" if DRY_RUN else "")
    log.info("  Media servers : %s", server_names)
    log.info("  Torrent clients: %d qBittorrent, %d Transmission",
             len(QBT_INSTANCES), len(TRANSMISSION_INSTANCES))
    log.info("  Instances     : %s", instance_list)
    log.info("  Line speed    : DL %.0f Mbps / UL %.0f Mbps",
             TOTAL_BANDWIDTH_BPS / 1_000_000, TOTAL_UPLOAD_BPS / 1_000_000)
    log.info("  Poll interval : %ds (idle) / %ds (active)%s",
             POLL_INTERVAL, POLL_INTERVAL_ACTIVE,
             "  ignore-LAN" if IGNORE_LAN_STREAMS else "")
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
                _status["stream_bandwidth_bps"] = int(server_bps)

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
            # Only update label when servers are reachable — don't overwrite
            # "UNREACHABLE" with "RAMP-UP" if a ramp was in progress when
            # the server became unreachable.
            if session_count >= 0:
                if ramp_remaining > 0:
                    _status["label"] = "RAMP-UP"
                elif session_count > 0:
                    _status["label"] = "THROTTLE"
                else:
                    _status["label"] = "NORMAL"

            if stop_event.is_set():
                break
            # Poll faster while streams are active or ramping down; a webhook
            # (wake_event) breaks the wait early for an immediate re-poll.
            active = session_count > 0 or ramp_remaining > 0
            interval = POLL_INTERVAL_ACTIVE if active else POLL_INTERVAL
            if wake_event.wait(interval) and not stop_event.is_set():
                log.debug("Re-polling early (woken by webhook)")
            wake_event.clear()
    finally:
        # Remove throttles on exit so qBittorrent isn't left limited
        if not DRY_RUN:
            log.info("Cleaning up — removing qbt speed limits")
            apply_limits(0, 0, "SHUTDOWN", force=True)
        log.info("qbt_flow stopped")

if __name__ == "__main__":
    main()
