# qbt-flow

Dynamic bandwidth manager for qBittorrent. Adjusts download and upload speed limits in real time based on active media-server streams (Plex, Jellyfin, or Emby) and configurable racing windows. When a stream is playing, qBittorrent is throttled to prevent buffering. When idle, limits are gradually removed and qBittorrent runs at full speed. During racing hours, one instance gets priority while others are hard-capped.

## How it works

1. Every N seconds (default: 15), the script polls your media server for active sessions.
2. It sums the actual stream bitrates (not just media bitrate — real transcoded/direct-play bandwidth).
3. It subtracts that usage (plus a headroom multiplier) from your total line speed.
4. The remaining bandwidth is divided between download and upload. If multiple qBittorrent instances are configured, the limit is split evenly between them (configurable), then pushed to each instance via the Web API.
5. If no streams are active, limits are gradually ramped up (configurable steps) before going unlimited.
6. **Racing window**: during configurable hours, the racing instance gets priority bandwidth while all other instances are hard-capped — regardless of Plex activity.
7. **Exponential backoff**: when the media server is unreachable, retries back off exponentially (2s → 4s → 8s … up to a configurable max) to reduce log noise and unnecessary requests.
8. **Status endpoint**: optional HTTP server exposes a JSON snapshot at `/status` and Prometheus-compatible metrics at `/metrics`.
9. On shutdown (SIGTERM/SIGINT), limits are automatically removed so qBittorrent isn't left throttled.

## Requirements

- Python 3.8+ (no third-party packages — stdlib only)
- Plex, Jellyfin, or Emby media server (accessible via HTTP)
- qBittorrent with Web UI enabled

## Install

```bash
git clone https://github.com/smit-p/qbt-flow.git
cd qbt-flow
cp config.env.example config.env
$EDITOR config.env   # fill in your media server URLs/tokens, QBT_INSTANCES, etc.
python3 qbt_flow.py
```

## Configuration

Copy `config.env.example` to `config.env` and edit the values. All settings can also be passed as environment variables.

| Variable | Default | Description |
|---|---|---|
| `PLEX_URL` | *(none)* | Plex server URL. Set with `PLEX_TOKEN` to enable Plex polling |
| `PLEX_TOKEN` | *(none)* | Plex authentication token ([how to find it](https://support.plex.tv/articles/204059436-finding-an-authentication-token-x-plex-token/)) |
| `JELLYFIN_URL` | *(none)* | Jellyfin URL. Set with `JELLYFIN_TOKEN` to enable Jellyfin polling |
| `JELLYFIN_TOKEN` | *(none)* | Jellyfin API key (Dashboard → API Keys → +) |
| `EMBY_URL` | *(none)* | Emby URL. Set with `EMBY_TOKEN` to enable Emby polling |
| `EMBY_TOKEN` | *(none)* | Emby API key (Dashboard → API Keys → +) |
| `QBT_INSTANCES` | `localhost:8080:admin:adminadmin` | Comma-separated list of qBittorrent instances: `host:port:user:pass[:scheme]` |
| `TOTAL_BANDWIDTH_BPS` | `1000000000` | Your download line speed in bits/sec (1 Gbps default) |
| `TOTAL_UPLOAD_BPS` | *(same as download)* | Your upload line speed in bits/sec — set for asymmetric connections |
| `QBT_HEADROOM_FRACTION` | `0.8` | Fraction of remaining bandwidth to give qbt (download) |
| `QBT_UPLOAD_FRACTION` | `0.9` | Fraction of remaining bandwidth to give qbt (upload) |
| `QBT_SPLIT_BETWEEN_INSTANCES` | `true` | Split bandwidth evenly across instances (set `false` to give each the full amount) |
| `MIN_QBT_DL_BYTES` | `10485760` (10 MB/s) | Minimum download limit — qbt is never throttled below this |
| `MIN_QBT_UL_BYTES` | `5242880` (5 MB/s) | Minimum upload limit |
| `PLEX_OVERHEAD_FACTOR` | `1.25` | Multiplier on stream bitrates to account for buffering |
| `POLL_INTERVAL` | `15` | Seconds between media server session polls |
| `REQUEST_TIMEOUT` | `10` | HTTP request timeout in seconds |
| `PLEX_UNREACHABLE_ACTION` | `keep` | What to do when all media servers are down: `keep` (retain last limits) or `unlimited` |
| `RAMP_UP_STEPS` | `3` | Cycles to ramp from throttled → unlimited when streams stop (0 = instant). Each step doubles the limit |
| `BACKOFF_MAX_INTERVAL` | `300` | Max per-server exponential backoff (seconds) when a media server is unreachable |
| `STATUS_PORT` | `0` (disabled) | Port for the status/metrics HTTP endpoint. Set to e.g. `9101` to enable |
| `LOG_FILE` | `throttle.log` (in script dir) | Log file path |
| `LOG_LEVEL` | `INFO` | Log verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `RACING_WINDOW_ENABLED` | `false` | Enable the racing window feature |
| `RACING_WINDOW_START` | `0` | Racing window start hour (24h, inclusive) |
| `RACING_WINDOW_END` | `7` | Racing window end hour (24h, exclusive) |
| `RACING_INSTANCE_PORT` | `39001` | Port of the racing qBittorrent instance |
| `RACING_NON_RACING_DL_LIMIT` | `1048576` (1 MB/s) | Download cap for non-racing instances during the window |
| `RACING_NON_RACING_UL_LIMIT` | `1048576` (1 MB/s) | Upload cap for non-racing instances during the window |

### Multiple qBittorrent instances

```env
QBT_INSTANCES=localhost:8080:admin:pass1,localhost:8081:admin:pass2
```

By default, calculated bandwidth is split evenly across instances. Set `QBT_SPLIT_BETWEEN_INSTANCES=false` to give each instance the full calculated limit.

Append `:https` to use TLS:

```env
QBT_INSTANCES=myserver:8443:admin:pass1:https
```

### Asymmetric connections

If your upload speed differs from download (common on cable/fibre), set both:

```env
TOTAL_BANDWIDTH_BPS=1000000000   # 1 Gbps down
TOTAL_UPLOAD_BPS=50000000        # 50 Mbps up
```

Upload limits will then be calculated against the upload total separately.

### Bandwidth example

If `TOTAL_BANDWIDTH_BPS=1000000000` (1 Gbps) and your media server is streaming a 20 Mbps 4K file:
- Stream usage = 20 Mbps × 1.25 overhead = 25 Mbps
- Remaining = 975 Mbps
- qbt download limit = 975 × 0.8 = 780 Mbps = ~97.5 MB/s

### Racing window

When `RACING_WINDOW_ENABLED=true`, during the configured hours (default: midnight to 7 AM), the racing instance gets all available bandwidth while other instances are hard-capped at `RACING_NON_RACING_DL_LIMIT` / `RACING_NON_RACING_UL_LIMIT` (default: 1 MB/s each). This prevents long-term media downloads from competing with time-sensitive racing torrents.

The window supports midnight wrapping — e.g. `RACING_WINDOW_START=22` and `RACING_WINDOW_END=6` covers 10 PM to 6 AM.

During the racing window, stream-aware throttling still applies to the racing instance: if the media server is streaming, the racing instance's bandwidth is reduced accordingly. Non-racing instances always stay at the hard cap regardless of stream activity.

```env
RACING_WINDOW_ENABLED=true
RACING_WINDOW_START=0
RACING_WINDOW_END=7
RACING_INSTANCE_PORT=39001
RACING_NON_RACING_DL_LIMIT=1048576   # 1 MB/s
RACING_NON_RACING_UL_LIMIT=1048576   # 1 MB/s
```

### Multi-server support

qbt-flow can poll **multiple media servers simultaneously**. Set URL + token for each server you want to monitor — streams from all configured servers are aggregated into a single bandwidth calculation.

```env
# Both Plex and Jellyfin active — streams are summed
PLEX_URL=http://localhost:32400
PLEX_TOKEN=your-plex-token

JELLYFIN_URL=http://localhost:8096
JELLYFIN_TOKEN=your-jellyfin-api-key
```

Each server has its own exponential backoff tracker — if one goes down, the others continue working. For Emby, the URL is typically `http://host:8920`; qbt-flow adds the `/emby/` prefix automatically.

The legacy `MEDIA_SERVER_TYPE` + `MEDIA_SERVER_URL` + `MEDIA_SERVER_TOKEN` config still works for single-server setups and is mapped to the right `*_URL`/`*_TOKEN` pair automatically.

### Gradual ramp-up

When streams stop, bandwidth limits ramp up gradually instead of jumping straight to unlimited. This avoids a sudden spike that could saturate your connection before qBittorrent's rate limiter adapts.

`RAMP_UP_STEPS` controls how many cycles the ramp takes (default: 3). Each step doubles the limit. Set to `0` for the old instant-unlimited behaviour.

### Exponential backoff

If the media server is unreachable, qbt-flow backs off exponentially (2, 4, 8 … seconds) up to `BACKOFF_MAX_INTERVAL` (default: 300 s). Each configured server has its own backoff tracker, so one server going down doesn’t affect polling of the others. Once a server responds again, its polling resumes at normal speed.

### Status / metrics endpoint

Set `STATUS_PORT` to expose a lightweight HTTP status page:

```env
STATUS_PORT=9101
```

- `GET /` or `GET /status` — JSON snapshot (streams, limits, uptime, configured servers).
- `GET /metrics` — Prometheus-compatible text format.

```bash
curl -s http://localhost:9101/status | python3 -m json.tool
curl -s http://localhost:9101/metrics
```

## Run as a systemd service

Create `/etc/systemd/system/qbt-flow.service`:

```ini
[Unit]
Description=qbt-flow — dynamic qBittorrent bandwidth manager
After=network.target plexmediaserver.service qbittorrent-nox@39000.service qbittorrent-nox@39001.service
Wants=plexmediaserver.service

[Service]
Type=simple
User=smit
WorkingDirectory=/home/smit/qbt-flow
ExecStart=/usr/bin/python3 /home/smit/qbt-flow/qbt_flow.py
Restart=on-failure
RestartSec=30
StandardOutput=null
StandardError=null

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now qbt-flow
sudo systemctl status qbt-flow
journalctl -u qbt-flow -f
```

## Logging

Logs are written to `throttle.log` in the script directory (configurable via `LOG_FILE`). Logs are automatically rotated at 5 MB with 3 backups kept. Set `LOG_LEVEL=DEBUG` for verbose output. Sample output:

```
2026-01-15 14:23:01 INFO qbt_flow starting (poll=5s, DL=1000 Mbps, UL=1000 Mbps, servers=plex+jellyfin)
2026-01-15 14:23:01 INFO [THROTTLE] http://localhost:39000: dl=47.4 MB/s ul=53.3 MB/s (2 stream(s), using ~43 Mbps, remaining DL 946 Mbps / UL 946 Mbps)
2026-01-15 14:23:01 INFO [THROTTLE] http://localhost:39001: dl=47.4 MB/s ul=53.3 MB/s (2 stream(s), using ~43 Mbps, remaining DL 946 Mbps / UL 946 Mbps)
2026-01-15 14:23:06 INFO [RAMP-UP] http://localhost:39000: dl=95.0 MB/s ul=106.6 MB/s (step 3/3)
2026-01-15 14:23:11 INFO [RAMP-UP] http://localhost:39000: dl=190.0 MB/s ul=213.3 MB/s (step 2/3)
2026-01-15 14:23:16 INFO [NORMAL] http://localhost:39000: dl=unlimited ul=unlimited
2026-01-15 14:23:16 INFO [NORMAL] http://localhost:39001: dl=unlimited ul=unlimited
2026-01-15 02:00:01 INFO [RACING] Racing window active (00:00–07:00) — media instance capped
```

## Dry-run mode

Test your configuration without touching qBittorrent:

```bash
python3 qbt_flow.py --dry-run
```

The script will poll your media server and log what limits *would* be applied, prefixed with `[DRY-RUN]`.

## License

MIT
