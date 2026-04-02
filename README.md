# plex-qbt-throttle

Dynamically throttles your qBittorrent download and upload speed limits based on active Plex streams. When Plex is playing, qBittorrent is throttled to prevent buffering. When Plex is idle, limits are removed and qBittorrent runs at full speed.

## How it works

1. Every N seconds (default: 15), the script polls `GET /status/sessions` on your Plex server.
2. It sums the actual stream bitrates reported by Plex (not just the media bitrate — it uses the real transcoded/direct play bandwidth).
3. It subtracts that usage (plus a headroom multiplier) from your total line speed.
4. The remaining bandwidth is divided between download and upload and pushed to all configured qBittorrent instances via the Web API.
5. If no streams are active, all speed limits are cleared (set to unlimited).

## Requirements

- Python 3.8+ (no third-party packages — stdlib only)
- Plex Media Server (local or remote, accessible via HTTP)
- qBittorrent with Web UI enabled

## Install

```bash
git clone https://github.com/smit-p/plex-qbt-throttle.git
cd plex-qbt-throttle
cp config.env.example config.env
$EDITOR config.env   # fill in PLEX_TOKEN, QBT_INSTANCES, etc.
python3 plex_qbt_throttle.py
```

## Configuration

Copy `config.env.example` to `config.env` and edit the values. All settings can also be passed as environment variables.

| Variable | Default | Description |
|---|---|---|
| `PLEX_URL` | `http://localhost:32400` | URL of your Plex server |
| `PLEX_TOKEN` | *(required)* | Your Plex authentication token ([how to find it](https://support.plex.tv/articles/204059436-finding-an-authentication-token-x-plex-token/)) |
| `QBT_INSTANCES` | `localhost:8080:admin:adminadmin` | Comma-separated list of qBittorrent instances: `host:port:user:pass` |
| `TOTAL_BANDWIDTH_BPS` | `1000000000` | Your line speed in bits/sec (1 Gbps default) |
| `QBT_HEADROOM_FRACTION` | `0.8` | Fraction of remaining bandwidth to give qbt (download) |
| `QBT_UPLOAD_FRACTION` | `0.9` | Fraction of remaining bandwidth to give qbt (upload) |
| `MIN_QBT_DL_BYTES` | `10485760` (10 MB/s) | Minimum download limit — qbt is never throttled below this |
| `MIN_QBT_UL_BYTES` | `5242880` (5 MB/s) | Minimum upload limit |
| `PLEX_OVERHEAD_FACTOR` | `1.25` | Multiplier on Plex stream bitrates to account for buffering |
| `POLL_INTERVAL` | `15` | Seconds between Plex session polls |
| `REQUEST_TIMEOUT` | `10` | HTTP request timeout in seconds |
| `LOG_FILE` | `throttle.log` (in script dir) | Log file path |

### Multiple qBittorrent instances

```env
QBT_INSTANCES=localhost:8080:admin:pass1,localhost:8081:admin:pass2
```

Both instances will have their limits adjusted simultaneously.

### Bandwidth example

If `TOTAL_BANDWIDTH_BPS=1000000000` (1 Gbps) and Plex is streaming a 20 Mbps 4K file:
- Plex usage = 20 Mbps × 1.25 overhead = 25 Mbps
- Remaining = 975 Mbps
- qbt download limit = 975 × 0.8 = 780 Mbps = ~97.5 MB/s

## Run as a systemd service

Create `/etc/systemd/system/plex-qbt-throttle.service`:

```ini
[Unit]
Description=Plex → qBittorrent bandwidth throttle
After=network.target

[Service]
Type=simple
User=smit
WorkingDirectory=/home/smit/plex-qbt-throttle
ExecStart=/usr/bin/python3 /home/smit/plex-qbt-throttle/plex_qbt_throttle.py
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now plex-qbt-throttle
sudo systemctl status plex-qbt-throttle
journalctl -u plex-qbt-throttle -f
```

## Logging

Logs are written to `throttle.log` in the script directory (configurable via `LOG_FILE`). Sample output:

```
2025-01-15 14:23:01 [INFO] 2 active stream(s) — Plex using 43.2 Mbps
2025-01-15 14:23:01 [INFO] qbt limits set: DL=119.7 MB/s  UL=134.7 MB/s
2025-01-15 14:23:16 [INFO] No active streams — removing qbt limits
```

## License

MIT
