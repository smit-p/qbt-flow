# qbt-flow — single-file, stdlib-only. No build step, no dependencies.
FROM python:3.12-alpine

LABEL org.opencontainers.image.title="qbt-flow" \
      org.opencontainers.image.description="Automatic bandwidth manager for qBittorrent, driven by active Plex/Jellyfin/Emby streams" \
      org.opencontainers.image.source="https://github.com/smit-p/qbt-flow" \
      org.opencontainers.image.licenses="MIT"

WORKDIR /app
COPY qbt_flow.py ./

# Log to stdout only (the container platform captures it) and default the
# status/webhook endpoint on. Everything else is configured via -e env vars.
ENV LOG_FILE="" \
    STATUS_PORT=9101

EXPOSE 9101

# Lightweight healthcheck against the status endpoint (stdlib only).
HEALTHCHECK --interval=60s --timeout=5s --start-period=10s --retries=3 \
    CMD python3 -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:'+os.environ.get('STATUS_PORT','9101')+'/status', timeout=3)" || exit 1

# Run unbuffered so logs appear immediately in `docker logs`.
ENTRYPOINT ["python3", "-u", "qbt_flow.py"]
