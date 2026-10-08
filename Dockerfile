# One libc family, start to finish. The previous build installed wheels in
# python:3.12-slim (glibc) and copied site-packages into python:3.12-alpine
# (musl); aiohttp and its dependencies ship compiled extensions, and a
# manylinux wheel is not loadable under musl. It appeared to work only because
# some packages have pure-Python fallbacks (audit O01).
FROM python:3.12-slim AS builder

WORKDIR /app

COPY requirements.txt ./
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    MALLOC_ARENA_MAX=2 \
    PATH="/opt/venv/bin:$PATH"

# Unprivileged, with a fixed UID so the bind-mounted state/ and logs/ can be
# chowned to it on the host.
RUN groupadd --gid 10001 bridge \
    && useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin bridge

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv

# An allowlist, not `COPY . .`: the image contents should not depend on what
# happens to be lying around in the working directory (audit O07).
COPY --chown=bridge:bridge app/ ./app/

RUN mkdir -p /app/state /app/logs /app/state/spool \
    && chown -R bridge:bridge /app

USER bridge

# Readiness, not liveness: reports healthy only once MAX has authorised us and
# the journal is reachable. Compose does not act on this by itself — see the
# note in docker-compose.yml.
HEALTHCHECK --interval=60s --timeout=10s --start-period=45s --retries=3 \
    CMD ["python", "-m", "app.healthcheck"]

CMD ["python", "-m", "app.main"]
