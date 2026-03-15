# ── Stage 1: build Python wheels ─────────────────────────────────────────────
FROM python:3.12-alpine AS builder

# coincurve needs a C compiler + libffi
RUN apk add --no-cache gcc musl-dev libffi-dev git

# Clone upstream cost-based source (MIT licence)
WORKDIR /src
RUN git clone --depth 1 https://github.com/citadeldaniel/cost-based .

# Install Python dependencies into a local prefix so we can copy them cleanly
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: lean runtime image ───────────────────────────────────────────────
FROM python:3.12-alpine

# tini = proper PID-1 / signal handling; curl = health-check
RUN apk add --no-cache tini curl

# Copy installed wheels from build stage
COPY --from=builder /install /usr/local

# Copy application source from the cloned upstream repo
WORKDIR /app
COPY --from=builder /src/app.py \
                    /src/database.py \
                    /src/electrs_client.py \
                    /src/mempool_client.py \
                    /src/xpub_derivation.py \
                    ./
COPY --from=builder /src/static    /app/static
COPY --from=builder /src/templates /app/templates

# Replace the StartOS entrypoint with our Umbrel-compatible one
COPY docker_entrypoint.sh /usr/local/bin/docker_entrypoint.sh
RUN chmod +x /usr/local/bin/docker_entrypoint.sh

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD ["/usr/local/bin/docker_entrypoint.sh", "check-web"]

ENTRYPOINT ["/usr/local/bin/docker_entrypoint.sh"]
