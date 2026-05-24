ARG REDIS_VERSION=8.0.0
ARG BASE_IMAGE=redis:8.0-alpine

FROM debian:bookworm-slim AS builder

ARG REDIS_VERSION

RUN apt-get update && apt-get install -y \
    build-essential curl libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://github.com/redis/redis/archive/refs/tags/${REDIS_VERSION}.tar.gz" \
    | tar xz -C /tmp

WORKDIR /tmp/redis-${REDIS_VERSION}

# Stamp the version string so the startup log proves this binary is ours.
RUN sed -i 's/REDIS_VERSION "\([^"]*\)"/REDIS_VERSION "\1-russ"/' src/version.h \
    && grep REDIS_VERSION src/version.h

# Patch the tilt trigger to prevent tilt mode under Colima/Docker Desktop on macOS
# where the VM clock can jump by more than 2 s between sentinel timer calls.
# Redis 6.x defines the trigger as a #define (SENTINEL_TILT_TRIGGER, uppercase).
# Redis 7+/8 uses a local variable (sentinel_tilt_trigger, lowercase).
# Raise the threshold to ~23 days and drop the negative-delta check for both forms.
RUN if grep -q 'sentinel_tilt_trigger = 2000;' src/sentinel.c; then \
        echo "Patching Redis 7+/8 tilt trigger (variable form)" && \
        sed -i 's/sentinel_tilt_trigger = 2000;/sentinel_tilt_trigger = 2000000000;/' src/sentinel.c && \
        sed -i 's/delta < 0 || delta > sentinel_tilt_trigger/delta > sentinel_tilt_trigger/' src/sentinel.c; \
    elif grep -q 'define SENTINEL_TILT_TRIGGER 2000' src/sentinel.c; then \
        echo "Patching Redis 6.x tilt trigger (#define form)" && \
        sed -i 's/define SENTINEL_TILT_TRIGGER 2000/define SENTINEL_TILT_TRIGGER 2000000000/' src/sentinel.c && \
        sed -i 's/delta < 0 || delta > SENTINEL_TILT_TRIGGER/delta > SENTINEL_TILT_TRIGGER/' src/sentinel.c; \
    else \
        echo "ERROR: could not find tilt trigger pattern in src/sentinel.c" >&2 && exit 1; \
    fi \
    && grep -n 'TILT_TRIGGER\|tilt_trigger' src/sentinel.c | head -5

RUN make -j$(nproc) MALLOC=libc \
    && cp src/redis-sentinel /redis-sentinel

FROM ${BASE_IMAGE}
COPY --from=builder /redis-sentinel /usr/local/bin/redis-sentinel
