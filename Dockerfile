FROM python:3.12-slim AS runtime

# Fixed uid/gid so a bind-mounted store can be made writable deterministically:
#   chown -R 10002:10002 ./hub-data
RUN groupadd -r -g 10002 hub && useradd -r -u 10002 -g hub hub \
    && mkdir -p /data && chown hub:hub /data

WORKDIR /app
COPY hub ./hub
COPY hubread ./hubread
COPY datasets ./datasets
RUN chown -R hub:hub /app

USER hub
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
VOLUME /data

# Liveness, not "the process exists": a hub that has hung still has a running process, but its
# heartbeat stops advancing, and `health` exits non-zero once the file is older than the policy.
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
  CMD ["python", "-m", "hub", "--root", "/data", "--datasets", "/app/datasets", "health"]

ENTRYPOINT ["python", "-m", "hub", "--root", "/data", "--datasets", "/app/datasets"]
CMD ["run"]
