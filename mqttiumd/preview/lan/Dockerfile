FROM ubuntu:24.04

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates mosquitto-clients python3-venv \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/lan-requirements.txt
RUN python3 -m venv /opt/lan \
    && /opt/lan/bin/python -m pip install --no-cache-dir -r /tmp/lan-requirements.txt
ENV PYTHONDONTWRITEBYTECODE=1
ENTRYPOINT ["/opt/lan/bin/python", "/role/lan_restart.py"]
