#!/usr/bin/env bash
# Per-boot runtime setup: bring up Docker and the MQTT broker the integration
# tests expect on 127.0.0.1:11883. Both steps are idempotent.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bash "$here/docker.sh"
bash "$here/broker.sh"
