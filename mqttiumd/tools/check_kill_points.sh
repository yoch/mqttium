#!/usr/bin/env bash
# Refuses a build of the workspace in which a manifest turns on the kill
# points of the log store or the SQLite store (feature `kill-points` of
# mqttiumd-store-sqlite, plan docs/missions/L7-kill-inside-a-transaction.md).
# Run from the mqttiumd directory:
#
#   tools/check_kill_points.sh
#
# The feature is only ever turned on on the command line of a test or of
# the bench's child build (`--features mqttiumd-store-sqlite/kill-points`),
# never by a dependency declaration: a declaration would carry it into
# every build of the package that holds it, the product's included. The
# check reads the feature graph of the normal dependencies of every
# package of the workspace, without any feature asked on the command line,
# and fails if `kill-points` is in it. A running store also says which
# build it is: `SqliteStore::effective_settings().kill_points`.
#
# Exit 0 when no manifest turns the feature on, 1 otherwise.
set -euo pipefail

tree=$(cargo tree --workspace --locked -e normal,features --prefix none)
if grep -F 'mqttiumd-store-sqlite feature "kill-points"' <<<"$tree" >/dev/null; then
    echo "kill points: a manifest turns on the feature kill-points of mqttiumd-store-sqlite:"
    cargo tree --workspace --locked -e normal,features -i mqttiumd-store-sqlite \
        --features mqttiumd-store-sqlite/kill-points 2>/dev/null | head -20 || true
    exit 1
fi
if grep -F 'mqttiumd-store-volatile feature "log-write-points"' <<<"$tree" >/dev/null; then
    echo "kill points: a manifest turns on the log store write points"
    exit 1
fi
echo "kill points: no manifest of the workspace turns them on"
