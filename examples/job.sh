#!/bin/sh
# Example job handler for dosyaisler.
# argv: $1 = job id, $2 = payload file (JSON, deleted by the worker afterwards).
# Fails (exit 2) when the payload contains "fail": true.
id="$1"
pf="$2"
echo "handling $id"
if grep -q '"fail":true' "$pf"; then
    echo "simulated failure for $id" >&2
    exit 2
fi
exit 0
