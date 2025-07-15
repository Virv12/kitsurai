#!/usr/bin/env bash
set -e

TABLE="$1"
TIME="$2"
KEY="asdf"

TEMP="$(mktemp)"
printf "%0100d" 0 > "$TEMP"
#echo hello > "$TEMP"

[ -z "$TIME" ] || sleep $(( $(date -d "$TIME" +%s) - $(date +%s) ))
ab -n100000 -c500 -p "$TEMP" http://localhost:8000/$TABLE/$KEY
rm -f "$TEMP"
