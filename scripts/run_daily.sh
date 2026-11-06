#!/bin/bash
#
# Daily crypto Telegram scrape (local, launchd-driven). Replaces the old
# GitHub Actions cron + com.dom.daily-scraper-sync (git pull + rsync --delete) pair,
# which ran ~6h late on GH and so always fed the 08:50 briefing yesterday's file.
# main.py writes the JSON straight into the CoWork crypto-tg-news/ folder.
#   launchd: com.dom.crypto-scraper-daily (daily 08:00 Mac-local time)
set -uo pipefail

REPO="/Users/dom/Coding/daily-scraper"
PY="$REPO/venv/bin/python"
LOCKDIR="/tmp/crypto-scraper.lock.d"
export OUTPUT_DIR="/Users/dom/CoWork/morning-briefing/crypto-tg-news"
# Home timezone is WIB; keep filename dates in WIB even when travelling.
export TZ="Asia/Jakarta"

# single-writer lock (atomic mkdir; no flock on macOS)
if ! mkdir "$LOCKDIR" 2>/dev/null; then
  echo "$(date '+%F %T') another run holds the lock — skipping"; exit 0
fi
trap 'rmdir "$LOCKDIR" 2>/dev/null' EXIT

cd "$REPO" || { echo "repo missing: $REPO"; exit 1; }

# wait for network (laptop may have just woken); give up after ~60s and try anyway
for _ in $(seq 1 30); do ping -c1 -W1 api.telegram.org >/dev/null 2>&1 && break; sleep 2; done

echo "$(date '+%F %T %Z') crypto scrape start"
"$PY" main.py
rc=$?
echo "$(date '+%F %T %Z') crypto scrape exit=$rc -> $OUTPUT_DIR"
exit $rc
