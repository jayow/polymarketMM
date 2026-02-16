#!/bin/bash
# Autonomous watchdog for the LP bot (runs indefinitely).
# - Auto-restarts on crash
# - Monitors balance and kills bot if it drops too fast
# - Tracks fill rates and errors
# - Logs all events to watchdog.log

# No set -e: grep returns 1 on no match which would kill the script

BOT_DIR="$(cd "$(dirname "$0")" && pwd)"
BOT_LOG="$BOT_DIR/bot_test.log"
WATCHDOG_LOG="$BOT_DIR/watchdog.log"
DURATION_HOURS=0  # 0 = run forever
CHECK_INTERVAL=60  # seconds between checks
BALANCE_DROP_THRESHOLD=100  # kill bot if balance drops more than $100 from peak (matches 2x order size)
FILL_RATE_THRESHOLD=10     # kill bot if >10 BUY fills in 5 minutes
PEAK_BALANCE_FILE="$BOT_DIR/.watchdog_peak_balance"

cd "$BOT_DIR" || exit 1

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [WATCHDOG] $1" >> "$WATCHDOG_LOG"
}

get_bot_pid() {
    local pid
    pid=$(ps aux 2>/dev/null | grep "[P]ython bot.py" | awk '{print $2}' | head -1)
    echo "$pid"
}

get_balance() {
    local bal
    bal=$(grep "Balance: \\\$" "$BOT_LOG" 2>/dev/null | tail -1 | grep -oE '\$[0-9]+\.[0-9]+' | head -1 | tr -d '$' || echo "")
    echo "$bal"
}

count_recent_buy_fills() {
    # Count BUY fills in last 5 minutes using timestamp comparison
    local now_epoch cutoff_epoch count
    now_epoch=$(date +%s)
    cutoff_epoch=$((now_epoch - 300))

    count=0
    while IFS= read -r line; do
        # Extract timestamp from log line: "2026-02-13 17:00:00,123 ..."
        local ts
        ts=$(echo "$line" | grep -oE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}' || echo "")
        if [ -n "$ts" ]; then
            local line_epoch
            line_epoch=$(date -j -f '%Y-%m-%d %H:%M:%S' "$ts" +%s 2>/dev/null || echo "0")
            if [ "$line_epoch" -ge "$cutoff_epoch" ] 2>/dev/null; then
                count=$((count + 1))
            fi
        fi
    done < <(grep "WS FILL: BUY" "$BOT_LOG" 2>/dev/null || true)

    echo "$count"
}

start_bot() {
    log "Starting bot..."
    # Clear old log to avoid counting stale events
    : > "$BOT_LOG"
    cd "$BOT_DIR" || return
    nohup python3 bot.py >> "$BOT_LOG" 2>&1 &
    local pid=$!
    sleep 5
    if kill -0 "$pid" 2>/dev/null; then
        log "Bot started with PID $pid"
    else
        log "ERROR: Bot failed to start"
    fi
}

kill_bot() {
    local reason="${1:-unknown}"
    local pid
    pid=$(get_bot_pid)
    if [ -n "$pid" ]; then
        log "KILLING BOT (PID $pid): $reason"
        kill "$pid" 2>/dev/null || true
        sleep 5
        # Force kill if still alive
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null || true
            log "Force-killed PID $pid"
        fi
        sleep 2
    fi
}

emergency_cancel_all() {
    log "EMERGENCY: Cancelling all orders on exchange..."
    cd "$BOT_DIR" || return
    python3 -c "
import os, sys
sys.path.insert(0, '.')
from dotenv import load_dotenv
load_dotenv(override=True)
from py_clob_client.client import ClobClient
import config
client = ClobClient(
    host=config.CLOB_HOST,
    key=config.PRIVATE_KEY,
    chain_id=config.CHAIN_ID,
    signature_type=config.SIGNATURE_TYPE,
    funder=config.WALLET_ADDRESS,
)
creds = client.create_or_derive_api_creds()
client.set_api_creds(creds)
client.cancel_all()
print('All orders cancelled')
" >> "$WATCHDOG_LOG" 2>&1 || log "Failed to cancel all orders"
}

# --- MAIN ---

START_EPOCH=$(date +%s)
log "=== WATCHDOG STARTED (indefinite) ==="
log "Balance drop threshold: \$${BALANCE_DROP_THRESHOLD}"
log "Fill rate threshold: ${FILL_RATE_THRESHOLD} BUY fills / 5min"

# Kill any existing bot and start fresh
kill_bot "fresh start"
start_bot

# Wait for first balance log
sleep 30

# Record starting balance
CURRENT_BAL=$(get_balance)
START_BALANCE="${CURRENT_BAL:-0}"

# Restore peak from file if it exists (survives watchdog restarts)
if [ -f "$PEAK_BALANCE_FILE" ]; then
    SAVED_PEAK=$(cat "$PEAK_BALANCE_FILE" 2>/dev/null || echo "0")
    # Use the higher of saved peak and current balance
    HIGHER=$(echo "$SAVED_PEAK > $START_BALANCE" | bc -l 2>/dev/null || echo "0")
    if [ "$HIGHER" = "1" ]; then
        PEAK_BALANCE="$SAVED_PEAK"
        log "Restored peak balance from file: \$${PEAK_BALANCE}"
    else
        PEAK_BALANCE="$START_BALANCE"
    fi
else
    PEAK_BALANCE="$START_BALANCE"
fi

if [ -z "$PEAK_BALANCE" ] || [ "$PEAK_BALANCE" = "" ]; then
    PEAK_BALANCE="0"
fi
log "Starting balance: \$${START_BALANCE}, peak: \$${PEAK_BALANCE}"

CHECK_NUM=0
RESTARTS=0
EMERGENCY_STOPS=0

while true; do
    sleep "$CHECK_INTERVAL"
    CHECK_NUM=$((CHECK_NUM + 1))

    # --- Check 1: Is bot alive? ---
    BOT_PID=$(get_bot_pid)
    if [ -z "$BOT_PID" ]; then
        RESTARTS=$((RESTARTS + 1))
        log "CHECK #${CHECK_NUM}: Bot NOT running! Restarting... (restart #${RESTARTS})"
        if [ "$RESTARTS" -gt 10 ]; then
            log "FATAL: Too many restarts (${RESTARTS}). Stopping watchdog."
            emergency_cancel_all
            break
        fi
        start_bot
        sleep 30
        continue
    fi

    # --- Check 1.5: Is bot stuck? (log not updated in 5 min) ---
    if [ -n "$BOT_PID" ]; then
        LAST_MOD=$(stat -f %m "$BOT_LOG" 2>/dev/null || echo "0")
        NOW_EPOCH=$(date +%s)
        STALE_SECONDS=$((NOW_EPOCH - LAST_MOD))
        if [ "$STALE_SECONDS" -gt 300 ]; then
            log "Bot log stale for ${STALE_SECONDS}s — bot may be stuck. Restarting."
            kill_bot "stuck (log stale ${STALE_SECONDS}s)"
            RESTARTS=$((RESTARTS + 1))
            start_bot
            sleep 30
            continue
        fi
    fi

    # --- Check 2: Balance protection ---
    CURRENT_BALANCE=$(get_balance)
    if [ -n "$CURRENT_BALANCE" ] && [ "$CURRENT_BALANCE" != "0" ] && [ "$CURRENT_BALANCE" != "" ]; then
        # Update peak
        PEAK_CMP=$(echo "$CURRENT_BALANCE > $PEAK_BALANCE" | bc -l 2>/dev/null || echo "0")
        if [ "$PEAK_CMP" = "1" ]; then
            PEAK_BALANCE="$CURRENT_BALANCE"
            echo "$PEAK_BALANCE" > "$PEAK_BALANCE_FILE"
        fi

        # Check for significant drop from peak
        DROP=$(echo "$PEAK_BALANCE - $CURRENT_BALANCE" | bc -l 2>/dev/null || echo "0")
        DROP_CMP=$(echo "$DROP > $BALANCE_DROP_THRESHOLD" | bc -l 2>/dev/null || echo "0")
        if [ "$DROP_CMP" = "1" ]; then
            EMERGENCY_STOPS=$((EMERGENCY_STOPS + 1))
            log "EMERGENCY: Balance dropped \$${DROP} from peak \$${PEAK_BALANCE} -> \$${CURRENT_BALANCE}"
            kill_bot "balance drop exceeds threshold"
            emergency_cancel_all
            sleep 60
            # Keep peak balance (persistent) — prevents slow-bleed resets
            log "Restarting (peak remains \$${PEAK_BALANCE})"
            start_bot
            sleep 30
            continue
        fi
    fi

    # --- Check 3: Fill rate guard ---
    RECENT_FILLS=$(count_recent_buy_fills)
    RECENT_FILLS=${RECENT_FILLS:-0}
    if [ "$RECENT_FILLS" -gt "$FILL_RATE_THRESHOLD" ] 2>/dev/null; then
        EMERGENCY_STOPS=$((EMERGENCY_STOPS + 1))
        log "EMERGENCY: ${RECENT_FILLS} BUY fills in 5min (threshold: ${FILL_RATE_THRESHOLD})"
        kill_bot "excessive fill rate"
        emergency_cancel_all
        sleep 300  # Wait 5 minutes before restart
        start_bot
        sleep 30
        continue
    fi

    # --- Check 3.5: Error rate guard ---
    ERROR_CUTOFF=$(($(date +%s) - 300))
    RECENT_ERRORS=0
    while IFS= read -r line; do
        ts=$(echo "$line" | grep -oE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}' || echo "")
        if [ -n "$ts" ]; then
            le=$(date -j -f '%Y-%m-%d %H:%M:%S' "$ts" +%s 2>/dev/null || echo "0")
            if [ "$le" -ge "$ERROR_CUTOFF" ] 2>/dev/null; then
                RECENT_ERRORS=$((RECENT_ERRORS + 1))
            fi
        fi
    done < <(grep "Error in main loop" "$BOT_LOG" 2>/dev/null || true)
    if [ "${RECENT_ERRORS:-0}" -gt 50 ]; then
        log "WARNING: ${RECENT_ERRORS} main loop errors in 5min — restarting bot"
        kill_bot "high error rate (${RECENT_ERRORS} errors/5min)"
        start_bot
        sleep 30
        continue
    fi

    # --- Check 4: Gather stats ---
    ERROR_COUNT=$(grep -c "ERROR:" "$BOT_LOG" 2>/dev/null || echo "0")
    FILL_COUNT=$(grep -c "WS FILL:" "$BOT_LOG" 2>/dev/null || echo "0")
    BUY_FILL_COUNT=$(grep -c "WS FILL: BUY" "$BOT_LOG" 2>/dev/null || echo "0")
    SELL_FILL_COUNT=$(grep -c "WS FILL: SELL" "$BOT_LOG" 2>/dev/null || echo "0")
    DRIFT_COUNT=$(grep -c "drift" "$BOT_LOG" 2>/dev/null || echo "0")
    INV_COUNT=$(grep "with inventory" "$BOT_LOG" 2>/dev/null | tail -1 | grep -oE '[0-9]+ with inventory' | cut -d' ' -f1 || echo "0")
    ACTIVE_COUNT=$(grep "Active:" "$BOT_LOG" 2>/dev/null | tail -1 | grep -oE 'Active: [0-9]+' | cut -d' ' -f2 || echo "0")
    PHANTOM_COUNT=$(grep -c "phantom" "$BOT_LOG" 2>/dev/null || echo "0")
    COOLDOWN_SKIP=$(grep -c "cooldown" "$BOT_LOG" 2>/dev/null || echo "0")

    # --- Check 5: WS connection health ---
    WS_STATUS=$(grep "WS:" "$BOT_LOG" 2>/dev/null | tail -1 | grep -oE 'WS: [^ |]+' || echo "WS: ?")

    # --- Periodic log (every 5 minutes = every 5 checks) ---
    if [ $((CHECK_NUM % 5)) -eq 0 ]; then
        RUNTIME_MIN=$(( ($(date +%s) - START_EPOCH) / 60 ))
        log "STATUS #${CHECK_NUM} (${RUNTIME_MIN}min): bal=\$${CURRENT_BALANCE:-?} peak=\$${PEAK_BALANCE} | active=${ACTIVE_COUNT} inv=${INV_COUNT} | buy_fills=${BUY_FILL_COUNT} sell_fills=${SELL_FILL_COUNT} errors=${ERROR_COUNT} drifts=${DRIFT_COUNT} phantoms=${PHANTOM_COUNT} | restarts=${RESTARTS} emergencies=${EMERGENCY_STOPS} | ${WS_STATUS}"
    fi
done

log "=== WATCHDOG ENDED (${RESTARTS} restarts, ${EMERGENCY_STOPS} emergencies) ==="
log "Final balance: \$$(get_balance) | Start: \$${START_BALANCE} | Peak: \$${PEAK_BALANCE}"

# Clean shutdown (only reached if too many restarts triggered break)
kill_bot "watchdog stopped"
