#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG="$SCRIPT_DIR/bot_test.log"
MONITOR_LOG="$SCRIPT_DIR/monitor_report.log"
END_TIME=$(($(date +%s) + 7200))  # 2 hours from now

echo "=== MONITOR STARTED $(date) ===" > "$MONITOR_LOG"
echo "Will check every 15 min for 2 hours" >> "$MONITOR_LOG"
echo "" >> "$MONITOR_LOG"

check_num=0
while [ $(date +%s) -lt $END_TIME ]; do
    check_num=$((check_num + 1))
    echo "========== CHECK #${check_num} at $(date '+%Y-%m-%d %H:%M:%S') ==========" >> "$MONITOR_LOG"
    
    # Check if bot process is alive (macOS uses capital-P Python binary)
    BOT_PID=$(ps aux | grep "[P]ython bot.py" | awk '{print $2}')
    if [ -n "$BOT_PID" ]; then
        echo "[OK] Bot process alive (PID $BOT_PID)" >> "$MONITOR_LOG"
    else
        echo "[CRITICAL] Bot process NOT running!" >> "$MONITOR_LOG"
    fi
    
    # Get latest status line
    grep "Active:" "$LOG" | tail -1 >> "$MONITOR_LOG"
    
    # Count errors since last check (last 15 min of log)
    errors=$(grep -c "ERROR" "$LOG" 2>/dev/null || echo 0)
    warns=$(grep -c "WARNING" "$LOG" 2>/dev/null || echo 0)
    fills=$(grep -c "WS FILL:" "$LOG" 2>/dev/null || echo 0)
    drifts=$(grep -c "drift" "$LOG" 2>/dev/null || echo 0)
    reconnects=$(grep -c "Reconnecting" "$LOG" 2>/dev/null || echo 0)
    stop_losses=$(grep -c "STOP-LOSS" "$LOG" 2>/dev/null || echo 0)
    phantom=$(grep -c "phantom" "$LOG" 2>/dev/null || echo 0)
    
    echo "  Totals: errors=$errors warns=$warns fills=$fills drifts=$drifts reconnects=$reconnects stop_losses=$stop_losses phantom=$phantom" >> "$MONITOR_LOG"
    
    # Show any new errors or fills since last check
    recent_errors=$(tail -500 "$LOG" | grep -E "ERROR|STOP-LOSS|phantom|not enough balance|CRITICAL" | tail -5)
    if [ -n "$recent_errors" ]; then
        echo "  Recent issues:" >> "$MONITOR_LOG"
        echo "$recent_errors" >> "$MONITOR_LOG"
    fi
    
    recent_fills=$(tail -500 "$LOG" | grep "WS FILL:" | tail -5)
    if [ -n "$recent_fills" ]; then
        echo "  Recent fills:" >> "$MONITOR_LOG"
        echo "$recent_fills" >> "$MONITOR_LOG"
    fi
    
    echo "" >> "$MONITOR_LOG"
    
    sleep 900  # 15 minutes
done

echo "=== MONITOR ENDED $(date) ===" >> "$MONITOR_LOG"
