import os
from dotenv import load_dotenv

load_dotenv(override=True)

# --- Secrets (from .env) ---
PRIVATE_KEY: str = os.environ.get("POLYMARKET_PRIVATE_KEY", "")
WALLET_ADDRESS: str = os.environ.get("POLYMARKET_WALLET_ADDRESS", "")

# --- API ---
CLOB_HOST = "https://clob.polymarket.com"
GAMMA_HOST = "https://gamma-api.polymarket.com"
CHAIN_ID = 137
SIGNATURE_TYPE = 2  # Polymarket browser proxy wallet (Gnosis Safe)

# --- Strategy parameters ---
SPREAD_BUFFER_FRACTION = 0.40
# Place orders at (max_spread * (1 - fraction)) from midpoint.
# 0.40 = use 60% of the reward range as distance, keeping 40% closer to mid.
# Q-score is QUADRATIC: S = ((v-s)/v)^2 * size, where v=max_spread, s=distance.
# At 0.40 buffer: Q = (0.40)^2 = 0.16 (78% more reward than 0.30's 0.09).
# Safe at 0.40 because volatility filter (MAX_VOLATILITY_RATIO=2.0) excludes
# markets where price swings would frequently cross our order zone.
MIN_SPREAD_BUFFER = 0.002
# Floor: never less than 0.2 cents buffer regardless of max_spread.

DRIFT_THRESHOLD_FRACTION = 0.15
# Re-adjust orders when midpoint drifts > this fraction of max_spread.
# Must be < SPREAD_BUFFER_FRACTION to detect drift before leaving range.
# 0.15 vs previous 0.08 = ~50% fewer drift adjustments (less churn).
MIN_DRIFT_THRESHOLD = 0.005
# Floor: never less than 0.5 cents drift threshold (was 0.2 cents).

MAX_MARKETS_CAP = 0
# 0 = no hard cap (dynamic formula decides based on balance).
# Set to a positive number to override with a hard ceiling.
# Note: very high counts (50+) may hit API rate limits on midpoint checks.

COST_PER_MARKET_ESTIMATE = 10.0
# Heuristic for "how many markets can we enter" calculation.
# Orders don't lock collateral — this only drives the market count heuristic.
# At 10, ~$660 balance → ~55 markets. Circuit breaker limits actual fill
# exposure to 1 market at a time, so real capital at risk is just MAX_ENTRY_COST.
# With WS active, per-market API overhead is near zero (price data via WS).

BALANCE_RESERVE_FRACTION = 0.10
# Keep 10% of balance as reserve for drift adjustments and rounding.

MONITOR_INTERVAL_SECONDS = 5
# How often to sync fills and check midpoints for drift.

RESCAN_INTERVAL_SECONDS = 180
# How often to do a full market re-ranking (3 minutes).

ORDER_GRACE_PERIOD_SECONDS = 30
# Newly placed orders may not appear in get_orders() immediately.
# Treat orders younger than this as still alive (prevents phantom fills).

DETAIL_CANDIDATES = 80
# How many pre-filtered candidates to fetch orderbook details for before
# final ranking. Higher = more API calls per scan cycle but more markets
# survive the book-depth filter. Each candidate needs ~4 API calls.

# --- Market filtering thresholds ---
MIN_REWARD_RATE = 0.5       # Minimum daily USDC reward to consider
MAX_BOOK_DEPTH_USDC = 5000  # Skip markets with extremely deep books
MIN_BOOK_DEPTH_USDC = 500   # Skip markets with thin books (illiquid, hard to sell)
MIN_DAILY_VOLUME = 5000     # Skip markets with < $5K 24hr volume (Gamma API)
MIN_MAX_SPREAD = 0.01       # Minimum max_spread (too tight = no room)
MAX_SPREAD_RATIO = 1.5      # Skip markets where current_spread > max_spread * this
# Markets with spreads much wider than max_spread have no active LPs — we'd be the
# sole provider and get adversely selected (filled when price moves against us).
MAX_VOLATILITY_RATIO = 2.0  # Skip markets where 24h_price_range / max_spread > this
# Measures how many times price swings across our order zone per day.
# Data: <= 2.0x captures 25 markets ($3,075/day pool) while excluding all markets
# that caused losses (Elon tweets 13.7x, Bangladesh 8x, Bad Bunny 6x).
MIN_VOLATILITY_DATA_POINTS = 10  # Require at least 10 hourly data points (~10h of history)
# New markets with insufficient data are too risky — can't assess volatility.
MAX_MARKETS_PER_EVENT = 3   # Max markets from the same Gamma event group
# Prevents correlated fills (e.g., 6 Elon tweet buckets filling simultaneously).
# Data: 6 of 11 markets were Elon tweet buckets sharing one event — one trigger fills all.
MIN_MIDPOINT = 0.05         # Skip extreme low-price markets
MAX_MIDPOINT = 0.95         # Skip extreme high-price markets
MIN_HOURS_TO_EXPIRY = 72    # Skip markets expiring within 3 days

# --- Scoring boosts ---
NEG_RISK_SCORE_BOOST = 1.3  # 30% score bonus for neg_risk markets (shared collateral)

# --- Capital allocation ---
TIER_TOP_CUTOFF = 0.3            # Top 30% of ranked markets = top tier
TIER_MID_CUTOFF = 0.7            # Next 40% (30%-70%) = mid tier; rest = base
SIZE_MULTIPLIER_TOP_TIER = 1.0   # Disabled: uniform sizing reduces loss per fill
SIZE_MULTIPLIER_MID_TIER = 1.0   # Disabled: re-enable after confirming profitability

# --- Safety limits ---
MAX_ORDER_SIZE = 500        # Never place a single order larger than this (shares)
MAX_SINGLE_ORDER_USDC = 250 # Never risk more than this on one order (price * size)
MAX_INVENTORY_PER_SIDE = 300  # Max shares to hold per side (Yes/No) per market
MAX_ENTRY_COST = 100.0      # Total per-market cost cap (both sides combined) — 2x for higher rewards
MAX_ORDERS_PER_MARKET = 3   # Hard cap: typically 2 BUY + 1 SELL (or fewer)
STOP_LOSS_FRACTION = 0.6    # Force-sell if loss exceeds 60% of max_spread
MIN_STOP_LOSS = 0.01        # Floor: never less than 1 cent stop-loss
MAX_SELL_RETRIES = 5        # After 5 consecutive SELL failures (~25s), reset phantom inventory

# --- WebSocket settings ---
WS_PING_INTERVAL_SECONDS = 5
# Polymarket requires a "PING" heartbeat. Official Rust client uses 5 seconds.

WS_MAX_RECONNECT_DELAY = 60
# Maximum backoff delay (seconds) between reconnection attempts.
# Backoff sequence: 1, 2, 4, 8, 16, 32, 60, 60, ...

WS_ENABLED = True
# Master switch. Set False to disable WS and fall back to pure REST polling.

FILL_COOLDOWN_SECONDS = 300
# After a SELL fill completes, do NOT re-place BUY for this many seconds.
# Prevents the fill->sell->buy->fill infinite cycle in volatile markets.

MAX_FILLS_BEFORE_BLOCK = 3
# If a side gets filled this many times within FILL_COOLDOWN_SECONDS,
# block re-entry for that side until next full rescan cycle.

REST_FALLBACK_INTERVAL_SECONDS = 30
# How often to do REST-based fill sync as a fallback when WS is connected.
# When WS is disconnected, falls back to MONITOR_INTERVAL_SECONDS (5s).

STARTUP_COOLDOWN_SECONDS = 60
# After bot start/restart, wait this long before placing first BUY orders.
# Prevents immediate fills when cooldown state was lost on restart.
# During this window: WS connects, inventory reconciled, existing positions detected.

# --- Global circuit breaker ---
GLOBAL_CIRCUIT_BREAKER = True
# When ANY BUY order fills, immediately cancel ALL BUY orders across ALL markets.
# Limits worst case to exactly 1 fill at a time (prevents cascade fills).
# After cancelling, enters GLOBAL_FILL_PAUSE_SECONDS cooldown before re-placing.

GLOBAL_FILL_PAUSE_SECONDS = 120
# After a BUY fill triggers the circuit breaker, pause ALL new BUY placements
# globally for this many seconds. During this window: SELL unwind runs,
# no new BUYs placed (drift adjustments skip BUYs, cooldown re-entries blocked).
# After pause expires, BUYs are re-placed at next rescan cycle.

# --- Time-based sizing ---
# Scale order sizes based on US market activity patterns.
# US market hours (high fill risk) = defensive sizing.
# US off-hours (low fill risk) = increased sizing for more rewards.
# Hours are in LOCAL time (system timezone, UTC+8).
PEAK_HOURS_START = 22   # 22:00 local = ~9:00 AM ET (US market opens)
PEAK_HOURS_END = 7      # 07:00 local = ~6:00 PM ET (US after-hours end)
OFF_PEAK_SIZE_MULTIPLIER = 1.0  # 1x uniform sizing (go-wide strategy)
PEAK_SIZE_MULTIPLIER = 1.0      # 1x min_size during peak (baseline, defensive)

# --- Peak hours market cap ---
PEAK_MAX_MARKETS = 12
# During US market hours, cap at 12 markets (reduced exposure).
# Off-peak uses dynamic formula capped at OFF_PEAK_MAX_MARKETS.
# exit_stale_markets() removes excess markets on the next rescan.

OFF_PEAK_MAX_MARKETS = 50
# During off-peak (low fill risk), allow up to 50 markets for max rewards.
# Safe because WS handles price data (no per-market API calls for drift)
# and circuit breaker limits fill exposure to 1 market at a time.

# --- Market blacklist after fill ---
MARKET_BLACKLIST_SECONDS = 7200
# After a BUY fill, blacklist that market for 2 hours before re-entering.
# The market conditions that caused the fill (volatility, adverse flow) likely persist.

# --- Self-healing ---
MAX_CONSECUTIVE_ERRORS = 20
# If the main loop errors this many times in a row, self-terminate.
# The watchdog will restart the bot with fresh state.
# 20 × 0.5s = 10 seconds of continuous errors before exit.
