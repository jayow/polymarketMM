import atexit
import os
import signal
import subprocess
import sys
import time

import requests
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import BalanceAllowanceParams, AssetType

import config
from market_scanner import MarketScanner, MarketOpportunity
from order_manager import OrderManager
from price_monitor import PriceMonitor
from utils import setup_logger, safe_api_call, round_price_up, round_price_down, clamp_price, get_size_multiplier, is_peak_hours
from ws_monitor import WSMonitor, TradeEvent, OrderEvent

logger = setup_logger("LPRewardsBot")

LOCK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".bot.lock")


class LPRewardsBot:
    def __init__(self):
        self.running = False
        self.client: ClobClient = None
        self.scanner: MarketScanner = None
        self.order_manager: OrderManager = None
        self.price_monitor: PriceMonitor = None
        self.ws_monitor: WSMonitor = None
        self.active_opportunities: dict[str, MarketOpportunity] = {}
        self._recovered_token_ids: set[str] = set()  # token_ids with pending recovery SELLs
        self._recovered_order_ids: set[str] = set()  # order IDs from recovery SELLs
        # Maps token_id -> (size, order_id, sell_price) for adoption into OrderManager
        self._recovery_info: dict[str, tuple[float, str | None, float]] = {}
        self._last_rest_sync: float = 0.0  # for REST fallback pacing
        self._start_time: float = 0.0  # set in run(), for startup cooldown

    def initialize(self):
        """Initialize CLOB client and all components."""
        if not config.PRIVATE_KEY or not config.WALLET_ADDRESS:
            logger.error(
                "Missing POLYMARKET_PRIVATE_KEY or POLYMARKET_WALLET_ADDRESS in .env"
            )
            sys.exit(1)

        logger.info("Initializing CLOB client...")
        self.client = ClobClient(
            host=config.CLOB_HOST,
            key=config.PRIVATE_KEY,
            chain_id=config.CHAIN_ID,
            signature_type=config.SIGNATURE_TYPE,
            funder=config.WALLET_ADDRESS,
        )

        # Create or derive API credentials
        creds = self.client.create_or_derive_api_creds()
        self.client.set_api_creds(creds)
        logger.info("API credentials set")

        self.scanner = MarketScanner(self.client)
        self.order_manager = OrderManager(self.client)
        self.price_monitor = PriceMonitor(self.client, self.order_manager)

        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Cancel any stale orders from previous session (retry until clean)
        self._cancel_all_with_retry()
        try:
            params = BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                signature_type=config.SIGNATURE_TYPE,
            )
            bal = self.client.get_balance_allowance(params)
            usdc = int(bal.get("balance", "0")) / 1_000_000
            logger.info(f"USDC balance: ${usdc:.2f}")
        except Exception:
            pass

        # Detect existing share positions from previous runs
        self._recover_existing_positions()

        # Initialize WebSocket monitor
        if config.WS_ENABLED:
            self.ws_monitor = WSMonitor(
                api_key=self.client.creds.api_key,
                api_secret=self.client.creds.api_secret,
                api_passphrase=self.client.creds.api_passphrase,
            )
            logger.info("WebSocket monitor initialized (will start in run())")

        logger.info("Bot initialized successfully")

    def _cancel_all_with_retry(self, max_attempts: int = 20):
        """Cancel all orders on exchange, retrying until get_orders returns empty."""
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            try:
                self.client.cancel_all()
            except Exception as e:
                logger.warning(f"cancel_all attempt {attempt} failed: {e}")
                if "Credentials" in str(e):
                    logger.error("API credentials not set — cannot cancel orders")
                    return

            time.sleep(3)

            try:
                open_orders = safe_api_call(self.client.get_orders)
                count = len(open_orders) if isinstance(open_orders, list) else 0
                if count == 0:
                    logger.info(f"Exchange clean after {attempt} cancel_all call(s)")
                    return
                logger.warning(f"Still {count} orders on exchange after attempt {attempt}")
            except Exception as e:
                logger.warning(f"Failed to check orders after cancel: {e}")
                if "Credentials" in str(e):
                    logger.error("API credentials not set — cannot verify orders")
                    return

        logger.error(f"Failed to cancel all orders after {max_attempts} attempts")

    def _place_recovery_sell(self, token_id: str, size: float):
        """Place a SELL order for orphaned/recovered shares. Track for orphan cleanup."""
        from py_clob_client.clob_types import (
            OrderArgs, MarketOrderArgs, OrderType,
            BalanceAllowanceParams, AssetType,
        )
        try:
            mid_resp = safe_api_call(self.client.get_midpoint, token_id)
            mid = float(mid_resp.get("mid", 0) if isinstance(mid_resp, dict) else mid_resp)
            tick = float(safe_api_call(self.client.get_tick_size, token_id))

            self.client.update_balance_allowance(BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
                signature_type=config.SIGNATURE_TYPE,
            ))

            sell_price = round_price_down(mid, tick)
            sell_price = clamp_price(sell_price, tick)

            book = safe_api_call(self.client.get_order_book, token_id)
            min_size = float(getattr(book, "min_order_size", 0) or 0)

            if min_size > 0 and size < min_size:
                logger.info(f"  Using FOK market SELL ({size} < min_size {min_size})")
                args = MarketOrderArgs(
                    token_id=token_id, amount=size, side="SELL", price=sell_price,
                )
                signed = self.client.create_market_order(args)
                resp = safe_api_call(self.client.post_order, signed, OrderType.FOK)
            else:
                args = OrderArgs(price=sell_price, size=size, side="SELL", token_id=token_id)
                signed = self.client.create_order(args)
                resp = safe_api_call(self.client.post_order, signed, OrderType.GTC)

            oid = None
            if isinstance(resp, dict):
                oid = resp.get("orderID") or resp.get("orderId")
                if oid:
                    self._recovered_order_ids.add(oid)

            self._recovered_token_ids.add(token_id)
            self._recovery_info[token_id] = (size, oid, sell_price)
            logger.info(f"  Recovery SELL @{sell_price:.4f} x{size} for {token_id[:16]}...")
        except Exception as e:
            err_str = str(e)
            if "404" in err_str or "No orderbook" in err_str:
                logger.warning(
                    f"  Market dead for {token_id[:16]}... (no orderbook) — "
                    f"writing off {size} shares as unrecoverable"
                )
                # Don't add to _recovered_token_ids — nothing to track
                return
            logger.warning(f"  Failed to place recovery SELL for {token_id[:16]}...: {e}")
            self._recovered_token_ids.add(token_id)
            self._recovery_info[token_id] = (size, None, 0.0)

    def _recover_existing_positions(self):
        """Place SELL orders for shares held from previous runs and block re-entry."""
        try:
            r = requests.get(
                f"https://data-api.polymarket.com/positions"
                f"?user={config.WALLET_ADDRESS.lower()}",
                timeout=10,
            )
            positions = r.json()
        except Exception as e:
            logger.warning(f"Failed to fetch existing positions: {e}")
            return

        for pos in positions:
            size = float(pos.get("size", 0))
            if size <= 0:
                continue

            title = pos.get("title", "Unknown")[:50]
            outcome = pos.get("outcome", "?")
            token_id = pos.get("asset", "")

            if not token_id:
                continue

            self._recovered_token_ids.add(token_id)
            logger.info(f"Found existing position: {title} [{outcome}] = {size} shares (blocking re-entry)")
            self._place_recovery_sell(token_id, size)

    def _force_adopt_recovery_positions(self):
        """Adopt ALL recovery positions into OrderManager immediately.

        Fetches market data from sampling API (or Gamma fallback) so recovery
        positions get WS tracking and SELL repricing without waiting for the
        scanner to include them. This ensures positions like Claude 5 get
        active best_ask undercutting from the very start.
        """
        if not self._recovery_info:
            return

        from order_manager import MarketPosition, ActiveOrder, SELL
        import json as _json

        logger.info(
            f"Force-adopting {len(self._recovery_info)} recovery position(s) "
            f"into OrderManager..."
        )

        # Build token_id -> sampling market lookup (one paginated fetch)
        sampling_by_token: dict[str, dict] = {}
        try:
            raw_markets = self.scanner.fetch_all_sampling_markets()
            for m in raw_markets:
                for t in m.get("tokens", []):
                    tid = t.get("token_id", "")
                    if tid:
                        sampling_by_token[tid] = m
        except Exception as e:
            logger.warning(f"Failed to fetch sampling markets for adoption: {e}")

        for token_id in list(self._recovery_info.keys()):
            info = self._recovery_info.get(token_id)
            if not info:
                continue
            size, order_id, sell_price = info

            # --- Look up market data ---
            condition_id = ""
            token_yes = ""
            token_no = ""
            max_spread = 0.03  # default 3 cents
            min_size = 50.0
            question = ""

            sampling = sampling_by_token.get(token_id)
            if sampling:
                condition_id = sampling.get("condition_id", "")
                question = sampling.get("question", "")[:50]
                rewards = sampling.get("rewards") or {}
                max_spread_cents = float(rewards.get("max_spread", 3.0))
                max_spread = max_spread_cents / 100.0
                min_size = float(rewards.get("min_size", 50))

                for t in sampling.get("tokens", []):
                    outcome = t.get("outcome", "").lower()
                    if outcome == "yes":
                        token_yes = t.get("token_id", "")
                    elif outcome == "no":
                        token_no = t.get("token_id", "")

                if not token_yes or not token_no:
                    tokens = sampling.get("tokens", [])
                    if len(tokens) >= 2:
                        token_yes = tokens[0].get("token_id", "")
                        token_no = tokens[1].get("token_id", "")

            if not condition_id or not token_yes or not token_no:
                # Fallback: query Gamma API by token ID
                try:
                    resp = requests.get(
                        f"{config.GAMMA_HOST}/markets",
                        params={"clob_token_ids": token_id},
                        timeout=10,
                    )
                    markets = resp.json()
                    if markets:
                        market = markets[0]
                        condition_id = market.get("conditionId", "")
                        question = (market.get("question", "") or "")[:50]
                        clob_ids = market.get("clobTokenIds") or []
                        if isinstance(clob_ids, str):
                            clob_ids = _json.loads(clob_ids)
                        if len(clob_ids) >= 2:
                            token_yes = clob_ids[0]
                            token_no = clob_ids[1]
                except Exception as e:
                    logger.warning(
                        f"Gamma lookup failed for {token_id[:16]}...: {e}"
                    )

            if not condition_id or not token_yes or not token_no:
                logger.warning(
                    f"Cannot adopt {token_id[:16]}... — missing market data"
                )
                continue

            # Skip if already in OrderManager
            if condition_id in self.order_manager.positions:
                self._recovered_token_ids.discard(token_yes)
                self._recovered_token_ids.discard(token_no)
                if order_id:
                    self._recovered_order_ids.discard(order_id)
                self._recovery_info.pop(token_id, None)
                continue

            # Get tick_size
            try:
                tick_size = float(
                    safe_api_call(self.client.get_tick_size, token_yes)
                )
            except Exception:
                tick_size = 0.01

            # Get current midpoint
            mid = self.price_monitor.get_current_midpoint(token_yes)
            if mid is None:
                mid = sell_price if sell_price > 0 else 0.5

            # Determine which side has recovery shares
            is_yes = (token_id == token_yes)

            # Create the position
            position = MarketPosition(
                condition_id=condition_id,
                token_id_yes=token_yes,
                token_id_no=token_no,
                max_spread=max_spread,
                min_size=min_size,
                tick_size=tick_size,
                last_midpoint=mid,
            )

            if is_yes:
                position.yes_inventory = size
                position.yes_entry_price = sell_price
            else:
                position.no_inventory = size
                position.no_entry_price = sell_price

            # Track the existing SELL order
            if order_id and sell_price > 0:
                tracked_order = ActiveOrder(
                    order_id=order_id,
                    token_id=token_id,
                    side=SELL,
                    price=sell_price,
                    size=size,
                    condition_id=condition_id,
                    placed_at=time.time(),
                    midpoint_at_placement=mid,
                )
                position.orders.append(tracked_order)

            self.order_manager.positions[condition_id] = position

            # Subscribe WS
            if self.ws_monitor:
                self.ws_monitor.subscribe_market({token_yes, token_no})
                self.ws_monitor.subscribe_user({condition_id})

            # Clean up recovery tracking
            self._recovered_token_ids.discard(token_yes)
            self._recovered_token_ids.discard(token_no)
            if order_id:
                self._recovered_order_ids.discard(order_id)
            self._recovery_info.pop(token_id, None)

            label = "YES" if is_yes else "NO"
            logger.info(
                f"Force-adopted recovery {label}: {question} | "
                f"inv={size} SELL@{sell_price:.4f} mid={mid:.4f} "
                f"spread={max_spread:.4f} | WS tracking active"
            )

        remaining = len(self._recovery_info)
        if remaining > 0:
            logger.warning(
                f"{remaining} recovery position(s) could not be adopted "
                f"(will retry via scanner)"
            )

    def _reconcile_inventory(self):
        """Two-way reconciliation between local tracking and exchange reality.

        1. Reset phantom inventory: tracked locally but not on exchange.
        2. Detect missed fills: shares on exchange with no pending SELL — update
           inventory and place SELL unwind orders.
        """
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType

        try:
            r = requests.get(
                f"https://data-api.polymarket.com/positions"
                f"?user={config.WALLET_ADDRESS.lower()}",
                timeout=10,
            )
            exchange_positions = r.json()
        except Exception as e:
            logger.warning(f"Reconciliation: failed to fetch positions: {e}")
            return

        # Build lookup: token_id -> actual share count on exchange
        actual_shares: dict[str, float] = {}
        for pos in exchange_positions:
            token_id = pos.get("asset", "")
            size = float(pos.get("size", 0))
            if token_id and size > 0:
                actual_shares[token_id] = size

        # Build lookup: token_id -> position that tracks it
        token_to_position: dict[str, tuple[str, bool]] = {}
        for cid, position in self.order_manager.positions.items():
            token_to_position[position.token_id_yes] = (cid, True)
            token_to_position[position.token_id_no] = (cid, False)

        # Also include recovered token_ids (these have standalone SELLs)
        # and phantom tokens (confirmed no balance — block re-creation)
        recovered_tokens = set(self._recovered_token_ids)
        phantom_tokens = self.order_manager._phantom_tokens

        # --- Direction 1: Reset phantom inventory (tracked > 0 but exchange = 0) ---
        for cid, position in self.order_manager.positions.items():
            for is_yes, token_id, inv_attr in [
                (True, position.token_id_yes, "yes_inventory"),
                (False, position.token_id_no, "no_inventory"),
            ]:
                tracked = getattr(position, inv_attr)
                actual = actual_shares.get(token_id, 0)

                if tracked > 0 and actual == 0:
                    label = "YES" if is_yes else "NO"
                    logger.warning(
                        f"Reconciliation: {cid[:16]}... {label} inventory "
                        f"tracked={tracked} but exchange=0 — resetting"
                    )
                    setattr(position, inv_attr, 0)
                    # Clear retry counter so future legitimate fills get fresh attempts
                    self.order_manager._sell_fail_counts.pop((cid, token_id), None)
                elif tracked > 0 and actual > 0 and abs(tracked - actual) > 0.5:
                    # Inventory mismatch: we missed a partial fill (e.g. SELL
                    # filled during a reprice race). Update to match reality.
                    label = "YES" if is_yes else "NO"
                    logger.warning(
                        f"Reconciliation: {cid[:16]}... {label} inventory "
                        f"tracked={tracked:.2f} but exchange={actual:.2f} — correcting"
                    )
                    setattr(position, inv_attr, actual)
                    self.order_manager._sell_fail_counts.pop((cid, token_id), None)

        # --- Direction 2: Detect untracked shares and place SELL orders ---
        for token_id, actual_size in actual_shares.items():
            # Skip tokens being recovered at startup (they already have SELL orders)
            # or tokens confirmed to have phantom inventory (stale data API)
            if token_id in recovered_tokens or token_id in phantom_tokens:
                continue

            info = token_to_position.get(token_id)
            if info:
                cid, is_yes = info
                position = self.order_manager.positions.get(cid)
                if not position:
                    continue

                inv_attr = "yes_inventory" if is_yes else "no_inventory"
                tracked_inv = getattr(position, inv_attr)

                # Check if a SELL is already pending for this side
                has_sell = any(
                    o.side == "SELL" and o.token_id == token_id
                    for o in position.orders
                )

                if tracked_inv == 0 and not has_sell:
                    # Exchange has shares we don't know about — missed fill
                    label = "YES" if is_yes else "NO"
                    logger.warning(
                        f"Reconciliation: {cid[:16]}... {label} has {actual_size} "
                        f"shares on exchange but inv=0 and no SELL — placing SELL"
                    )
                    setattr(position, inv_attr, actual_size)
                    mid = self.order_manager._get_current_midpoint(position.token_id_yes)
                    if mid is None:
                        mid = position.last_midpoint
                    sell_price = self.order_manager._calculate_sell_price(
                        mid, position.max_spread, position.tick_size, is_yes
                    )
                    sell_order = self.order_manager._place_order(
                        token_id, sell_price, actual_size,
                        "SELL", cid, mid,
                        min_order_size=position.min_size,
                    )
                    if sell_order:
                        self.order_manager._track_order(position, sell_order)

                elif tracked_inv > 0 and not has_sell:
                    # We know about inventory but SELL is missing — place one
                    label = "YES" if is_yes else "NO"
                    logger.warning(
                        f"Reconciliation: {cid[:16]}... {label} inv={tracked_inv} "
                        f"but no SELL pending — placing SELL"
                    )
                    mid = self.order_manager._get_current_midpoint(position.token_id_yes)
                    if mid is None:
                        mid = position.last_midpoint
                    sell_price = self.order_manager._calculate_sell_price(
                        mid, position.max_spread, position.tick_size, is_yes
                    )
                    sell_order = self.order_manager._place_order(
                        token_id, sell_price, tracked_inv,
                        "SELL", cid, mid,
                        min_order_size=position.min_size,
                    )
                    if sell_order:
                        self.order_manager._track_order(position, sell_order)
            else:
                # Shares on exchange for a completely untracked market
                # (e.g. fill happened right as market was being exited)
                logger.warning(
                    f"Reconciliation: {token_id[:16]}... has {actual_size} shares "
                    f"on exchange but not in ANY tracked position — recovery SELL"
                )
                self._place_recovery_sell(token_id, actual_size)

    def _force_sell_stale_positions(self):
        """Hourly sweep: find ALL on-chain positions and force-sell any without a pending SELL.

        This catches positions that slipped through tracking — e.g., after crashes,
        phantom resets, or race conditions. More aggressive than _reconcile_inventory()
        because it doesn't skip recovered/phantom tokens.
        """
        try:
            r = requests.get(
                f"https://data-api.polymarket.com/positions"
                f"?user={config.WALLET_ADDRESS.lower()}",
                timeout=10,
            )
            positions = r.json()
        except Exception as e:
            logger.warning(f"Force-sell sweep: failed to fetch positions: {e}")
            return

        sold_count = 0
        for p in positions:
            token_id = p.get("asset", "")
            size = float(p.get("size", 0))
            if not token_id or size <= 0:
                continue

            # Check if this token already has a SELL order tracked
            has_tracked_sell = False
            for pos in self.order_manager.positions.values():
                if token_id in (pos.token_id_yes, pos.token_id_no):
                    has_tracked_sell = any(
                        o.side == "SELL" and o.token_id == token_id
                        for o in pos.orders
                    )
                    break

            if has_tracked_sell:
                continue

            # No tracked SELL — force sell it
            logger.info(
                f"Force-sell sweep: {token_id[:16]}... has {size} shares with no SELL — selling"
            )
            self._place_recovery_sell(token_id, size)
            sold_count += 1

        if sold_count > 0:
            logger.info(f"Force-sell sweep: placed {sold_count} recovery SELLs")
        else:
            logger.info("Force-sell sweep: no stale positions found")

    def _get_usdc_balance(self) -> float:
        """Fetch current free USDC balance from the exchange."""
        try:
            params = BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                signature_type=config.SIGNATURE_TYPE,
            )
            bal = self.client.get_balance_allowance(params)
            return int(bal.get("balance", "0")) / 1_000_000
        except Exception as e:
            logger.warning(f"Balance API failed: {e}")
            return 0.0

    def _compute_max_markets(self) -> int:
        """Dynamically compute max markets based on free USDC + active positions."""
        free_balance = self._get_usdc_balance()
        active_count = len(self.order_manager.positions)

        usable = free_balance * (1.0 - config.BALANCE_RESERVE_FRACTION)
        new_affordable = int(usable / config.COST_PER_MARKET_ESTIMATE)
        total = max(1, active_count + new_affordable)
        if config.MAX_MARKETS_CAP > 0:
            total = min(total, config.MAX_MARKETS_CAP)

        # Apply time-based market caps
        peak = is_peak_hours()
        if peak:
            peak_cap = getattr(config, "PEAK_MAX_MARKETS", 0)
            if peak_cap > 0:
                total = min(total, peak_cap)
        else:
            off_peak_cap = getattr(config, "OFF_PEAK_MAX_MARKETS", 0)
            if off_peak_cap > 0:
                total = min(total, off_peak_cap)

        peak_label = " (PEAK)" if peak else ""
        logger.info(
            f"Balance: ${free_balance:.2f} | active: {active_count} | "
            f"can afford {new_affordable} more | max markets: {total}{peak_label}"
        )
        return total

    def scan_and_select_markets(self) -> list[MarketOpportunity]:
        """Run a full market scan and return top opportunities."""
        max_markets = self._compute_max_markets()
        logger.info("Scanning markets...")
        # Pass recovery tokens so the scanner force-includes those markets
        force_tokens = set(self._recovery_info.keys()) if self._recovery_info else None
        return self.scanner.scan_and_rank(
            max_markets=max_markets, force_include_tokens=force_tokens
        )

    def _adopt_recovery_position(self, opp: MarketOpportunity):
        """Convert a recovery SELL into a fully tracked OrderManager position.

        This enables drift detection and SELL repricing for positions
        inherited from a previous bot session.
        """
        from order_manager import MarketPosition, ActiveOrder, SELL

        # Find which token has recovery shares
        for is_yes, token_id in [(True, opp.token_id_yes), (False, opp.token_id_no)]:
            info = self._recovery_info.get(token_id)
            if not info:
                continue

            size, order_id, sell_price = info

            # Get current midpoint
            mid = self.price_monitor.get_current_midpoint(opp.token_id_yes)
            if mid is None:
                mid = opp.midpoint

            position = MarketPosition(
                condition_id=opp.condition_id,
                token_id_yes=opp.token_id_yes,
                token_id_no=opp.token_id_no,
                max_spread=opp.max_spread,
                min_size=opp.min_size,
                tick_size=opp.tick_size,
                last_midpoint=mid,
            )

            # Set inventory
            if is_yes:
                position.yes_inventory = size
                position.yes_entry_price = sell_price  # approximate
            else:
                position.no_inventory = size
                position.no_entry_price = sell_price

            # Track the existing SELL order if it was placed successfully
            if order_id and sell_price > 0:
                tracked_order = ActiveOrder(
                    order_id=order_id,
                    token_id=token_id,
                    side=SELL,
                    price=sell_price,
                    size=size,
                    condition_id=opp.condition_id,
                    placed_at=time.time(),
                    midpoint_at_placement=mid,
                )
                position.orders.append(tracked_order)

            self.order_manager.positions[opp.condition_id] = position

            # Subscribe WS
            if self.ws_monitor:
                self.ws_monitor.subscribe_market({opp.token_id_yes, opp.token_id_no})
                self.ws_monitor.subscribe_user({opp.condition_id})

            # Clean up recovery tracking (now fully managed by OrderManager)
            self._recovered_token_ids.discard(opp.token_id_yes)
            self._recovered_token_ids.discard(opp.token_id_no)
            if order_id:
                self._recovered_order_ids.discard(order_id)
            self._recovery_info.pop(token_id, None)

            label = "YES" if is_yes else "NO"
            logger.info(
                f"Adopted recovery {label} position for {opp.question[:50]} | "
                f"inv={size} SELL@{sell_price:.4f} mid={mid:.4f} | "
                f"drift detection now active"
            )
            return  # only one token per market can have recovery

    def place_initial_orders(self, opportunities: list[MarketOpportunity]):
        """Place orders on newly selected markets.

        Uses uniform sizing (min_size) across all markets.
        Existing safety caps (MAX_ORDER_SIZE, MAX_SINGLE_ORDER_USDC) still apply."""
        # Startup cooldown: don't place new BUY orders until WS is connected
        # and we've had time to sync state (prevents fills when cooldown state was lost)
        startup_cooldown = getattr(config, "STARTUP_COOLDOWN_SECONDS", 60)
        if self._start_time and time.time() - self._start_time < startup_cooldown:
            remaining = int(startup_cooldown - (time.time() - self._start_time))
            logger.info(f"Startup cooldown: {remaining}s remaining before placing new orders")
            return

        # Global circuit breaker: don't place new orders during fill pause
        if self.order_manager.is_global_paused:
            remaining = int(
                config.GLOBAL_FILL_PAUSE_SECONDS
                - (time.time() - self.order_manager._last_global_fill)
            )
            logger.info(f"Global fill pause: {remaining}s remaining before placing new orders")
            return

        active_cids = set(self.order_manager.get_active_condition_ids())

        for opp in opportunities:
            if opp.condition_id in active_cids:
                continue  # Already have orders here

            # Skip blacklisted markets (recently filled — wait before re-entering)
            if self.order_manager.is_blacklisted(opp.condition_id):
                continue

            # Adopt recovery positions into OrderManager for drift tracking
            if (opp.token_id_yes in self._recovered_token_ids
                    or opp.token_id_no in self._recovered_token_ids):
                self._adopt_recovery_position(opp)
                continue

            # Cap size so total per-market cost (both sides) stays within MAX_ENTRY_COST
            # BUY Yes + BUY No prices sum to ~1.0, so total cost ≈ min_size
            opp.min_size = min(opp.min_size, config.MAX_ENTRY_COST)

            position = self.order_manager.place_two_sided_orders(opp)
            if position:
                self.active_opportunities[opp.condition_id] = opp
                # Subscribe new market to WS channels
                if self.ws_monitor:
                    self.ws_monitor.subscribe_market(
                        {opp.token_id_yes, opp.token_id_no}
                    )
                    self.ws_monitor.subscribe_user({opp.condition_id})

    def _cleanup_orphaned_orders(self):
        """Cancel orphaned orders that aren't tracked by the bot.

        Drift adjustments can leave orphans when cancel API returns success
        but the order lingers. This identifies true orphans (excluding recovery
        SELLs and tracked orders) and cancels them individually. Runs each
        rescan cycle so orphans get cleaned up over multiple passes.
        """
        try:
            open_orders = safe_api_call(self.client.get_orders)
            if not isinstance(open_orders, list):
                return

            # All IDs the bot owns: tracked positions + recovery SELLs
            known_ids = set(self._recovered_order_ids)
            for position in self.order_manager.positions.values():
                for order in position.orders:
                    known_ids.add(order.order_id)

            # Find true orphans
            orphan_ids = []
            for o in open_orders:
                oid = o.get("id") if isinstance(o, dict) else getattr(o, "id", None)
                if oid and oid not in known_ids:
                    orphan_ids.append(oid)

            if not orphan_ids:
                return

            logger.warning(
                f"Found {len(orphan_ids)} orphaned orders "
                f"(known: {len(known_ids)}) — cancelling"
            )
            for oid in orphan_ids:
                try:
                    safe_api_call(self.client.cancel, oid)
                except Exception:
                    pass  # Best effort; next cycle will retry

        except Exception as e:
            logger.warning(f"Orphan cleanup failed: {e}")

    def _check_active_volatility(self):
        """Exit active markets whose volatility has spiked above threshold.

        Runs during each rescan cycle. For each active position, fetches
        24h price history and computes volatility ratio. If it exceeds
        MAX_VOLATILITY_RATIO, cancels BUYs (keeps SELLs for unwind).
        """
        if config.MAX_VOLATILITY_RATIO <= 0:
            return

        for cid in list(self.order_manager.get_active_condition_ids()):
            position = self.order_manager.positions.get(cid)
            if not position:
                continue

            token_id = position.token_id_yes
            try:
                resp = requests.get(
                    f"{config.CLOB_HOST}/prices-history",
                    params={"market": token_id, "interval": "1d", "fidelity": 60},
                    timeout=10,
                )
                if resp.status_code != 200:
                    continue
                history = resp.json().get("history", [])
                if len(history) < 2:
                    continue
                prices = [float(p["p"]) for p in history if "p" in p]
                if not prices:
                    continue

                price_range = max(prices) - min(prices)
                vol_ratio = price_range / position.max_spread

                if vol_ratio > config.MAX_VOLATILITY_RATIO:
                    logger.warning(
                        f"VOLATILITY EXIT: {cid[:16]}... ratio={vol_ratio:.1f}x "
                        f"(>{config.MAX_VOLATILITY_RATIO}x) — cancelling BUYs"
                    )
                    # Cancel BUY orders only; keep SELLs for inventory unwind
                    buy_orders = [o for o in position.orders if o.side == "BUY"]
                    for order in buy_orders:
                        try:
                            safe_api_call(self.client.cancel, order.order_id)
                        except Exception:
                            pass
                    position.orders = [o for o in position.orders if o.side != "BUY"]

                    # If no inventory, fully exit
                    if position.yes_inventory == 0 and position.no_inventory == 0:
                        self.order_manager.cancel_market_orders(cid)
                        self.active_opportunities.pop(cid, None)

            except Exception as e:
                logger.warning(f"Volatility check failed for {cid[:16]}...: {e}")

    def exit_stale_markets(self, current_best: list[MarketOpportunity]):
        """Remove orders from markets no longer in the top list.

        If a position still holds inventory (filled BUY waiting to SELL),
        only cancel BUY orders and keep the position alive so the SELL
        unwind completes via retry_pending_sells.
        """
        best_cids = {opp.condition_id for opp in current_best}
        active_cids = list(self.order_manager.get_active_condition_ids())

        for cid in active_cids:
            if cid not in best_cids:
                position = self.order_manager.positions.get(cid)
                has_inventory = position and (
                    position.yes_inventory > 0 or position.no_inventory > 0
                )

                if has_inventory:
                    # Don't fully exit — still holding shares that need to be sold.
                    # Cancel BUY orders only; keep SELL orders and position alive.
                    buy_orders = [o for o in position.orders if o.side == "BUY"]
                    for order in buy_orders:
                        try:
                            safe_api_call(self.client.cancel, order.order_id)
                        except Exception:
                            pass
                    position.orders = [o for o in position.orders if o.side != "BUY"]
                    logger.info(
                        f"Stale market {cid[:16]}... has inventory "
                        f"(Y={position.yes_inventory} N={position.no_inventory}) "
                        f"— cancelled BUYs, keeping for SELL unwind"
                    )
                else:
                    logger.info(f"Exiting stale market {cid[:16]}...")
                    self.order_manager.cancel_market_orders(cid)

                self.active_opportunities.pop(cid, None)

    def _sync_ws_subscriptions(self):
        """Sync WS subscriptions with current active positions."""
        if not self.ws_monitor:
            return

        token_ids, condition_ids = self.order_manager.get_all_subscribed_ids()

        # Also include recovered token IDs (we want price updates for them too)
        for tid in self._recovered_token_ids:
            token_ids.add(tid)

        self.ws_monitor.subscribe_market(token_ids)
        self.ws_monitor.subscribe_user(condition_ids)

    def run(self):
        """Main event loop with WebSocket-driven price and fill monitoring."""
        self.initialize()
        self.running = True
        self._start_time = time.time()
        last_scan_time = 0
        last_status_time = 0

        # Start WebSocket connections
        if self.ws_monitor:
            self.ws_monitor.start()
            logger.info("WebSocket connections started")

        # Force-adopt all recovery positions into OrderManager immediately.
        # This ensures ALL held inventory gets WS tracking, drift detection,
        # and best_ask undercutting — even markets not selected by the scanner.
        self._force_adopt_recovery_positions()
        self._sync_ws_subscriptions()

        last_size_multiplier = get_size_multiplier()
        last_force_sell_time = 0
        logger.info(f"Bot started. Size multiplier: {last_size_multiplier:.1f}x. Press Ctrl+C to stop.")

        consecutive_errors = 0

        while self.running:
            now = time.time()
            ws_active = False

            try:
                # === Phase 0: Time-based sizing transition ===
                current_multiplier = get_size_multiplier()
                if current_multiplier != last_size_multiplier:
                    logger.info(
                        f"Size multiplier changed: {last_size_multiplier:.1f}x -> "
                        f"{current_multiplier:.1f}x — replacing all BUY orders"
                    )
                    for cid in list(self.order_manager.positions.keys()):
                        pos = self.order_manager.positions.get(cid)
                        if not pos:
                            continue
                        # Only replace if position has BUY orders (skip SELL-only/inventory positions)
                        has_buy = any(o.side == "BUY" for o in pos.orders)
                        if not has_buy:
                            continue
                        midpoint = self.price_monitor.last_midpoints.get(cid, pos.last_midpoint)
                        self.order_manager.replace_orders(cid, midpoint)
                    last_size_multiplier = current_multiplier

                # === Phase 1: Periodic full re-scan (unchanged cadence) ===
                if now - last_scan_time >= config.RESCAN_INTERVAL_SECONDS:
                    self._reconcile_inventory()
                    self._cleanup_orphaned_orders()
                    self._check_active_volatility()
                    opportunities = self.scan_and_select_markets()
                    self.exit_stale_markets(opportunities)
                    self.place_initial_orders(opportunities)
                    last_scan_time = now

                    # Update WS subscriptions after placing/exiting orders
                    self._sync_ws_subscriptions()

                    # Hourly force-sell sweep for stale positions
                    if now - last_force_sell_time >= 3600:
                        self._force_sell_stale_positions()
                        last_force_sell_time = now

                # === Phase 2: Event processing (WS primary, REST fallback) ===
                ws_active = (
                    self.ws_monitor
                    and self.ws_monitor.is_market_connected
                    and self.ws_monitor.is_user_connected
                )

                if ws_active:
                    # 2a: Process fill events from user WS channel
                    trade_events = self.ws_monitor.drain_trade_events()
                    for event in trade_events:
                        if isinstance(event, TradeEvent) and event.status == "MATCHED":
                            self.order_manager.handle_ws_fill(
                                order_id=event.order_id,
                                asset_id=event.asset_id,
                                side=event.side,
                                size_matched=event.size_matched,
                                price=event.price,
                            )

                    # 2b: Process price events from market WS channel
                    price_events = self.ws_monitor.drain_price_events()
                    if price_events:
                        drifted, stop_losses, sell_reprices = self.price_monitor.update_midpoints_from_ws(
                            price_events
                        )
                        if drifted:
                            self.price_monitor.adjust_drifted_positions(drifted)
                        for cid in stop_losses:
                            self.order_manager.force_exit_market(cid)
                            self.active_opportunities.pop(cid, None)
                        # Aggressive SELL repricing for inventory positions
                        for cid in sell_reprices:
                            midpoint = self.price_monitor.last_midpoints.get(cid)
                            if midpoint:
                                self.order_manager.reprice_sell_if_stale(
                                    cid, midpoint,
                                    best_asks=self.price_monitor.last_best_asks,
                                )

                    # 2c: REST fallback sync on longer interval (safety net)
                    if now - self._last_rest_sync >= config.REST_FALLBACK_INTERVAL_SECONDS:
                        fills = self.order_manager.sync_with_exchange()
                        if fills:
                            self.order_manager.handle_filled_orders(fills)
                        self._last_rest_sync = now

                else:
                    # === Phase 2 (fallback): Pure REST polling ===
                    fills = self.order_manager.sync_with_exchange()
                    if fills:
                        self.order_manager.handle_filled_orders(fills)

                    drifted, stop_losses, sell_reprices = self.price_monitor.check_all_positions()
                    if drifted:
                        self.price_monitor.adjust_drifted_positions(drifted)
                    for cid in stop_losses:
                        self.order_manager.force_exit_market(cid)
                        self.active_opportunities.pop(cid, None)
                    # Aggressive SELL repricing for inventory positions
                    for cid in sell_reprices:
                        midpoint = self.price_monitor.last_midpoints.get(cid)
                        if midpoint:
                            self.order_manager.reprice_sell_if_stale(cid, midpoint)

                # === Phase 3: Cooldown re-entries and SELL retries ===
                self.order_manager.process_cooldown_reentries()
                self.order_manager.retry_pending_sells()

                # === Phase 4: Status logging (every 30s to reduce noise) ===
                if now - last_status_time >= 30:
                    active = len(self.order_manager.positions)
                    total_orders = sum(
                        len(p.orders)
                        for p in self.order_manager.positions.values()
                    )
                    ws_status = ""
                    if self.ws_monitor:
                        m = "OK" if self.ws_monitor.is_market_connected else "DOWN"
                        u = "OK" if self.ws_monitor.is_user_connected else "DOWN"
                        ws_status = f" | WS: mkt={m} usr={u}"
                    inv_count = sum(
                        1 for p in self.order_manager.positions.values()
                        if p.yes_inventory > 0 or p.no_inventory > 0
                    )
                    inv_info = f" | {inv_count} with inventory" if inv_count else ""
                    pause_info = ""
                    if self.order_manager.is_global_paused:
                        remaining = int(
                            config.GLOBAL_FILL_PAUSE_SECONDS
                            - (time.time() - self.order_manager._last_global_fill)
                        )
                        pause_info = f" | PAUSED ({remaining}s)"
                    size_mult = get_size_multiplier()
                    size_info = f" | size={size_mult:.1f}x" if size_mult != 1.0 else ""
                    logger.info(
                        f"Active: {active} markets, {total_orders} orders{inv_info}{ws_status}{pause_info}{size_info} | "
                        f"Next scan in {max(0, config.RESCAN_INTERVAL_SECONDS - (time.time() - last_scan_time)):.0f}s"
                    )
                    last_status_time = now

                consecutive_errors = 0

            except Exception as e:
                consecutive_errors += 1
                logger.error(f"Error in main loop ({consecutive_errors}/{config.MAX_CONSECUTIVE_ERRORS}): {e}", exc_info=True)
                if consecutive_errors >= config.MAX_CONSECUTIVE_ERRORS:
                    logger.critical(
                        f"FATAL: {consecutive_errors} consecutive errors — self-terminating for watchdog restart"
                    )
                    self.shutdown()
                    sys.exit(1)

            # Sleep: 0.5s when WS active (fast event draining), 5s when REST-only
            sleep_time = 0.5 if ws_active else config.MONITOR_INTERVAL_SECONDS
            time.sleep(sleep_time)

    def shutdown(self):
        """Graceful shutdown - stop WS, cancel all open orders."""
        self.running = False
        logger.info("Shutting down...")

        # Stop WebSocket connections first
        if self.ws_monitor:
            self.ws_monitor.stop()
            logger.info("WebSocket connections stopped")

        if self.client:
            self._cancel_all_with_retry()

        if self.order_manager:
            self.order_manager.positions.clear()

        logger.info("Shutdown complete")

    def _signal_handler(self, sig, frame):
        logger.info("Shutdown signal received")
        self.shutdown()
        sys.exit(0)


def _kill_existing_instances():
    """Find and kill ALL other bot.py processes before starting.

    On macOS, nohup processes run as e.g.
      /Library/Frameworks/Python.framework/.../Python bot.py
    so we search for any process whose command line contains 'bot.py',
    excluding our own PID.
    """
    my_pid = os.getpid()

    while True:
        # Find all PIDs whose command line contains "bot.py" (case-insensitive)
        try:
            result = subprocess.run(
                ["pgrep", "-if", "bot\\.py"],
                capture_output=True, text=True, timeout=5,
            )
            pids = [
                int(p) for p in result.stdout.strip().split("\n")
                if p.strip() and int(p) != my_pid
            ]
        except Exception:
            pids = []

        if not pids:
            break

        logger.warning(f"Killing {len(pids)} existing bot instance(s): {pids}")
        for pid in pids:
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass

        time.sleep(1)  # Wait for OS to reap, then verify they're gone


def _acquire_lock():
    """Kill existing instances and write PID lock file."""
    _kill_existing_instances()

    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))
    atexit.register(_release_lock)


def _release_lock():
    """Remove PID lock file."""
    try:
        os.remove(LOCK_FILE)
    except FileNotFoundError:
        pass


def main():
    _acquire_lock()
    bot = LPRewardsBot()
    try:
        bot.run()
    except KeyboardInterrupt:
        bot.shutdown()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        bot.shutdown()
        sys.exit(1)


if __name__ == "__main__":
    main()
