import time
from dataclasses import dataclass, field

import requests
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    OrderArgs, MarketOrderArgs, OrderType,
    BalanceAllowanceParams, AssetType,
)

import config
from market_scanner import MarketOpportunity
from utils import (
    setup_logger,
    round_price_down,
    round_price_up,
    clamp_price,
    safe_api_call,
    get_size_multiplier,
)

logger = setup_logger("OrderManager")

BUY = "BUY"
SELL = "SELL"


@dataclass
class ActiveOrder:
    order_id: str
    token_id: str
    side: str
    price: float
    size: float
    condition_id: str
    placed_at: float
    midpoint_at_placement: float


@dataclass
class FillEvent:
    token_id: str
    side: str
    price: float
    size: float
    condition_id: str


@dataclass
class MarketPosition:
    condition_id: str
    token_id_yes: str
    token_id_no: str
    max_spread: float
    min_size: float
    tick_size: float
    orders: list[ActiveOrder] = field(default_factory=list)
    last_midpoint: float = 0.0
    yes_inventory: float = 0.0  # Yes shares held from filled BUY Yes
    no_inventory: float = 0.0   # No shares held from filled BUY No
    yes_entry_price: float = 0.0  # BUY Yes fill price (for stop-loss)
    no_entry_price: float = 0.0   # BUY No fill price (for stop-loss)
    # Fill tracking and cooldown (prevents fill→sell→buy→fill cycle)
    yes_fill_times: list[float] = field(default_factory=list)
    no_fill_times: list[float] = field(default_factory=list)
    yes_last_sell_fill: float = 0.0  # timestamp of last SELL YES fill
    no_last_sell_fill: float = 0.0   # timestamp of last SELL NO fill
    yes_blocked: bool = False  # True = too many fills, block re-entry
    no_blocked: bool = False


class OrderManager:
    def __init__(self, client: ClobClient):
        self.client = client
        self.positions: dict[str, MarketPosition] = {}
        self._sell_fail_counts: dict[tuple[str, str], int] = {}  # (cid, token_id) -> consecutive failures
        self._phantom_tokens: set[str] = set()  # tokens confirmed to have no balance (block reconciliation)
        self._last_global_fill: float = 0.0  # timestamp of last BUY fill (for circuit breaker)
        self._market_blacklist: dict[str, float] = {}  # condition_id -> blacklisted_at timestamp


    @property
    def is_global_paused(self) -> bool:
        """True if global circuit breaker cooldown is active (no BUYs allowed)."""
        if not getattr(config, "GLOBAL_CIRCUIT_BREAKER", False):
            return False
        pause = getattr(config, "GLOBAL_FILL_PAUSE_SECONDS", 120)
        return self._last_global_fill > 0 and time.time() - self._last_global_fill < pause

    def blacklist_market(self, condition_id: str) -> None:
        """Blacklist a market after a BUY fill. Prevents re-entry for MARKET_BLACKLIST_SECONDS."""
        self._market_blacklist[condition_id] = time.time()
        blacklist_hours = getattr(config, "MARKET_BLACKLIST_SECONDS", 7200) / 3600
        logger.info(f"Blacklisted {condition_id[:16]}... for {blacklist_hours:.0f}h after fill")

    def is_blacklisted(self, condition_id: str) -> bool:
        """Check if a market is blacklisted (recently filled)."""
        bl_time = self._market_blacklist.get(condition_id)
        if bl_time is None:
            return False
        elapsed = time.time() - bl_time
        if elapsed >= getattr(config, "MARKET_BLACKLIST_SECONDS", 7200):
            del self._market_blacklist[condition_id]
            return False
        return True

    def cancel_all_buys(self, reason: str = "") -> int:
        """Cancel ALL BUY orders across ALL markets. Returns count cancelled.

        Used by the global circuit breaker to immediately reduce fill exposure
        after any BUY fills. SELL orders are preserved for inventory unwind.
        """
        cancelled = 0
        for cid, position in self.positions.items():
            buy_orders = [o for o in position.orders if o.side == BUY]
            for order in buy_orders:
                try:
                    safe_api_call(self.client.cancel, order.order_id)
                    cancelled += 1
                except Exception:
                    pass
            position.orders = [o for o in position.orders if o.side != BUY]

        if cancelled > 0:
            logger.warning(
                f"CIRCUIT BREAKER: Cancelled {cancelled} BUY orders across all markets"
                f"{' — ' + reason if reason else ''}"
            )
        return cancelled

    def _get_order_status(self, order_id: str) -> str:
        """Get definitive order status from the CLOB API.

        Returns one of:
          "MATCHED"   — order was filled (real fill)
          "CANCELLED" — order was cancelled (not filled, should stop tracking)
          "LIVE"      — order is still active (get_orders() just missed it)
          "UNKNOWN"   — API error (couldn't determine)
        """
        try:
            order_data = safe_api_call(self.client.get_order, order_id)
            if isinstance(order_data, dict):
                status = str(order_data.get("status", "")).upper()
                size_matched = float(order_data.get("size_matched", 0) or 0)
                if status == "MATCHED" or size_matched > 0:
                    return "MATCHED"
                if status in ("CANCELLED", "EXPIRED"):
                    logger.info(
                        f"Order {order_id[:16]}... status={status} — NOT a fill"
                    )
                    return "CANCELLED"
                # Still live (get_orders() missed it)
                logger.info(
                    f"Order {order_id[:16]}... status={status} — still live (phantom miss)"
                )
                return "LIVE"
        except Exception as e:
            logger.warning(f"Failed to verify order {order_id[:16]}...: {e}")
        return "UNKNOWN"

    def _get_current_midpoint(self, token_id: str) -> float | None:
        try:
            resp = safe_api_call(self.client.get_midpoint, token_id)
            if isinstance(resp, dict):
                mid = float(resp.get("mid", 0))
            else:
                mid = float(resp)
            if mid <= 0 or mid >= 1:
                return None
            return mid
        except Exception:
            return None

    def _track_order(self, position: MarketPosition, order: ActiveOrder) -> bool:
        """Add order to position tracking, respecting MAX_ORDERS_PER_MARKET.

        If the cap would be exceeded, cancels the just-placed order on the
        exchange and returns False so it doesn't become an orphan.
        """
        if len(position.orders) >= config.MAX_ORDERS_PER_MARKET:
            logger.warning(
                f"Order cap ({config.MAX_ORDERS_PER_MARKET}) for "
                f"{position.condition_id[:16]}..., cancelling excess {order.side}"
            )
            try:
                safe_api_call(self.client.cancel, order.order_id)
            except Exception:
                pass
            return False
        position.orders.append(order)
        return True

    @staticmethod
    def _compute_buffer(max_spread: float, tick_size: float) -> float:
        """Compute per-market spread buffer (proportional to max_spread)."""
        buf = max(max_spread * config.SPREAD_BUFFER_FRACTION, config.MIN_SPREAD_BUFFER)
        return max(buf, tick_size)

    def calculate_order_prices(
        self,
        midpoint: float,
        max_spread: float,
        tick_size: float,
    ) -> tuple[float, float]:
        """
        Calculate BUY Yes and BUY No prices at the edge of the reward range.

        BUY Yes at low edge -> Q_one (bid-side scoring).
        BUY No at equivalent high edge -> Q_two (ask-side scoring).
        Buying No is economically equivalent to selling Yes (both use USDC).

        After rounding, verify the price is strictly inside the reward range.
        If rounding pushed it to or past the boundary, move one tick inward.
        """
        buffer = self._compute_buffer(max_spread, tick_size)
        effective_spread = max_spread - buffer

        # BUY Yes: below midpoint
        buy_yes_price = midpoint - effective_spread
        buy_yes_price = round_price_down(buy_yes_price, tick_size)
        buy_yes_price = clamp_price(buy_yes_price, tick_size)
        # Post-rounding boundary check: ensure strictly inside reward range
        # Use round(,8) to avoid float comparison issues (e.g. 0.035 - epsilon)
        if round(midpoint - buy_yes_price, 8) >= max_spread:
            buy_yes_price += tick_size
            buy_yes_price = clamp_price(buy_yes_price, tick_size)

        # BUY No: equivalent to SELL Yes at (midpoint + effective_spread)
        # No price = 1 - Yes price, so buy_no = 1 - (midpoint + effective_spread)
        no_midpoint = 1.0 - midpoint
        buy_no_price = no_midpoint - effective_spread
        buy_no_price = round_price_down(buy_no_price, tick_size)
        buy_no_price = clamp_price(buy_no_price, tick_size)
        # Post-rounding boundary check
        if round(no_midpoint - buy_no_price, 8) >= max_spread:
            buy_no_price += tick_size
            buy_no_price = clamp_price(buy_no_price, tick_size)

        return buy_yes_price, buy_no_price

    def _calculate_sell_price(
        self, midpoint: float, max_spread: float, tick_size: float, is_yes: bool
    ) -> float:
        """Calculate SELL price at or just below midpoint for fastest fill.

        Uses round_price_down to place at or below midpoint, making us
        the first ask in queue. In tight-spread markets this may match
        the best bid (instant fill) — which is desirable for inventory unwind.
        """
        if is_yes:
            sell_price = round_price_down(midpoint, tick_size)
        else:
            no_midpoint = 1.0 - midpoint
            sell_price = round_price_down(no_midpoint, tick_size)
        return clamp_price(sell_price, tick_size)

    def place_two_sided_orders(self, opp: MarketOpportunity) -> MarketPosition:
        """
        Place BUY Yes + BUY No orders for a market.

        BUY Yes at edge-low -> Q_one scoring (bid side).
        BUY No at edge-high -> Q_two scoring (ask side).
        Both orders use USDC only (no need to hold shares).
        """
        # Global circuit breaker: don't place new BUYs during pause
        if self.is_global_paused:
            remaining = int(
                config.GLOBAL_FILL_PAUSE_SECONDS
                - (time.time() - self._last_global_fill)
            )
            logger.debug(
                f"Skipping new orders for {opp.condition_id[:16]}... "
                f"(global pause, {remaining}s remaining)"
            )
            return None

        buy_yes_price, buy_no_price = self.calculate_order_prices(
            opp.midpoint, opp.max_spread, opp.tick_size
        )

        no_midpoint = 1.0 - opp.midpoint
        if buy_yes_price >= opp.midpoint or buy_no_price >= no_midpoint:
            logger.warning(
                f"Invalid prices for {opp.condition_id}: "
                f"buy_yes={buy_yes_price}, buy_no={buy_no_price}, "
                f"mid={opp.midpoint}, no_mid={no_midpoint}"
            )
            return None

        position = MarketPosition(
            condition_id=opp.condition_id,
            token_id_yes=opp.token_id_yes,
            token_id_no=opp.token_id_no,
            max_spread=opp.max_spread,
            min_size=opp.min_size,
            tick_size=opp.tick_size,
            last_midpoint=opp.midpoint,
        )

        # Apply time-based sizing (larger during US off-hours, baseline during peak)
        buy_size = opp.min_size * get_size_multiplier()

        # Place BUY Yes order (Q_one: bid side)
        buy_yes_order = self._place_order(
            opp.token_id_yes, buy_yes_price, buy_size, BUY, opp.condition_id, opp.midpoint
        )
        if buy_yes_order:
            self._track_order(position, buy_yes_order)

        # Place BUY No order (Q_two: ask side equivalent)
        buy_no_order = self._place_order(
            opp.token_id_no, buy_no_price, buy_size, BUY, opp.condition_id, opp.midpoint
        )
        if buy_no_order:
            self._track_order(position, buy_no_order)

        if position.orders:
            self.positions[opp.condition_id] = position
            logger.info(
                f"Placed {len(position.orders)} orders on {opp.question[:50]} | "
                f"BUY_YES@{buy_yes_price:.4f} BUY_NO@{buy_no_price:.4f} | "
                f"size={buy_size} mid={opp.midpoint:.4f} ({get_size_multiplier():.1f}x)"
            )
        else:
            logger.warning(f"Failed to place any orders for {opp.condition_id}")

        return position

    def _place_order(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str,
        condition_id: str,
        midpoint: float,
        min_order_size: float = 0,
    ) -> ActiveOrder | None:
        """Place a single order and return tracked ActiveOrder.

        For SELL orders below min_order_size, uses a FOK market order
        so the exchange accepts it and fills immediately.
        """
        # Sanity checks: reject obviously bad or dangerous orders
        if price <= 0 or price >= 1 or size <= 0:
            logger.error(f"Refusing order with invalid params: price={price}, size={size}")
            return None
        if size > config.MAX_ORDER_SIZE:
            logger.error(f"Refusing order: size {size} exceeds MAX_ORDER_SIZE {config.MAX_ORDER_SIZE}")
            return None
        if price * size > config.MAX_SINGLE_ORDER_USDC:
            logger.error(f"Refusing order: cost ${price * size:.2f} exceeds MAX_SINGLE_ORDER_USDC ${config.MAX_SINGLE_ORDER_USDC}")
            return None
        # Enforce inventory cap: don't place BUY if filling it would exceed limit
        if side == BUY:
            position = self.positions.get(condition_id)
            if position:
                current_inv = (position.yes_inventory if token_id == position.token_id_yes
                               else position.no_inventory)
                if current_inv + size > config.MAX_INVENTORY_PER_SIDE:
                    logger.warning(
                        f"Refusing BUY: would exceed MAX_INVENTORY_PER_SIDE "
                        f"({current_inv} + {size} > {config.MAX_INVENTORY_PER_SIDE})"
                    )
                    return None
        try:
            # Ensure conditional token allowance is set before selling shares
            if side == SELL:
                self.client.update_balance_allowance(BalanceAllowanceParams(
                    asset_type=AssetType.CONDITIONAL,
                    token_id=token_id,
                    signature_type=config.SIGNATURE_TYPE,
                ))

            # For sub-min_size SELLs: try GTC first (exchange min is usually lower
            # than rewards min_size). Only fall back to FOK if exchange rejects.
            is_sub_min_sell = (side == SELL and min_order_size > 0 and size < min_order_size)

            if is_sub_min_sell:
                # Try GTC first — exchange minimum is often much lower than rewards min_size
                try:
                    order_args = OrderArgs(
                        price=price, size=size, side=side, token_id=token_id,
                    )
                    signed_order = self.client.create_order(order_args)
                    resp = safe_api_call(self.client.post_order, signed_order, OrderType.GTC)
                except Exception as gtc_err:
                    # GTC rejected (likely below exchange minimum) — try FOK
                    logger.info(f"GTC SELL rejected ({gtc_err}), trying FOK for {size} shares")
                    market_args = MarketOrderArgs(
                        token_id=token_id, amount=size, side=side, price=price,
                    )
                    signed_order = self.client.create_market_order(market_args)
                    resp = safe_api_call(self.client.post_order, signed_order, OrderType.FOK)
            else:
                order_args = OrderArgs(
                    price=price,
                    size=size,
                    side=side,
                    token_id=token_id,
                )
                signed_order = self.client.create_order(order_args)
                resp = safe_api_call(self.client.post_order, signed_order, OrderType.GTC)

            order_id = None
            if isinstance(resp, dict):
                if not resp.get("success", True):
                    logger.error(f"Order rejected: {resp.get('errorMsg', 'unknown')}")
                    return None
                order_id = resp.get("orderID") or resp.get("orderId")
            else:
                order_id = getattr(resp, "orderID", None) or getattr(resp, "id", None)

            if not order_id:
                logger.warning(f"No order ID returned for {side} {token_id}")
                return None

            return ActiveOrder(
                order_id=order_id,
                token_id=token_id,
                side=side,
                price=price,
                size=size,
                condition_id=condition_id,
                placed_at=time.time(),
                midpoint_at_placement=midpoint,
            )

        except Exception as e:
            logger.error(f"Failed to place {side} order at {price}: {e}")
            return None

    def cancel_market_orders(self, condition_id: str) -> bool:
        """Cancel all orders for a specific market."""
        position = self.positions.get(condition_id)
        if not position:
            return True

        success = True
        for order in position.orders:
            try:
                safe_api_call(self.client.cancel, order.order_id)
                logger.info(f"Cancelled {order.side} order {order.order_id}")
            except Exception as e:
                logger.error(f"Failed to cancel order {order.order_id}: {e}")
                success = False

        if success:
            del self.positions[condition_id]

        return success

    def cancel_all_orders(self) -> bool:
        """Cancel every tracked order."""
        try:
            safe_api_call(self.client.cancel_all)
            self.positions.clear()
            logger.info("Cancelled all orders")
            return True
        except Exception as e:
            logger.error(f"Failed to cancel all orders: {e}")
            return False

    def force_exit_market(self, condition_id: str):
        """Stop-loss exit: cancel BUYs, place or reprice SELL at current midpoint.

        Unlike replace_orders(), does NOT re-place BUY orders. The market
        is being abandoned to limit losses.

        If a SELL already exists but is far from the current midpoint,
        reprices it (cancel + re-place) so it can actually fill.
        Throttled to once per 30s per market to avoid churn.
        """
        position = self.positions.get(condition_id)
        if not position:
            return

        # Only cancel BUY orders; keep existing SELLs alive
        buy_orders = [o for o in position.orders if o.side == BUY]
        sell_orders = [o for o in position.orders if o.side == SELL]
        for order in buy_orders:
            try:
                safe_api_call(self.client.cancel, order.order_id)
            except Exception:
                pass
        position.orders = list(sell_orders)  # keep SELLs

        mid = self._get_current_midpoint(position.token_id_yes)
        if mid is None:
            mid = position.last_midpoint

        for is_yes, token_id, inv_attr in [
            (True, position.token_id_yes, "yes_inventory"),
            (False, position.token_id_no, "no_inventory"),
        ]:
            inv = getattr(position, inv_attr)
            if inv <= 0:
                continue

            # Calculate target SELL price at current midpoint (aggressive: round down)
            if is_yes:
                target_price = round_price_down(mid, position.tick_size)
            else:
                no_mid = 1.0 - mid
                target_price = round_price_down(no_mid, position.tick_size)
            target_price = clamp_price(target_price, position.tick_size)
            token_label = "YES" if is_yes else "NO"

            existing_sell = next(
                (o for o in sell_orders if o.token_id == token_id), None
            )

            if existing_sell is None:
                # No SELL exists — place one at current midpoint
                sell_order = self._place_order(
                    token_id, target_price, inv,
                    SELL, condition_id, mid,
                    min_order_size=position.min_size,
                )
                if sell_order:
                    self._track_order(position, sell_order)
                    logger.warning(
                        f"STOP-LOSS SELL {token_label}@{target_price:.4f} "
                        f"x{inv} for {condition_id[:16]}..."
                    )
            elif abs(existing_sell.price - target_price) >= position.tick_size:
                # SELL exists but stale — reprice immediately (no throttle)
                try:
                    safe_api_call(self.client.cancel, existing_sell.order_id)
                except Exception:
                    pass
                position.orders = [o for o in position.orders if o.order_id != existing_sell.order_id]
                time.sleep(0.5)  # brief wait for token unlock
                sell_order = self._place_order(
                    token_id, target_price, inv,
                    SELL, condition_id, mid,
                    min_order_size=position.min_size,
                )
                if sell_order:
                    self._track_order(position, sell_order)
                    logger.info(
                        f"Repriced SELL {token_label}: "
                        f"{existing_sell.price:.4f} -> {target_price:.4f} "
                        f"x{inv} for {condition_id[:16]}..."
                    )

        # Only remove if no orders AND no inventory (SELL may have failed — retry will catch it)
        if not position.orders and position.yes_inventory == 0 and position.no_inventory == 0:
            del self.positions[condition_id]
            logger.info(f"Stop-loss exit complete for {condition_id[:16]}... (no inventory)")

    def replace_orders(
        self,
        condition_id: str,
        new_midpoint: float,
    ) -> MarketPosition | None:
        """Cancel old orders, then place new ones at updated midpoint.
        Preserves inventory and re-places SELL unwind orders if holding shares.

        Before cancelling, checks which tracked orders are still live.
        Any BUY orders that vanished since the last sync are treated as fills
        so inventory is updated and SELL unwinds are placed.
        """
        position = self.positions.get(condition_id)
        if not position:
            return None

        # --- Detect fills that happened since last sync (race window) ---
        try:
            open_orders = safe_api_call(self.client.get_orders)
            live_ids = set()
            if isinstance(open_orders, list):
                for o in open_orders:
                    oid = o.get("id") if isinstance(o, dict) else getattr(o, "id", None)
                    if oid:
                        live_ids.add(oid)

            now = time.time()
            grace = config.ORDER_GRACE_PERIOD_SECONDS
            for order in position.orders:
                if order.order_id not in live_ids and now - order.placed_at >= grace:
                    if order.side == BUY:
                        # Verify fill via get_order() before assuming
                        status = self._get_order_status(order.order_id)
                        if status != "MATCHED" and status != "UNKNOWN":
                            continue  # Phantom — order was cancelled or still live
                        is_yes = order.token_id == position.token_id_yes
                        if is_yes:
                            position.yes_inventory += order.size
                            position.yes_entry_price = new_midpoint
                        else:
                            position.no_inventory += order.size
                            position.no_entry_price = new_midpoint
                        label = "YES" if is_yes else "NO"
                        logger.info(
                            f"FILL (during replace): BUY {label} "
                            f"{order.size}@{order.price:.4f} for {condition_id[:16]}..."
                        )
        except Exception as e:
            logger.warning(f"Failed to check fills before replace: {e}")

        # Snapshot inventory (including any just-detected fills)
        yes_inv = position.yes_inventory
        no_inv = position.no_inventory
        yes_entry = position.yes_entry_price
        no_entry = position.no_entry_price

        self.cancel_market_orders(condition_id)

        buy_yes_price, buy_no_price = self.calculate_order_prices(
            new_midpoint, position.max_spread, position.tick_size
        )

        no_midpoint = 1.0 - new_midpoint
        if buy_yes_price >= new_midpoint or buy_no_price >= no_midpoint:
            logger.warning(
                f"Invalid replacement prices for {condition_id}: "
                f"buy_yes={buy_yes_price}, buy_no={buy_no_price}, mid={new_midpoint}"
            )
            return None

        now = time.time()
        new_position = MarketPosition(
            condition_id=condition_id,
            token_id_yes=position.token_id_yes,
            token_id_no=position.token_id_no,
            max_spread=position.max_spread,
            min_size=position.min_size,
            tick_size=position.tick_size,
            last_midpoint=new_midpoint,
            yes_inventory=yes_inv,
            no_inventory=no_inv,
            yes_entry_price=yes_entry,
            no_entry_price=no_entry,
            # Preserve cooldown state across order replacements
            yes_fill_times=position.yes_fill_times,
            no_fill_times=position.no_fill_times,
            yes_last_sell_fill=position.yes_last_sell_fill,
            no_last_sell_fill=position.no_last_sell_fill,
            yes_blocked=position.yes_blocked,
            no_blocked=position.no_blocked,
        )

        # Only place BUY for sides with no inventory AND not in cooldown
        # Also respect global circuit breaker pause
        global_paused = self.is_global_paused
        yes_in_cooldown = (
            global_paused
            or position.yes_blocked
            or (position.yes_last_sell_fill > 0
                and now - position.yes_last_sell_fill < config.FILL_COOLDOWN_SECONDS)
        )
        no_in_cooldown = (
            global_paused
            or position.no_blocked
            or (position.no_last_sell_fill > 0
                and now - position.no_last_sell_fill < config.FILL_COOLDOWN_SECONDS)
        )

        buy_size = position.min_size * get_size_multiplier()

        if yes_inv == 0 and not yes_in_cooldown:
            buy_yes_order = self._place_order(
                position.token_id_yes, buy_yes_price, buy_size,
                BUY, condition_id, new_midpoint,
            )
            if buy_yes_order:
                self._track_order(new_position, buy_yes_order)
        elif yes_in_cooldown and yes_inv == 0:
            logger.debug(f"Skipping BUY YES for {condition_id[:16]}... (cooldown)")

        if no_inv == 0 and not no_in_cooldown:
            buy_no_order = self._place_order(
                position.token_id_no, buy_no_price, buy_size,
                BUY, condition_id, new_midpoint,
            )
            if buy_no_order:
                self._track_order(new_position, buy_no_order)
        elif no_in_cooldown and no_inv == 0:
            logger.debug(f"Skipping BUY NO for {condition_id[:16]}... (cooldown)")

        # If holding shares, wait for conditional token to unlock after cancel
        if yes_inv > 0 or no_inv > 0:
            time.sleep(1.5)

        # If holding Yes shares, place SELL Yes to unwind (at new midpoint)
        if yes_inv > 0:
            sell_yes_price = self._calculate_sell_price(
                new_midpoint, position.max_spread, position.tick_size, is_yes=True
            )
            sell_yes = self._place_order(
                position.token_id_yes, sell_yes_price, yes_inv,
                SELL, condition_id, new_midpoint,
                min_order_size=position.min_size,
            )
            if sell_yes:
                self._track_order(new_position, sell_yes)
                logger.info(f"  + SELL_YES@{sell_yes_price:.4f} x{yes_inv} (unwind)")

        # If holding No shares, place SELL No to unwind
        if no_inv > 0:
            sell_no_price = self._calculate_sell_price(
                new_midpoint, position.max_spread, position.tick_size, is_yes=False
            )
            sell_no = self._place_order(
                position.token_id_no, sell_no_price, no_inv,
                SELL, condition_id, new_midpoint,
                min_order_size=position.min_size,
            )
            if sell_no:
                self._track_order(new_position, sell_no)
                logger.info(f"  + SELL_NO@{sell_no_price:.4f} x{no_inv} (unwind)")

        if new_position.orders:
            self.positions[condition_id] = new_position
            logger.info(
                f"Replaced orders for {condition_id[:16]}... | "
                f"new mid={new_midpoint:.4f} BUY_YES@{buy_yes_price:.4f} BUY_NO@{buy_no_price:.4f}"
            )
        return new_position

    def reprice_sell_orders(
        self, condition_id: str, new_midpoint: float
    ) -> bool:
        """Cancel and re-place only SELL orders at an updated midpoint.

        Used when drift is detected on a SELL-only position (during cooldown).
        Does NOT touch BUY orders or place new BUYs.
        """
        position = self.positions.get(condition_id)
        if not position:
            return False

        sell_orders = [o for o in position.orders if o.side == SELL]
        if not sell_orders:
            return False

        # Cancel existing SELL orders
        for order in sell_orders:
            try:
                safe_api_call(self.client.cancel, order.order_id)
                logger.info(f"Cancelled SELL order {order.order_id[:16]}... for reprice")
            except Exception as e:
                logger.error(f"Failed to cancel SELL {order.order_id[:16]}...: {e}")

        # Remove cancelled SELLs from tracking
        position.orders = [o for o in position.orders if o.side != SELL]
        position.last_midpoint = new_midpoint

        # Wait for conditional token to unlock after cancel (prevents "not enough balance")
        time.sleep(1.5)

        # Reset sell fail counters — this is a fresh placement, not a retry
        self._sell_fail_counts.pop((condition_id, position.token_id_yes), None)
        self._sell_fail_counts.pop((condition_id, position.token_id_no), None)

        placed = False

        # Re-place SELL for Yes inventory
        if position.yes_inventory > 0:
            sell_price = self._calculate_sell_price(
                new_midpoint, position.max_spread, position.tick_size, is_yes=True
            )
            sell_order = self._place_order(
                position.token_id_yes, sell_price, position.yes_inventory,
                SELL, condition_id, new_midpoint,
                min_order_size=position.min_size,
            )
            if sell_order:
                self._track_order(position, sell_order)
                logger.info(
                    f"Repriced SELL YES@{sell_price:.4f} x{position.yes_inventory} "
                    f"for {condition_id[:16]}..."
                )
                placed = True

        # Re-place SELL for No inventory
        if position.no_inventory > 0:
            sell_price = self._calculate_sell_price(
                new_midpoint, position.max_spread, position.tick_size, is_yes=False
            )
            sell_order = self._place_order(
                position.token_id_no, sell_price, position.no_inventory,
                SELL, condition_id, new_midpoint,
                min_order_size=position.min_size,
            )
            if sell_order:
                self._track_order(position, sell_order)
                logger.info(
                    f"Repriced SELL NO@{sell_price:.4f} x{position.no_inventory} "
                    f"for {condition_id[:16]}..."
                )
                placed = True

        return placed

    def sync_with_exchange(self) -> list[FillEvent]:
        """Reconcile local state with exchange. Detect and return filled orders.

        Safeguards against phantom fills:
        - Orders placed < 30s ago are given a grace period (API propagation delay)
        - If ALL tracked orders vanish at once, treat it as an API error
        """
        fills = []
        now = time.time()
        grace_period = config.ORDER_GRACE_PERIOD_SECONDS

        try:
            open_orders = safe_api_call(self.client.get_orders)
            if open_orders is None:
                return fills

            # Build set of order IDs that are still live on exchange
            live_ids = set()
            if isinstance(open_orders, list):
                for o in open_orders:
                    oid = o.get("id") if isinstance(o, dict) else getattr(o, "id", None)
                    if oid:
                        live_ids.add(oid)

            # Count total tracked orders to detect bulk disappearance
            total_tracked = sum(len(p.orders) for p in self.positions.values())

            # Detect filled orders
            missing_count = 0
            for cid, position in list(self.positions.items()):
                surviving = []
                newly_missing = []
                for order in position.orders:
                    if order.order_id in live_ids:
                        surviving.append(order)
                    elif now - order.placed_at < grace_period:
                        # Recently placed — keep it, don't treat as fill yet
                        surviving.append(order)
                        logger.debug(
                            f"Order {order.order_id[:16]}... not yet visible "
                            f"({now - order.placed_at:.0f}s old), keeping"
                        )
                    else:
                        newly_missing.append(order)
                        missing_count += 1

                # If ALL tracked orders across ALL markets vanish, it's an API error
                if missing_count >= total_tracked and total_tracked > 2:
                    logger.warning(
                        f"ALL {total_tracked} tracked orders missing from exchange — "
                        f"likely API error, skipping fill detection this cycle"
                    )
                    return []

                for order in newly_missing:
                    # Verify fill via get_order() — prevents phantom fills
                    status = self._get_order_status(order.order_id)
                    if status == "LIVE":
                        surviving.append(order)  # Still active, get_orders() missed it
                        continue
                    if status == "CANCELLED":
                        continue  # Dead order, not a fill — just drop it
                    # MATCHED or UNKNOWN (conservative: treat as fill)
                    is_yes = order.token_id == position.token_id_yes
                    token_label = "YES" if is_yes else "NO"
                    logger.info(
                        f"FILL: {order.side} {token_label} {order.size}@{order.price:.4f} "
                        f"for {cid[:16]}..."
                    )
                    fills.append(FillEvent(
                        token_id=order.token_id,
                        side=order.side,
                        price=order.price,
                        size=order.size,
                        condition_id=cid,
                    ))
                position.orders = surviving

        except Exception as e:
            logger.warning(f"Failed to sync with exchange: {e}")

        return fills

    def handle_filled_orders(self, fills: list[FillEvent]):
        """Process fills: on BUY fill place SELL unwind only; on SELL fill re-place BUY.

        Order lifecycle per side:
          BUY fills  -> hold shares -> place SELL unwind (do NOT re-place BUY)
          SELL fills -> unwind done -> re-place BUY to resume earning rewards
        Max orders per market = 3 (2 BUYs + 1 SELL unwind).

        Fills are aggregated per (condition_id, token_id, side) before processing
        to avoid placing duplicate SELL orders for the same shares.
        """
        # --- Aggregate fills per (condition_id, token_id, side) ---
        aggregated: dict[tuple[str, str, str], float] = {}
        for fill in fills:
            key = (fill.condition_id, fill.token_id, fill.side)
            aggregated[key] = aggregated.get(key, 0) + fill.size

        for (cid, token_id, side), total_size in aggregated.items():
            position = self.positions.get(cid)
            if not position:
                continue

            is_yes = token_id == position.token_id_yes
            token_label = "YES" if is_yes else "NO"

            # Get fresh midpoint
            mid = self._get_current_midpoint(position.token_id_yes)
            if mid is None:
                mid = position.last_midpoint
            position.last_midpoint = mid

            if side == BUY:
                # BUY filled -> cancel all remaining BUYs, update inventory, place SELL
                if is_yes:
                    position.yes_inventory += total_size
                    position.yes_entry_price = mid
                    position.yes_fill_times.append(time.time())
                else:
                    position.no_inventory += total_size
                    position.no_entry_price = mid
                    position.no_fill_times.append(time.time())

                inv = position.yes_inventory if is_yes else position.no_inventory
                logger.info(
                    f"Inventory update: {token_label} = {inv} shares "
                    f"(+{total_size} from fill)"
                )

                # Cancel ALL remaining BUY orders for this market (prevent double fill)
                buy_orders = [o for o in position.orders if o.side == BUY]
                for order in buy_orders:
                    try:
                        safe_api_call(self.client.cancel, order.order_id)
                    except Exception:
                        pass
                position.orders = [o for o in position.orders if o.side != BUY]

                # GLOBAL CIRCUIT BREAKER: Cancel ALL BUYs across ALL markets
                now = time.time()
                if getattr(config, "GLOBAL_CIRCUIT_BREAKER", False):
                    self._last_global_fill = now
                    self.cancel_all_buys(
                        f"triggered by REST BUY {token_label} fill in {cid[:16]}..."
                    )

                # Check for runaway fill pattern
                now = time.time()
                fill_times = position.yes_fill_times if is_yes else position.no_fill_times
                recent = [t for t in fill_times if now - t < config.FILL_COOLDOWN_SECONDS]
                if len(recent) >= config.MAX_FILLS_BEFORE_BLOCK:
                    if is_yes:
                        position.yes_blocked = True
                    else:
                        position.no_blocked = True
                    logger.warning(
                        f"BLOCKED: {token_label} side of {cid[:16]}... "
                        f"({len(recent)} fills in {config.FILL_COOLDOWN_SECONDS}s)"
                    )

                # Skip SELL if one already exists for this side
                has_sell = any(
                    o.side == SELL and o.token_id == token_id
                    for o in position.orders
                )
                if has_sell:
                    logger.info(
                        f"SELL already pending for {token_label}, skipping duplicate"
                    )
                    continue

                # Place single SELL to unwind total filled amount
                sell_price = self._calculate_sell_price(
                    mid, position.max_spread, position.tick_size, is_yes
                )
                sell_order = self._place_order(
                    token_id, sell_price, inv,
                    SELL, cid, mid,
                    min_order_size=position.min_size,
                )
                if sell_order:
                    self._track_order(position, sell_order)
                    logger.info(
                        f"Placed unwind SELL {token_label}@{sell_price:.4f} "
                        f"x{inv} for {cid[:16]}..."
                    )

            elif side == SELL:
                # SELL filled -> unwind complete. Record cooldown, do NOT re-place BUY.
                if is_yes:
                    position.yes_inventory = max(0, position.yes_inventory - total_size)
                    position.yes_entry_price = 0.0
                    position.yes_last_sell_fill = time.time()
                else:
                    position.no_inventory = max(0, position.no_inventory - total_size)
                    position.no_entry_price = 0.0
                    position.no_last_sell_fill = time.time()

                inv = position.yes_inventory if is_yes else position.no_inventory
                logger.info(
                    f"Unwind complete: SELL {token_label} filled | "
                    f"inventory now {inv} shares | "
                    f"cooldown {config.FILL_COOLDOWN_SECONDS}s before re-entry"
                )
                # DO NOT re-place BUY. process_cooldown_reentries() handles it.

        # Clean up positions with no orders and no inventory
        for cid in list(self.positions.keys()):
            pos = self.positions[cid]
            if not pos.orders and pos.yes_inventory == 0 and pos.no_inventory == 0:
                logger.info(f"Position fully closed for {cid[:16]}...")
                del self.positions[cid]

    def retry_pending_sells(self):
        """Retry placing SELL orders for positions holding inventory without a pending SELL.

        Handles the case where a BUY fill was detected but the SELL failed
        (e.g. settlement delay — shares not yet in wallet). Called every loop
        iteration so it retries until the shares settle and the SELL succeeds.

        After MAX_SELL_RETRIES consecutive failures, assumes the inventory is
        phantom (fill detection was wrong or shares already sold) and resets
        to 0. Adds token to _phantom_tokens to block reconciliation from
        re-creating the phantom entry from stale data API.
        """
        for cid, position in list(self.positions.items()):
            for is_yes, token_id, inv_attr in [
                (True, position.token_id_yes, "yes_inventory"),
                (False, position.token_id_no, "no_inventory"),
            ]:
                inv = getattr(position, inv_attr)
                if inv <= 0:
                    continue

                # Check if a SELL already exists for this side
                has_sell = any(
                    o.side == SELL and o.token_id == token_id
                    for o in position.orders
                )
                if has_sell:
                    continue

                fail_key = (cid, token_id)
                fail_count = self._sell_fail_counts.get(fail_key, 0)

                # After max retries, verify on-chain before resetting.
                # The shares may actually exist but the SELL keeps failing due to
                # timing (e.g. cancel didn't propagate). Don't lose real inventory.
                if fail_count >= config.MAX_SELL_RETRIES:
                    label = "YES" if is_yes else "NO"
                    # Check actual on-chain balance before declaring phantom
                    actual_on_chain = 0.0
                    try:
                        r = requests.get(
                            f"https://data-api.polymarket.com/positions"
                            f"?user={config.WALLET_ADDRESS.lower()}",
                            timeout=10,
                        )
                        for p in r.json():
                            if p.get("asset", "") == token_id:
                                actual_on_chain = float(p.get("size", 0))
                                break
                    except Exception:
                        pass

                    if actual_on_chain > 0.5:
                        # Shares ARE on-chain — not phantom. Reset counter and
                        # wait for next cycle (shares may need more time to unlock).
                        logger.warning(
                            f"SELL failed {fail_count}x for {label} {cid[:16]}... but "
                            f"{actual_on_chain} shares confirmed on-chain — keeping "
                            f"(resetting retry counter)"
                        )
                        self._sell_fail_counts[fail_key] = 0
                        # Update tracked inventory to match on-chain reality
                        setattr(position, inv_attr, actual_on_chain)
                        continue
                    else:
                        # Truly phantom — no shares on-chain. Safe to reset.
                        logger.warning(
                            f"SELL failed {fail_count}x for {label} {cid[:16]}... — "
                            f"confirmed 0 shares on-chain, resetting phantom "
                            f"inventory ({inv} -> 0)"
                        )
                        setattr(position, inv_attr, 0)
                        if is_yes:
                            position.yes_entry_price = 0.0
                        else:
                            position.no_entry_price = 0.0
                        self._sell_fail_counts.pop(fail_key, None)
                        self._phantom_tokens.add(token_id)
                        continue

                # Throttle retries: only attempt once per rescan cycle (~3 min)
                # to avoid spamming the API for positions that can't sell
                last_attempt_key = ("_last_retry", cid, token_id)
                last_attempt = self._sell_fail_counts.get(last_attempt_key, 0)
                if time.time() - last_attempt < config.RESCAN_INTERVAL_SECONDS:
                    continue
                self._sell_fail_counts[last_attempt_key] = time.time()

                # No pending SELL for held inventory — retry
                label = "YES" if is_yes else "NO"
                mid = self._get_current_midpoint(position.token_id_yes)
                if mid is None:
                    mid = position.last_midpoint

                sell_price = self._calculate_sell_price(
                    mid, position.max_spread, position.tick_size, is_yes
                )
                logger.info(
                    f"Retrying SELL {label}@{sell_price:.4f} x{inv} "
                    f"for {cid[:16]}... (attempt {fail_count + 1}/{config.MAX_SELL_RETRIES})"
                )
                sell_order = self._place_order(
                    token_id, sell_price, inv,
                    SELL, cid, mid,
                    min_order_size=position.min_size,
                )
                if sell_order:
                    self._track_order(position, sell_order)
                    self._sell_fail_counts.pop(fail_key, None)
                    logger.info(
                        f"SELL retry succeeded: {label}@{sell_price:.4f} "
                        f"x{inv} for {cid[:16]}..."
                    )
                else:
                    self._sell_fail_counts[fail_key] = fail_count + 1

        # Clean up positions with no orders and no inventory (phantom reset may have cleared them)
        for cid in list(self.positions.keys()):
            pos = self.positions[cid]
            if not pos.orders and pos.yes_inventory == 0 and pos.no_inventory == 0:
                logger.info(f"Position fully cleared for {cid[:16]}... (phantom inventory reset)")
                del self.positions[cid]

    def handle_ws_fill(
        self, order_id: str, asset_id: str, side: str,
        size_matched: float, price: float,
    ) -> None:
        """Process a single fill event from WebSocket.

        Receives exact fill data (order_id, size_matched) — no inference needed.
        On BUY fill: cancel all BUYs, update inventory, place SELL, no re-buy.
        On SELL fill: reduce inventory, record cooldown, no re-buy.

        IMPORTANT: The WS trade event's 'side' field is unreliable for
        determining maker side. We use the tracked order's actual side instead.
        """
        # Find which position owns this order
        position = None
        cid = None
        matched_order = None
        for c, pos in self.positions.items():
            for order in pos.orders:
                if order.order_id == order_id:
                    position = pos
                    cid = c
                    matched_order = order
                    break
            if position:
                break

        if not position or not matched_order:
            # Order not tracked — likely a recently-cancelled SELL that filled
            # before the cancel propagated (race condition in reprice_sell_if_stale).
            # Match by asset_id to the correct position and reduce inventory.
            for c, pos in self.positions.items():
                for is_yes_check, tid in [(True, pos.token_id_yes), (False, pos.token_id_no)]:
                    if tid == asset_id:
                        inv_attr = "yes_inventory" if is_yes_check else "no_inventory"
                        inv = getattr(pos, inv_attr)
                        if inv > 0:
                            new_inv = max(0, inv - size_matched)
                            setattr(pos, inv_attr, new_inv)
                            token_label = "YES" if is_yes_check else "NO"
                            if new_inv == 0:
                                if is_yes_check:
                                    pos.yes_entry_price = 0.0
                                    pos.yes_last_sell_fill = time.time()
                                else:
                                    pos.no_entry_price = 0.0
                                    pos.no_last_sell_fill = time.time()
                            logger.info(
                                f"WS FILL (untracked order): {token_label} "
                                f"{size_matched}@{price:.4f} for {c[:16]}... | "
                                f"inventory {inv:.2f} -> {new_inv:.2f}"
                            )
                            # Remove any stale SELL orders for this token
                            pos.orders = [
                                o for o in pos.orders
                                if not (o.side == SELL and o.token_id == tid)
                            ]
                            # Clean up empty position
                            if (not pos.orders and pos.yes_inventory == 0
                                    and pos.no_inventory == 0):
                                del self.positions[c]
                                logger.info(f"Position fully closed for {c[:16]}...")
                            return
            logger.warning(f"WS fill for unknown order {order_id[:16]}... — ignoring")
            return

        # Use the tracked order's side — the WS event's inferred side is unreliable
        # (Polymarket trade event 'side' is maker's side, not taker's)
        actual_side = matched_order.side
        is_yes = matched_order.token_id == position.token_id_yes
        token_label = "YES" if is_yes else "NO"
        now = time.time()

        # Handle partial vs full fill:
        # If size_matched < order size, the order is still live for the remainder.
        # Update tracked size instead of removing the order entirely.
        remaining = matched_order.size - size_matched
        if remaining > 0.001:  # still partially live (>0.001 to avoid float dust)
            matched_order.size = remaining
            logger.info(
                f"Partial fill: {actual_side} {token_label} "
                f"{size_matched}/{size_matched + remaining:.2f} filled, "
                f"{remaining:.2f} remaining on exchange"
            )
        else:
            # Fully filled — remove from tracking
            position.orders = [o for o in position.orders if o.order_id != order_id]

        if actual_side == BUY:
            logger.info(
                f"WS FILL: BUY {token_label} {size_matched}@{price:.4f} "
                f"for {cid[:16]}..."
            )

            # Cancel ALL remaining BUY orders for this market immediately
            buy_orders = [o for o in position.orders if o.side == BUY]
            for order in buy_orders:
                try:
                    safe_api_call(self.client.cancel, order.order_id)
                except Exception:
                    pass
            position.orders = [o for o in position.orders if o.side != BUY]

            # GLOBAL CIRCUIT BREAKER: Cancel ALL BUYs across ALL markets
            if getattr(config, "GLOBAL_CIRCUIT_BREAKER", False):
                self._last_global_fill = now
                self.cancel_all_buys(
                    f"triggered by BUY {token_label} fill in {cid[:16]}..."
                )

            # Blacklist this market to prevent re-entry
            self.blacklist_market(cid)

            # Update inventory with exact matched size (handles partial fills)
            if is_yes:
                position.yes_inventory += size_matched
                position.yes_entry_price = price
                position.yes_fill_times.append(now)
            else:
                position.no_inventory += size_matched
                position.no_entry_price = price
                position.no_fill_times.append(now)

            # Check for runaway fill pattern
            fill_times = position.yes_fill_times if is_yes else position.no_fill_times
            recent = [t for t in fill_times if now - t < config.FILL_COOLDOWN_SECONDS]
            if len(recent) >= config.MAX_FILLS_BEFORE_BLOCK:
                if is_yes:
                    position.yes_blocked = True
                else:
                    position.no_blocked = True
                logger.warning(
                    f"BLOCKED: {token_label} side of {cid[:16]}... "
                    f"({len(recent)} fills in {config.FILL_COOLDOWN_SECONDS}s)"
                )

            # Skip SELL if one already exists for this side
            token_id = matched_order.token_id
            has_sell = any(
                o.side == SELL and o.token_id == token_id
                for o in position.orders
            )
            if has_sell:
                logger.info(f"SELL already pending for {token_label}, skipping duplicate")
                return

            # Place SELL to unwind
            mid = self._get_current_midpoint(position.token_id_yes)
            if mid is None:
                mid = position.last_midpoint
            position.last_midpoint = mid

            inv = position.yes_inventory if is_yes else position.no_inventory
            sell_price = self._calculate_sell_price(
                mid, position.max_spread, position.tick_size, is_yes
            )
            sell_order = self._place_order(
                token_id, sell_price, inv,
                SELL, cid, mid,
                min_order_size=position.min_size,
            )
            if sell_order:
                self._track_order(position, sell_order)
                logger.info(
                    f"Placed unwind SELL {token_label}@{sell_price:.4f} "
                    f"x{inv} for {cid[:16]}..."
                )

        elif actual_side == SELL:
            logger.info(
                f"WS FILL: SELL {token_label} {size_matched}@{price:.4f} "
                f"for {cid[:16]}..."
            )

            if is_yes:
                position.yes_inventory = max(0, position.yes_inventory - size_matched)
                if position.yes_inventory == 0:
                    position.yes_entry_price = 0.0
                    position.yes_last_sell_fill = now
            else:
                position.no_inventory = max(0, position.no_inventory - size_matched)
                if position.no_inventory == 0:
                    position.no_entry_price = 0.0
                    position.no_last_sell_fill = now

            inv = position.yes_inventory if is_yes else position.no_inventory
            if inv == 0:
                logger.info(
                    f"Unwind complete: SELL {token_label} fully filled | "
                    f"cooldown {config.FILL_COOLDOWN_SECONDS}s before re-entry"
                )
            else:
                logger.info(
                    f"Partial SELL {token_label} filled ({size_matched} sold) | "
                    f"inventory now {inv} shares (SELL order still live)"
                )
            # DO NOT re-place BUY. process_cooldown_reentries() handles it.

        # Clean up empty positions
        if (not position.orders
                and position.yes_inventory == 0
                and position.no_inventory == 0):
            del self.positions[cid]
            logger.info(f"Position fully closed for {cid[:16]}...")

    def process_cooldown_reentries(self) -> None:
        """Re-place BUY orders for sides whose cooldown has expired.

        For each position side where:
        - SELL fill happened > FILL_COOLDOWN_SECONDS ago
        - No inventory, no existing BUY, side not blocked
        - Global circuit breaker not active
        Place a fresh BUY order.
        """
        # Global circuit breaker: don't re-enter any market during pause
        if self.is_global_paused:
            return

        now = time.time()

        for cid, position in list(self.positions.items()):
            # Skip blacklisted markets entirely
            if self.is_blacklisted(cid):
                continue

            mid = None  # lazy fetch

            for is_yes, token_id, inv_attr, sell_fill_attr, blocked_attr in [
                (True, position.token_id_yes, "yes_inventory",
                 "yes_last_sell_fill", "yes_blocked"),
                (False, position.token_id_no, "no_inventory",
                 "no_last_sell_fill", "no_blocked"),
            ]:
                last_sell = getattr(position, sell_fill_attr)
                if last_sell == 0:
                    continue  # never had a SELL fill on this side
                if getattr(position, blocked_attr):
                    continue  # side blocked due to runaway fills
                if getattr(position, inv_attr) > 0:
                    continue  # still holding shares
                if now - last_sell < config.FILL_COOLDOWN_SECONDS:
                    continue  # still in cooldown

                # Check no BUY already exists for this side
                has_buy = any(
                    o.side == BUY and o.token_id == token_id
                    for o in position.orders
                )
                if has_buy:
                    continue

                # Cooldown expired — safe to re-enter
                if mid is None:
                    mid = self._get_current_midpoint(position.token_id_yes)
                    if mid is None:
                        mid = position.last_midpoint

                buy_yes_price, buy_no_price = self.calculate_order_prices(
                    mid, position.max_spread, position.tick_size
                )
                buy_price = buy_yes_price if is_yes else buy_no_price
                label = "YES" if is_yes else "NO"

                reentry_size = position.min_size * get_size_multiplier()
                buy_order = self._place_order(
                    token_id, buy_price, reentry_size,
                    BUY, cid, mid,
                )
                if buy_order:
                    self._track_order(position, buy_order)
                    setattr(position, sell_fill_attr, 0.0)
                    logger.info(
                        f"Cooldown expired: re-placed BUY {label}@{buy_price:.4f} "
                        f"for {cid[:16]}... (waited {now - last_sell:.0f}s)"
                    )

    def reprice_sell_if_stale(
        self, condition_id: str, new_midpoint: float,
        best_asks: dict[str, float] | None = None,
    ) -> bool:
        """Reprice SELL orders immediately when WS detects a price change.

        Uses best_ask from WS to undercut competition: places SELL at
        best_ask - 1 tick (ahead of all other sellers). Falls back to
        midpoint-based pricing if best_ask is unavailable or is our own order.

        Only reprices if the target price differs from current SELL price
        by at least 1 tick (avoids unnecessary cancel+replace churn).
        No time-based throttle — reacts to every meaningful WS update.

        Returns True if any SELL was repriced or placed.
        """
        position = self.positions.get(condition_id)
        if not position:
            return False

        repriced = False
        for is_yes, token_id, inv_attr in [
            (True, position.token_id_yes, "yes_inventory"),
            (False, position.token_id_no, "no_inventory"),
        ]:
            inv = getattr(position, inv_attr)
            if inv <= 0:
                continue

            # Base price: at or below midpoint (round_price_down)
            target_price = self._calculate_sell_price(
                new_midpoint, position.max_spread, position.tick_size, is_yes
            )

            existing_sell = next(
                (o for o in position.orders if o.side == SELL and o.token_id == token_id),
                None,
            )

            # Undercut best_ask from WS if available
            if best_asks:
                ws_best_ask = best_asks.get(token_id)
                if ws_best_ask and ws_best_ask > position.tick_size * 2:
                    # Don't undercut our own order (best_ask == our sell price)
                    is_our_order = (
                        existing_sell is not None
                        and abs(ws_best_ask - existing_sell.price) < position.tick_size
                    )
                    if not is_our_order:
                        undercut = round_price_down(
                            ws_best_ask - position.tick_size, position.tick_size
                        )
                        if undercut > 0 and undercut < target_price:
                            target_price = clamp_price(undercut, position.tick_size)

            if existing_sell is None:
                # No SELL exists — place one
                sell_order = self._place_order(
                    token_id, target_price, inv,
                    SELL, condition_id, new_midpoint,
                    min_order_size=position.min_size,
                )
                if sell_order:
                    self._track_order(position, sell_order)
                    label = "YES" if is_yes else "NO"
                    logger.info(
                        f"WS: placed SELL {label}@{target_price:.4f} x{inv} "
                        f"for {condition_id[:16]}..."
                    )
                    repriced = True
                continue

            # SELL exists — only reprice if target price differs by >= 1 tick
            if abs(existing_sell.price - target_price) < position.tick_size:
                continue

            # Cancel old SELL and re-place at new price
            label = "YES" if is_yes else "NO"
            try:
                safe_api_call(self.client.cancel, existing_sell.order_id)
            except Exception:
                pass
            position.orders = [o for o in position.orders if o.order_id != existing_sell.order_id]
            time.sleep(0.5)  # brief wait for token unlock (aggressive)

            sell_order = self._place_order(
                token_id, target_price, inv,
                SELL, condition_id, new_midpoint,
                min_order_size=position.min_size,
            )
            if sell_order:
                self._track_order(position, sell_order)
                logger.info(
                    f"WS reprice SELL {label}: {existing_sell.price:.4f} -> "
                    f"{target_price:.4f} x{inv} for {condition_id[:16]}..."
                )
                repriced = True

        return repriced

    def get_all_subscribed_ids(self) -> tuple[set[str], set[str]]:
        """Return (token_ids, condition_ids) for all active positions.

        token_ids: for market WS channel (both Yes and No tokens)
        condition_ids: for user WS channel
        """
        token_ids = set()
        condition_ids = set()
        for cid, pos in self.positions.items():
            condition_ids.add(cid)
            token_ids.add(pos.token_id_yes)
            token_ids.add(pos.token_id_no)
        return token_ids, condition_ids

    def get_active_condition_ids(self) -> list[str]:
        return list(self.positions.keys())
