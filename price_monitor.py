import config
from order_manager import OrderManager
from utils import setup_logger, safe_api_call
from py_clob_client.client import ClobClient

logger = setup_logger("PriceMonitor")


class PriceMonitor:
    def __init__(self, client: ClobClient, order_manager: OrderManager):
        self.client = client
        self.order_manager = order_manager
        self.last_midpoints: dict[str, float] = {}  # condition_id -> midpoint
        self.last_best_asks: dict[str, float] = {}  # token_id -> best_ask (from WS)

    def get_current_midpoint(self, token_id: str) -> float | None:
        """Fetch current midpoint from CLOB."""
        try:
            resp = safe_api_call(self.client.get_midpoint, token_id)
            if isinstance(resp, dict):
                mid = float(resp.get("mid", 0))
            else:
                mid = float(resp)
            # Reject nonsensical midpoints (API glitch / manipulation)
            if mid <= 0 or mid >= 1:
                logger.warning(f"Rejecting invalid midpoint {mid} for {token_id}")
                return None
            return mid
        except Exception as e:
            logger.warning(f"Failed to get midpoint for {token_id}: {e}")
            return None

    def check_all_positions(self) -> tuple[list[str], list[str], list[str]]:
        """
        Check midpoints for all active positions.
        Returns (drifted, stop_losses, sell_reprices):
        - drifted: condition_ids needing re-adjustment at new midpoint
        - stop_losses: condition_ids needing force-exit (no new BUYs)
        - sell_reprices: condition_ids with inventory needing immediate SELL reprice
        """
        drifted = []
        stop_losses = []
        sell_reprices = []

        for cid, position in self.order_manager.positions.items():
            midpoint = self.get_current_midpoint(position.token_id_yes)
            if midpoint is None:
                continue

            self.last_midpoints[cid] = midpoint

            # Check if we should exit (extreme prices)
            if midpoint < config.MIN_MIDPOINT or midpoint > config.MAX_MIDPOINT:
                logger.warning(
                    f"Market {cid} midpoint at extreme ({midpoint:.4f}), flagging for exit"
                )
                stop_losses.append(cid)
                continue

            # Check stop-loss: if price moved too far against any held position
            stop_loss_threshold = max(
                position.max_spread * config.STOP_LOSS_FRACTION,
                config.MIN_STOP_LOSS,
            )
            stop_loss_hit = False
            if position.yes_inventory > 0 and position.yes_entry_price > 0:
                loss = position.yes_entry_price - midpoint
                if loss >= stop_loss_threshold:
                    logger.warning(
                        f"STOP-LOSS {cid[:16]}... YES: entry={position.yes_entry_price:.4f} "
                        f"now={midpoint:.4f} loss={loss:.4f} threshold={stop_loss_threshold:.4f}"
                    )
                    stop_loss_hit = True
            if position.no_inventory > 0 and position.no_entry_price > 0:
                no_mid = 1.0 - midpoint
                loss = position.no_entry_price - no_mid
                if loss >= stop_loss_threshold:
                    logger.warning(
                        f"STOP-LOSS {cid[:16]}... NO: entry={position.no_entry_price:.4f} "
                        f"now={no_mid:.4f} loss={loss:.4f} threshold={stop_loss_threshold:.4f}"
                    )
                    stop_loss_hit = True

            if stop_loss_hit:
                stop_losses.append(cid)
                continue

            # Check drift (proportional to market's max_spread)
            drift = abs(midpoint - position.last_midpoint)
            drift_threshold = max(
                position.max_spread * config.DRIFT_THRESHOLD_FRACTION,
                config.MIN_DRIFT_THRESHOLD,
            )
            if drift > drift_threshold:
                logger.info(
                    f"Drift detected for {cid}: "
                    f"{position.last_midpoint:.4f} -> {midpoint:.4f} "
                    f"(delta={drift:.4f})"
                )
                drifted.append(cid)
            elif position.yes_inventory > 0 or position.no_inventory > 0:
                # Sub-drift move but has inventory — reprice SELL aggressively
                sell_reprices.append(cid)

        return drifted, stop_losses, sell_reprices

    def update_midpoints_from_ws(self, price_events: list) -> tuple[list[str], list[str], list[str]]:
        """Process WebSocket price events and detect drift/stop-loss/sell-reprice.

        Replaces per-market get_midpoint() polling when WS is connected.
        Multiple events for the same asset are coalesced (latest wins).

        Returns (drifted, stop_losses, sell_reprices):
        - sell_reprices: inventory positions with sub-drift price changes
          needing immediate SELL repricing at new midpoint.
        """
        # Coalesce: keep only the latest event per asset_id
        latest: dict[str, object] = {}
        for event in price_events:
            latest[event.asset_id] = event

        if not latest:
            return [], [], []

        # Map token_id -> (condition_id, is_yes)
        token_to_position: dict[str, tuple[str, bool]] = {}
        for cid, position in self.order_manager.positions.items():
            token_to_position[position.token_id_yes] = (cid, True)
            token_to_position[position.token_id_no] = (cid, False)

        # Compute midpoints from WS data and store best_asks per token
        updated_mids: dict[str, float] = {}
        for asset_id, event in latest.items():
            info = token_to_position.get(asset_id)
            if not info:
                continue
            cid, is_yes = info

            mid = event.midpoint
            if mid <= 0 or mid >= 1:
                continue

            # Store best_ask per token for SELL undercutting
            if event.best_ask > 0:
                self.last_best_asks[asset_id] = event.best_ask

            if is_yes:
                updated_mids[cid] = mid
            else:
                # No token midpoint: Yes midpoint = 1 - No midpoint
                updated_mids[cid] = 1.0 - mid

        # Run drift/stop-loss/sell-reprice detection for updated markets only
        drifted = []
        stop_losses = []
        sell_reprices = []

        for cid, midpoint in updated_mids.items():
            position = self.order_manager.positions.get(cid)
            if not position:
                continue

            self.last_midpoints[cid] = midpoint

            # Extreme price check
            if midpoint < config.MIN_MIDPOINT or midpoint > config.MAX_MIDPOINT:
                logger.warning(
                    f"WS: {cid[:16]}... midpoint at extreme ({midpoint:.4f})"
                )
                stop_losses.append(cid)
                continue

            # Stop-loss check
            stop_loss_threshold = max(
                position.max_spread * config.STOP_LOSS_FRACTION,
                config.MIN_STOP_LOSS,
            )
            stop_loss_hit = False
            if position.yes_inventory > 0 and position.yes_entry_price > 0:
                loss = position.yes_entry_price - midpoint
                if loss >= stop_loss_threshold:
                    logger.warning(f"WS STOP-LOSS {cid[:16]}... YES: loss={loss:.4f}")
                    stop_loss_hit = True
            if position.no_inventory > 0 and position.no_entry_price > 0:
                no_mid = 1.0 - midpoint
                loss = position.no_entry_price - no_mid
                if loss >= stop_loss_threshold:
                    logger.warning(f"WS STOP-LOSS {cid[:16]}... NO: loss={loss:.4f}")
                    stop_loss_hit = True

            if stop_loss_hit:
                stop_losses.append(cid)
                continue

            # Drift check
            drift = abs(midpoint - position.last_midpoint)
            drift_threshold = max(
                position.max_spread * config.DRIFT_THRESHOLD_FRACTION,
                config.MIN_DRIFT_THRESHOLD,
            )
            if drift > drift_threshold:
                logger.info(
                    f"WS drift {cid[:16]}...: "
                    f"{position.last_midpoint:.4f} -> {midpoint:.4f} "
                    f"(delta={drift:.4f})"
                )
                drifted.append(cid)
            elif position.yes_inventory > 0 or position.no_inventory > 0:
                # Sub-drift move but has inventory — reprice SELL aggressively
                sell_reprices.append(cid)

        return drifted, stop_losses, sell_reprices

    def adjust_drifted_positions(self, drifted_condition_ids: list[str]):
        """Cancel old orders and place new ones at updated midpoints."""
        for cid in drifted_condition_ids:
            midpoint = self.last_midpoints.get(cid)
            if midpoint is None:
                continue

            # If at extreme, force-exit (preserves inventory for SELL unwind)
            if midpoint < config.MIN_MIDPOINT or midpoint > config.MAX_MIDPOINT:
                logger.info(f"Exiting market {cid} (extreme midpoint {midpoint:.4f})")
                self.order_manager.force_exit_market(cid)
                continue

            position = self.order_manager.positions.get(cid)
            if not position:
                continue

            # SELL-only positions (during cooldown): only reprice SELLs, don't touch BUYs
            has_buy = any(o.side == "BUY" for o in position.orders)
            if not has_buy and position.orders:
                result = self.order_manager.reprice_sell_orders(cid, midpoint)
                if result:
                    logger.info(f"Repriced SELL orders for {cid} at new midpoint {midpoint:.4f}")
                else:
                    logger.warning(f"Failed to reprice SELL orders for {cid}")
                continue

            # Normal positions: replace all orders at new midpoint
            result = self.order_manager.replace_orders(cid, midpoint)
            if result:
                logger.info(f"Adjusted orders for {cid} at new midpoint {midpoint:.4f}")
            else:
                logger.warning(f"Failed to adjust orders for {cid}")
