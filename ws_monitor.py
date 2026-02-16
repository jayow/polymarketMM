"""WebSocket monitor for Polymarket CLOB.

Two persistent connections:
  - Market channel (no auth): real-time price changes for drift detection
  - User channel (auth): real-time fill/order events for instant fill handling

Events are pushed into thread-safe queues and drained by the main thread.
"""

import json
import ssl
import threading
import time
from queue import Queue, Empty
from typing import Optional

import certifi
import websocket  # websocket-client

import config
from utils import setup_logger

logger = setup_logger("WSMonitor")

WS_BASE_URL = "wss://ws-subscriptions-clob.polymarket.com/ws"


# ---------------------------------------------------------------------------
# Event data classes
# ---------------------------------------------------------------------------

class PriceEvent:
    """Market channel: price_change event with best bid/ask."""
    __slots__ = ("asset_id", "best_bid", "best_ask", "timestamp")

    def __init__(self, asset_id: str, best_bid: float, best_ask: float, timestamp: float):
        self.asset_id = asset_id
        self.best_bid = best_bid
        self.best_ask = best_ask
        self.timestamp = timestamp

    @property
    def midpoint(self) -> float:
        return (self.best_bid + self.best_ask) / 2.0


class TradeEvent:
    """User channel: trade (fill) event with exact matched amount."""
    __slots__ = (
        "order_id", "asset_id", "side", "size_matched",
        "price", "status", "timestamp",
    )

    def __init__(
        self, order_id: str, asset_id: str, side: str,
        size_matched: float, price: float, status: str, timestamp: float,
    ):
        self.order_id = order_id
        self.asset_id = asset_id
        self.side = side
        self.size_matched = size_matched
        self.price = price
        self.status = status  # MATCHED, MINED, CONFIRMED
        self.timestamp = timestamp


class OrderEvent:
    """User channel: order lifecycle event (placement/cancellation)."""
    __slots__ = ("order_id", "asset_id", "event_type", "size_matched", "timestamp")

    def __init__(self, order_id: str, asset_id: str, event_type: str,
                 size_matched: float, timestamp: float):
        self.order_id = order_id
        self.asset_id = asset_id
        self.event_type = event_type  # PLACEMENT, UPDATE, CANCELLATION
        self.size_matched = size_matched
        self.timestamp = timestamp


# ---------------------------------------------------------------------------
# Base WebSocket connection with reconnection logic
# ---------------------------------------------------------------------------

class _BaseWSConnection:
    """Single WebSocket connection with auto-reconnect and heartbeat."""

    def __init__(self, name: str, url: str, event_queue: Queue):
        self.name = name
        self.url = url
        self.event_queue = event_queue
        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._running = False
        self._subscribed_ids: set[str] = set()
        self._reconnect_delay = 1.0
        self._handshake_sent = False  # True after initial handshake

    def start(self):
        with self._lock:
            if self._running:
                return
            self._running = True
        self._thread = threading.Thread(
            target=self._run_forever, daemon=True, name=f"ws-{self.name}",
        )
        self._thread.start()
        logger.info(f"[{self.name}] WebSocket thread started")

    def stop(self):
        with self._lock:
            self._running = False
            ws = self._ws
        if ws:
            try:
                ws.close()
            except Exception:
                pass

    def subscribe(self, ids: set[str]):
        with self._lock:
            new_ids = ids - self._subscribed_ids
            if not new_ids:
                return
            self._subscribed_ids |= new_ids
            ws = self._ws
        if ws and ws.sock and ws.sock.connected:
            self._send_dynamic_subscribe(new_ids)

    def unsubscribe(self, ids: set[str]):
        with self._lock:
            remove_ids = ids & self._subscribed_ids
            if not remove_ids:
                return
            self._subscribed_ids -= remove_ids
            ws = self._ws
        if ws and ws.sock and ws.sock.connected:
            self._send_unsubscribe(remove_ids)

    # --- Internal ---

    def _run_forever(self):
        while True:
            with self._lock:
                if not self._running:
                    break

            try:
                self._connect_and_run()
            except Exception as e:
                logger.warning(f"[{self.name}] WS error: {e}")

            with self._lock:
                if not self._running:
                    break
                delay = self._reconnect_delay
                self._reconnect_delay = min(delay * 2, config.WS_MAX_RECONNECT_DELAY)

            logger.info(f"[{self.name}] Reconnecting in {delay:.1f}s...")
            time.sleep(delay)

        logger.info(f"[{self.name}] WebSocket thread exiting")

    def _connect_and_run(self):
        self._handshake_sent = False
        ws = websocket.WebSocketApp(
            self.url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        with self._lock:
            self._ws = ws
        sslopt = {"ca_certs": certifi.where(), "cert_reqs": ssl.CERT_REQUIRED}
        ws.run_forever(ping_interval=20, ping_timeout=10, sslopt=sslopt)

    def _on_open(self, ws):
        logger.info(f"[{self.name}] Connected to {self.url}")
        with self._lock:
            self._reconnect_delay = 1.0
            ids_to_sub = set(self._subscribed_ids)
        # Phase 1: Send initial handshake (declares channel type)
        self._send_handshake(ws)
        self._handshake_sent = True
        # Phase 2: Subscribe to any pending IDs via operation message
        if ids_to_sub:
            self._send_dynamic_subscribe(ids_to_sub)
        # Start heartbeat thread (text-level "PING" in addition to protocol pings)
        threading.Thread(
            target=self._heartbeat_loop, daemon=True,
            name=f"ws-ping-{self.name}",
        ).start()

    def _heartbeat_loop(self):
        while True:
            with self._lock:
                if not self._running:
                    return
                ws = self._ws
            if not ws or not ws.sock or not ws.sock.connected:
                return
            try:
                ws.send("PING")
            except Exception:
                # Don't die on a single failure — retry after checking socket
                time.sleep(1)
                with self._lock:
                    ws = self._ws
                if not ws or not ws.sock or not ws.sock.connected:
                    return
                try:
                    ws.send("PING")
                except Exception:
                    logger.warning(f"[{self.name}] Heartbeat failed twice, exiting heartbeat loop")
                    return
            time.sleep(config.WS_PING_INTERVAL_SECONDS)

    def _on_error(self, ws, error):
        logger.warning(f"[{self.name}] WS error: {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        logger.info(f"[{self.name}] WS closed: {close_status_code} {close_msg}")

    def _on_message(self, ws, message):
        raise NotImplementedError

    def _send_handshake(self, ws):
        """Send initial channel handshake (declares type). Called once on open."""
        raise NotImplementedError

    def _send_dynamic_subscribe(self, ids: set[str]):
        """Subscribe to IDs after handshake (uses 'operation' field)."""
        raise NotImplementedError

    def _send_unsubscribe(self, ids: set[str]):
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Market channel: price changes (no auth)
# ---------------------------------------------------------------------------

class MarketWSConnection(_BaseWSConnection):
    """Subscribe to market price changes by token_id (asset_id). No auth."""

    def __init__(self, event_queue: Queue):
        super().__init__("market", f"{WS_BASE_URL}/market", event_queue)

    def _send_handshake(self, ws):
        """Send initial empty handshake to declare market channel."""
        msg = json.dumps({"assets_ids": [], "type": "market"})
        try:
            ws.send(msg)
            logger.info("[market] Handshake sent")
        except Exception as e:
            logger.warning(f"[market] Handshake failed: {e}")

    def _send_dynamic_subscribe(self, ids: set[str]):
        """Subscribe to assets using 'operation' field (post-handshake)."""
        id_list = list(ids)
        # Polymarket limit: 500 instruments per connection
        for i in range(0, len(id_list), 500):
            chunk = id_list[i:i + 500]
            msg = json.dumps({"assets_ids": chunk, "operation": "subscribe"})
            try:
                self._ws.send(msg)
                logger.info(f"[market] Subscribed to {len(chunk)} assets")
            except Exception as e:
                logger.warning(f"[market] Subscribe failed: {e}")

    def _send_unsubscribe(self, ids: set[str]):
        # Market channel: stale events are ignored by consumer.
        # Full unsubscribe requires reconnect (done automatically on rescan).
        pass

    def _on_message(self, ws, message: str):
        if message == "PONG":
            return

        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return

        events = data if isinstance(data, list) else [data]
        for event in events:
            event_type = event.get("event_type", "")

            if event_type == "price_change":
                self._handle_price_change(event)

    def _handle_price_change(self, event: dict):
        """Parse price_change event and push PriceEvent to queue."""
        # price_change can have price_changes[] array or top-level fields
        changes = event.get("price_changes", [event])
        for change in changes:
            asset_id = change.get("asset_id", "")
            if not asset_id:
                continue
            try:
                best_bid = float(change.get("best_bid", 0) or 0)
                best_ask = float(change.get("best_ask", 0) or 0)
                if best_bid <= 0 or best_ask <= 0:
                    continue
                self.event_queue.put(PriceEvent(
                    asset_id=asset_id,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    timestamp=time.time(),
                ))
            except (ValueError, TypeError):
                pass


# ---------------------------------------------------------------------------
# User channel: fills and order events (auth required)
# ---------------------------------------------------------------------------

class UserWSConnection(_BaseWSConnection):
    """Subscribe to user trade/order events by condition_id. Auth required."""

    def __init__(self, event_queue: Queue,
                 api_key: str, api_secret: str, api_passphrase: str):
        super().__init__("user", f"{WS_BASE_URL}/user", event_queue)
        self._api_key = api_key
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase

    def _send_handshake(self, ws):
        """Send initial handshake with auth to declare user channel."""
        msg = json.dumps({
            "markets": [],
            "type": "user",
            "auth": {
                "apiKey": self._api_key,
                "secret": self._api_secret,
                "passphrase": self._api_passphrase,
            },
        })
        try:
            ws.send(msg)
            logger.info("[user] Handshake sent (with auth)")
        except Exception as e:
            logger.warning(f"[user] Handshake failed: {e}")

    def _send_dynamic_subscribe(self, ids: set[str]):
        """Subscribe to markets using 'operation' field (post-handshake)."""
        id_list = list(ids)
        msg = json.dumps({
            "markets": id_list,
            "auth": {
                "apiKey": self._api_key,
                "secret": self._api_secret,
                "passphrase": self._api_passphrase,
            },
            "operation": "subscribe",
        })
        try:
            self._ws.send(msg)
            logger.info(f"[user] Subscribed to {len(id_list)} markets")
        except Exception as e:
            logger.warning(f"[user] Subscribe failed: {e}")

    def _send_unsubscribe(self, ids: set[str]):
        # Best-effort unsubscribe
        for cid in ids:
            msg = json.dumps({
                "markets": [cid],
                "auth": {
                    "apiKey": self._api_key,
                    "secret": self._api_secret,
                    "passphrase": self._api_passphrase,
                },
                "operation": "unsubscribe",
            })
            try:
                self._ws.send(msg)
            except Exception:
                pass

    def _on_message(self, ws, message: str):
        if message == "PONG":
            return

        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return

        events = data if isinstance(data, list) else [data]
        for event in events:
            event_type = event.get("event_type", "")

            if event_type == "trade":
                self._handle_trade(event)
            elif event_type == "order":
                self._handle_order(event)

    def _handle_trade(self, event: dict):
        """Parse trade event and push TradeEvent(s) to queue.

        A trade event contains maker_orders[] with per-order matched_amount.
        We emit one TradeEvent per maker order that matches one of our orders.
        """
        status = event.get("status", "").upper()

        # Parse maker_orders for our fill details
        for mo in event.get("maker_orders", []):
            order_id = mo.get("order_id", "") or mo.get("id", "")
            matched_amount = float(mo.get("matched_amount", 0) or 0)
            asset_id = mo.get("asset_id", "") or event.get("asset_id", "")
            price = float(mo.get("price", 0) or event.get("price", 0) or 0)

            if order_id and matched_amount > 0:
                # Determine side: maker_orders may not have side,
                # infer from the trade event (trade side is taker's side,
                # maker is the opposite)
                taker_side = event.get("side", "").upper()
                maker_side = "SELL" if taker_side == "BUY" else "BUY"

                self.event_queue.put(TradeEvent(
                    order_id=order_id,
                    asset_id=asset_id,
                    side=maker_side,
                    size_matched=matched_amount,
                    price=price,
                    status=status,
                    timestamp=time.time(),
                ))

    def _handle_order(self, event: dict):
        """Parse order lifecycle event."""
        order_id = event.get("id", "") or event.get("order_id", "")
        asset_id = event.get("asset_id", "")
        evt_type = event.get("type", "").upper()  # PLACEMENT, UPDATE, CANCELLATION
        size_matched = float(event.get("size_matched", 0) or 0)

        if order_id:
            self.event_queue.put(OrderEvent(
                order_id=order_id,
                asset_id=asset_id,
                event_type=evt_type,
                size_matched=size_matched,
                timestamp=time.time(),
            ))


# ---------------------------------------------------------------------------
# WSMonitor facade
# ---------------------------------------------------------------------------

class WSMonitor:
    """Manages both WS connections. Used by bot.py.

    Usage:
        monitor = WSMonitor(api_key, api_secret, api_passphrase)
        monitor.start()
        monitor.subscribe_market(token_ids)
        monitor.subscribe_user(condition_ids)

        # In main loop:
        prices = monitor.drain_price_events()
        fills = monitor.drain_trade_events()

        # Shutdown:
        monitor.stop()
    """

    def __init__(self, api_key: str, api_secret: str, api_passphrase: str):
        self.price_queue: Queue = Queue()
        self.trade_queue: Queue = Queue()

        self._market_ws = MarketWSConnection(self.price_queue)
        self._user_ws = UserWSConnection(
            self.trade_queue, api_key, api_secret, api_passphrase,
        )

    def start(self):
        self._market_ws.start()
        self._user_ws.start()

    def stop(self):
        self._market_ws.stop()
        self._user_ws.stop()

    def subscribe_market(self, token_ids: set[str]):
        self._market_ws.subscribe(token_ids)

    def unsubscribe_market(self, token_ids: set[str]):
        self._market_ws.unsubscribe(token_ids)

    def subscribe_user(self, condition_ids: set[str]):
        self._user_ws.subscribe(condition_ids)

    def unsubscribe_user(self, condition_ids: set[str]):
        self._user_ws.unsubscribe(condition_ids)

    def drain_price_events(self, max_events: int = 500) -> list[PriceEvent]:
        events = []
        for _ in range(max_events):
            try:
                event = self.price_queue.get_nowait()
                if isinstance(event, PriceEvent):
                    events.append(event)
            except Empty:
                break
        return events

    def drain_trade_events(self, max_events: int = 100) -> list:
        """Drain trade and order events. Returns list of TradeEvent | OrderEvent."""
        events = []
        for _ in range(max_events):
            try:
                events.append(self.trade_queue.get_nowait())
            except Empty:
                break
        return events

    @property
    def is_market_connected(self) -> bool:
        ws = self._market_ws._ws
        return bool(ws and ws.sock and ws.sock.connected)

    @property
    def is_user_connected(self) -> bool:
        ws = self._user_ws._ws
        return bool(ws and ws.sock and ws.sock.connected)
