from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import requests
from py_clob_client.client import ClobClient

import config
from utils import (
    setup_logger,
    compute_opportunity_score,
    calculate_book_depth_in_range,
    safe_api_call,
)

logger = setup_logger("MarketScanner")


@dataclass
class MarketOpportunity:
    condition_id: str
    question: str
    token_id_yes: str
    token_id_no: str
    midpoint: float
    reward_rate: float
    min_size: float
    max_spread: float
    book_depth_usdc: float
    current_spread: float
    tick_size: float
    neg_risk: bool
    opportunity_score: float = 0.0


@dataclass
class _PreCandidate:
    """Lightweight candidate from sampling data (no extra API calls needed)."""
    condition_id: str
    question: str
    token_id_yes: str
    token_id_no: str
    midpoint: float
    reward_rate: float
    min_size: float
    max_spread: float
    pre_score: float  # rough score from reward_rate alone


class MarketScanner:
    def __init__(self, client: ClobClient):
        self.client = client

    def _fetch_volume_data(self) -> tuple[dict[str, float], dict[str, str]]:
        """Fetch 24hr volume and event grouping from Gamma API.

        Returns (volumes, event_groups):
          volumes: {condition_id: volume_24hr}
          event_groups: {condition_id: event_id} for correlated market detection
        """
        volumes: dict[str, float] = {}
        event_groups: dict[str, str] = {}
        page_size = 500
        try:
            for offset in range(0, 5000, page_size):
                resp = requests.get(
                    f"{config.GAMMA_HOST}/markets",
                    params={
                        "active": "true",
                        "closed": "false",
                        "limit": page_size,
                        "offset": offset,
                        "order": "volume24hr",
                        "ascending": "false",
                    },
                    timeout=15,
                )
                if resp.status_code != 200:
                    break
                data = resp.json()
                if not data:
                    break
                above_threshold = 0
                for market in data:
                    cid = market.get("conditionId", "")
                    if not cid:
                        continue
                    vol = market.get("volume24hr", 0)
                    try:
                        vol_f = float(vol or 0)
                    except (ValueError, TypeError):
                        continue
                    volumes[cid] = vol_f
                    if vol_f >= config.MIN_DAILY_VOLUME:
                        above_threshold += 1
                    # Capture event grouping for diversity limits
                    events = market.get("events", [])
                    if events and isinstance(events, list):
                        event_groups[cid] = str(events[0].get("id", ""))
                # Stop early once a full page has no markets above threshold
                if above_threshold == 0:
                    break
            logger.info(f"Fetched volume data for {len(volumes)} markets from Gamma API")
        except Exception as e:
            logger.warning(f"Failed to fetch volume data from Gamma API: {e}")
        return volumes, event_groups

    def fetch_all_sampling_markets(self) -> list[dict]:
        """Fetch all sampling markets with pagination."""
        all_markets = []
        next_cursor = "MA=="
        end_cursor = "LTE="

        while next_cursor != end_cursor:
            resp = safe_api_call(
                self.client.get_sampling_simplified_markets, next_cursor
            )
            data = resp if isinstance(resp, list) else resp.get("data", [])
            all_markets.extend(data if isinstance(data, list) else [])
            if isinstance(resp, dict):
                next_cursor = resp.get("next_cursor", end_cursor)
            else:
                break

        logger.info(f"Fetched {len(all_markets)} sampling markets")
        return all_markets

    def _pre_filter(self, market: dict) -> Optional[_PreCandidate]:
        """
        Phase 1: Filter using only data from the sampling response.
        No extra API calls. Returns a lightweight candidate or None.
        """
        # Must be active and accepting orders
        if not market.get("active") or not market.get("accepting_orders"):
            return None
        if market.get("closed") or market.get("archived"):
            return None

        # Skip markets near expiration (high fill risk from sharp price swings)
        end_date = market.get("end_date_iso")
        if end_date:
            try:
                expiry = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                hours_left = (expiry - datetime.now(timezone.utc)).total_seconds() / 3600
                if hours_left < config.MIN_HOURS_TO_EXPIRY:
                    return None
            except (ValueError, TypeError):
                pass  # If we can't parse the date, don't filter

        # Extract rewards config
        rewards = market.get("rewards") or {}
        max_spread = rewards.get("max_spread")
        min_size = rewards.get("min_size")
        rates = rewards.get("rates") or []

        if not max_spread or not min_size or not rates:
            return None

        # API returns max_spread in cents (e.g. 4.5 = 4.5 cents).
        # Convert to price units (0.045) for all downstream math.
        max_spread = float(max_spread) / 100.0
        min_size = float(min_size)

        if max_spread < config.MIN_MAX_SPREAD:
            return None

        daily_rate = sum(float(r.get("rewards_daily_rate", 0)) for r in rates)
        if daily_rate < config.MIN_REWARD_RATE:
            return None

        # Extract tokens and prices from the response (no API call needed)
        tokens = market.get("tokens", [])
        if len(tokens) < 2:
            return None

        yes_id, no_id, midpoint = None, None, None
        for t in tokens:
            outcome = t.get("outcome", "").lower()
            if outcome == "yes":
                yes_id = t.get("token_id")
                midpoint = float(t.get("price", 0))
            elif outcome == "no":
                no_id = t.get("token_id")

        if not yes_id or not no_id:
            yes_id = tokens[0].get("token_id")
            no_id = tokens[1].get("token_id")
            midpoint = float(tokens[0].get("price", 0))

        if not yes_id or not no_id or not midpoint:
            return None

        if midpoint < config.MIN_MIDPOINT or midpoint > config.MAX_MIDPOINT:
            return None

        # Skip markets where entry cost per side exceeds limit
        worst_side_price = max(midpoint, 1.0 - midpoint)
        if min_size * worst_side_price > config.MAX_ENTRY_COST:
            return None

        condition_id = market.get("condition_id", "")
        question = market.get("question", market.get("description", "Unknown"))

        # Rough pre-score: higher reward rate = more interesting
        # (we'll refine with book depth after fetching orderbooks)
        pre_score = daily_rate / max(max_spread, 0.001)

        return _PreCandidate(
            condition_id=condition_id,
            question=question,
            token_id_yes=yes_id,
            token_id_no=no_id,
            midpoint=midpoint,
            reward_rate=daily_rate,
            min_size=min_size,
            max_spread=max_spread,
            pre_score=pre_score,
        )

    def _fetch_details(self, candidate: _PreCandidate) -> Optional[MarketOpportunity]:
        """
        Phase 2: Fetch orderbook, spread, tick_size for a pre-filtered candidate.
        Only called for top candidates (bounded by DETAIL_CANDIDATES).
        """
        try:
            book = safe_api_call(self.client.get_order_book, candidate.token_id_yes)

            book_depth = calculate_book_depth_in_range(
                book.bids, book.asks, candidate.midpoint, candidate.max_spread
            )
            if book_depth > config.MAX_BOOK_DEPTH_USDC:
                logger.debug(
                    f"  Skipped {candidate.question[:40]}: depth ${book_depth:.0f} > max ${config.MAX_BOOK_DEPTH_USDC}"
                )
                return None
            if book_depth < config.MIN_BOOK_DEPTH_USDC:
                logger.debug(
                    f"  Skipped {candidate.question[:40]}: depth ${book_depth:.0f} < min ${config.MIN_BOOK_DEPTH_USDC}"
                )
                return None

            spread_resp = safe_api_call(self.client.get_spread, candidate.token_id_yes)
            current_spread = float(spread_resp["spread"])

            # Skip markets where actual spread is much wider than max_spread.
            # These have no active LPs — we'd be sole provider and get adversely selected.
            if current_spread > candidate.max_spread * config.MAX_SPREAD_RATIO:
                logger.debug(
                    f"  Skipped {candidate.question[:40]}: spread {current_spread:.4f} > "
                    f"{candidate.max_spread:.4f} * {config.MAX_SPREAD_RATIO} (wide spread)"
                )
                return None

            # Volatility filter: reject markets where 24h price range exceeds
            # MAX_VOLATILITY_RATIO * max_spread. High ratio = price crosses our
            # order zone frequently = fills eat our capital.
            if config.MAX_VOLATILITY_RATIO > 0:
                try:
                    hist_resp = requests.get(
                        f"{config.CLOB_HOST}/prices-history",
                        params={
                            "market": candidate.token_id_yes,
                            "interval": "1d",
                            "fidelity": 60,
                        },
                        timeout=10,
                    )
                    if hist_resp.status_code == 200:
                        history = hist_resp.json().get("history", [])
                        prices = [float(p["p"]) for p in history if "p" in p]
                        min_points = getattr(config, "MIN_VOLATILITY_DATA_POINTS", 10)
                        if len(prices) < min_points:
                            logger.debug(
                                f"  Skipped {candidate.question[:40]}: "
                                f"only {len(prices)} price points "
                                f"(need {min_points}+)"
                            )
                            return None
                        if prices:
                            price_range = max(prices) - min(prices)
                            vol_ratio = price_range / candidate.max_spread
                            if vol_ratio > config.MAX_VOLATILITY_RATIO:
                                logger.debug(
                                    f"  Skipped {candidate.question[:40]}: "
                                    f"volatility ratio {vol_ratio:.1f}x > "
                                    f"{config.MAX_VOLATILITY_RATIO}x"
                                )
                                return None
                except Exception as vol_err:
                    logger.debug(f"  Volatility check failed for {candidate.question[:40]}: {vol_err}")
                    # Don't filter on failure — let other filters catch bad markets

            tick_size = float(safe_api_call(self.client.get_tick_size, candidate.token_id_yes))
            neg_risk = safe_api_call(self.client.get_neg_risk, candidate.token_id_yes)

        except Exception as e:
            logger.warning(f"Failed to fetch details for {candidate.condition_id}: {e}")
            return None

        score = compute_opportunity_score(
            candidate.reward_rate, book_depth, current_spread, candidate.max_spread
        )

        # neg_risk markets share collateral across outcomes — more capital efficient
        if neg_risk:
            score *= config.NEG_RISK_SCORE_BOOST

        return MarketOpportunity(
            condition_id=candidate.condition_id,
            question=candidate.question,
            token_id_yes=candidate.token_id_yes,
            token_id_no=candidate.token_id_no,
            midpoint=candidate.midpoint,
            reward_rate=candidate.reward_rate,
            min_size=candidate.min_size,
            max_spread=candidate.max_spread,
            book_depth_usdc=book_depth,
            current_spread=current_spread,
            tick_size=tick_size,
            neg_risk=neg_risk,
            opportunity_score=score,
        )

    def scan_and_rank(self, max_markets: int = 0, force_include_tokens: set[str] | None = None) -> list[MarketOpportunity]:
        """
        Two-phase scan:
        1. Pre-filter all 3000+ markets using data already in the response (instant)
        2. Fetch orderbook details only for top ~50 candidates (bounded API calls)
        3. Final rank and return top max_markets
        """
        if max_markets <= 0:
            # Fallback: use cap if set, otherwise no limit (return all scored)
            max_markets = config.MAX_MARKETS_CAP if config.MAX_MARKETS_CAP > 0 else 999

        raw_markets = self.fetch_all_sampling_markets()

        # Phase 1: fast pre-filter (no API calls)
        candidates = []
        for m in raw_markets:
            c = self._pre_filter(m)
            if c:
                candidates.append(c)

        # Phase 1.5: volume filter via Gamma API (one bulk request)
        volumes, event_groups = self._fetch_volume_data()
        if volumes:
            before = len(candidates)
            candidates = [
                c for c in candidates
                # Markets not in Gamma data are treated as zero volume (blocked).
                # We fetch top markets sorted by volume, so anything missing is low-vol.
                if volumes.get(c.condition_id, 0) >= config.MIN_DAILY_VOLUME
            ]
            filtered = before - len(candidates)
            if filtered > 0:
                logger.info(f"Volume filter: removed {filtered} illiquid markets (<${config.MIN_DAILY_VOLUME} 24hr)")

        # Force-include markets matching recovery tokens (even if filtered out)
        if force_include_tokens:
            top_cids = {c.condition_id for c in candidates}
            for m in raw_markets:
                tokens = m.get("tokens", [])
                tids = {t.get("token_id", "") for t in tokens}
                if tids & force_include_tokens:
                    cid = m.get("condition_id", "")
                    if cid and cid not in top_cids:
                        forced = self._pre_filter(m)
                        if forced:
                            candidates.append(forced)
                            logger.info(f"Force-included recovery market {cid[:16]}...")

        candidates.sort(key=lambda c: c.pre_score, reverse=True)
        top_candidates = candidates[:config.DETAIL_CANDIDATES]

        logger.info(
            f"Pre-filtered {len(raw_markets)} markets -> "
            f"{len(candidates)} eligible -> top {len(top_candidates)} for detail fetch"
        )

        # Phase 2: fetch details for top candidates only
        opportunities = []
        phase2_rejected = 0
        for c in top_candidates:
            opp = self._fetch_details(c)
            if opp:
                opportunities.append(opp)
            else:
                phase2_rejected += 1

        if phase2_rejected > 0:
            logger.info(
                f"Phase 2 detail filter: {len(top_candidates)} checked, "
                f"{len(opportunities)} passed, {phase2_rejected} rejected "
                f"(book depth ${config.MIN_BOOK_DEPTH_USDC}-${config.MAX_BOOK_DEPTH_USDC})"
            )

        opportunities.sort(key=lambda o: o.opportunity_score, reverse=True)

        # Phase 3: Event diversity limit — prevent correlated fills
        if event_groups and config.MAX_MARKETS_PER_EVENT > 0:
            event_counts: dict[str, int] = {}
            diverse_opps = []
            skipped_events = 0
            for opp in opportunities:
                eid = event_groups.get(opp.condition_id, opp.condition_id)
                count = event_counts.get(eid, 0)
                if count >= config.MAX_MARKETS_PER_EVENT:
                    skipped_events += 1
                    continue
                event_counts[eid] = count + 1
                diverse_opps.append(opp)
            if skipped_events > 0:
                logger.info(
                    f"Event diversity: capped {skipped_events} markets "
                    f"(max {config.MAX_MARKETS_PER_EVENT} per event group)"
                )
            opportunities = diverse_opps

        top = opportunities[:max_markets]

        # Ensure force-included markets make it into the final list
        if force_include_tokens:
            top_cids = {o.condition_id for o in top}
            for opp in opportunities[max_markets:]:
                if ({opp.token_id_yes, opp.token_id_no} & force_include_tokens
                        and opp.condition_id not in top_cids):
                    top.append(opp)

        # Fetch question text for final picks
        for opp in top:
            if opp.question == "Unknown":
                try:
                    market_info = safe_api_call(self.client.get_market, opp.condition_id)
                    opp.question = market_info.get("question", "Unknown") if isinstance(market_info, dict) else "Unknown"
                except Exception:
                    pass

        logger.info(
            f"Detailed analysis: {len(opportunities)} scored, top {len(top)} selected"
        )
        for opp in top:
            logger.info(
                f"  [{opp.opportunity_score:.6f}] {opp.question[:60]} "
                f"| rate=${opp.reward_rate:.2f}/day | depth=${opp.book_depth_usdc:.0f} "
                f"| spread={opp.current_spread:.4f} | mid={opp.midpoint:.4f}"
            )

        return top
