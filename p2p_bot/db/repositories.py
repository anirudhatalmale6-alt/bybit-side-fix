from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from p2p_bot.db.database import Database
from p2p_bot.config import Settings
from p2p_bot.models.opportunity import ArbitrageOpportunity
from p2p_bot.models.order import P2POrder
from p2p_bot.models.trade import Trade


class RawOrderRepository:
    def __init__(self, db: Database, settings: Settings | None = None) -> None:
        self.db = db
        self.settings = settings

    async def save_batch(self, orders: list[P2POrder]) -> int:
        if not orders:
            return 0
        rows = [
            (
                order.platform,
                order.order_id,
                order.side,
                order.asset,
                order.fiat,
                float(order.price),
                float(order.min_amount),
                float(order.max_amount),
                float(order.available),
                json.dumps(order.payment_methods),
                order.merchant_id,
                float(order.merchant_rating),
                order.merchant_orders,
                order.merchant_days,
                order.timestamp.isoformat(),
            )
            for order in orders
        ]
        rowcount = await self.db.executemany(
            """
            INSERT INTO raw_orders (
                platform, order_id, side, asset, fiat, price, min_amount, max_amount,
                available, payment_methods, merchant_id, merchant_rating, merchant_orders,
                merchant_days, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        return rowcount

    async def prune_history(self, *, batch_size: int = 50000) -> int:
        if self.settings is None:
            return 0
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=self.settings.raw_orders_retention_hours)).isoformat()
        total_deleted = 0
        while True:
            deleted = await self.db.execute_rowcount(
                """
                DELETE FROM raw_orders
                WHERE id IN (
                    SELECT id
                    FROM raw_orders
                    WHERE scraped_at < ?
                    LIMIT ?
                )
                """,
                (cutoff, batch_size),
            )
            if deleted <= 0:
                break
            total_deleted += deleted
        return total_deleted

    async def refresh_market_activity(
        self,
        windows_hours: tuple[int, ...],
        *,
        asset: str | None = None,
        fiats: tuple[str, ...] | None = None,
    ) -> int:
        ordered_hours = tuple(sorted({int(hours) for hours in windows_hours if int(hours) > 0}))
        if not ordered_hours:
            return 0

        refreshed_at = datetime.now(timezone.utc).isoformat()
        total_rows = 0
        for hours in ordered_hours:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
            clauses = ["scraped_at >= ?"]
            params: list[object] = [cutoff]
            if asset is not None:
                clauses.append("asset = ?")
                params.append(asset)
            if fiats:
                placeholders = ",".join("?" for _ in fiats)
                clauses.append(f"fiat IN ({placeholders})")
                params.extend(fiats)

            rows = await self.db.fetchall(
                f"""
                SELECT
                    platform,
                    asset,
                    fiat,
                    side,
                    COUNT(DISTINCT order_id) AS unique_ads,
                    COUNT(DISTINCT NULLIF(merchant_id, '')) AS unique_merchants
                FROM raw_orders
                WHERE {' AND '.join(clauses)}
                GROUP BY platform, asset, fiat, side
                """,
                tuple(params),
            )
            if rows:
                total_rows += await self.db.executemany(
                    """
                    INSERT INTO market_activity_rollups (
                        window_hours, platform, asset, fiat, side,
                        unique_ads, unique_merchants, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(window_hours, platform, asset, fiat, side)
                    DO UPDATE SET
                        unique_ads = excluded.unique_ads,
                        unique_merchants = excluded.unique_merchants,
                        updated_at = excluded.updated_at
                    """,
                    [
                        (
                            hours,
                            str(row["platform"]),
                            str(row["asset"]),
                            str(row["fiat"]),
                            str(row["side"]),
                            int(row["unique_ads"] or 0),
                            int(row["unique_merchants"] or 0),
                            refreshed_at,
                        )
                        for row in rows
                    ],
                )
            await self.db.execute_rowcount(
                """
                DELETE FROM market_activity_rollups
                WHERE window_hours = ?
                  AND updated_at < ?
                """,
                (hours, refreshed_at),
            )
        return total_rows

    async def recent_activity(
        self,
        windows_hours: tuple[int, ...] = (1, 2, 3, 24),
        *,
        asset: str | None = None,
        fiats: tuple[str, ...] | None = None,
    ) -> dict[int, dict[tuple[str, str, str, str], dict[str, float]]]:
        rollups = await self._recent_activity_from_rollups(
            windows_hours,
            asset=asset,
            fiats=fiats,
        )
        if rollups is not None:
            return rollups
        return await self._recent_activity_from_raw_orders(
            windows_hours,
            asset=asset,
            fiats=fiats,
        )

    async def _recent_activity_from_rollups(
        self,
        windows_hours: tuple[int, ...],
        *,
        asset: str | None = None,
        fiats: tuple[str, ...] | None = None,
    ) -> dict[int, dict[tuple[str, str, str, str], dict[str, float]]] | None:
        if not windows_hours:
            return {}

        ordered_hours = tuple(sorted(set(windows_hours)))
        clauses = [f"window_hours IN ({','.join('?' for _ in ordered_hours)})"]
        params: list[object] = list(ordered_hours)
        if asset is not None:
            clauses.append("asset = ?")
            params.append(asset)
        if fiats:
            placeholders = ",".join("?" for _ in fiats)
            clauses.append(f"fiat IN ({placeholders})")
            params.extend(fiats)
        if self.settings is not None:
            freshness_cutoff = (
                datetime.now(timezone.utc)
                - timedelta(seconds=max(self.settings.raw_orders_prune_interval_sec * 2, 900))
            ).isoformat()
            clauses.append("updated_at >= ?")
            params.append(freshness_cutoff)

        rows = await self.db.fetchall(
            f"""
            SELECT
                window_hours,
                platform,
                asset,
                fiat,
                side,
                unique_ads,
                unique_merchants
            FROM market_activity_rollups
            WHERE {' AND '.join(clauses)}
            """,
            tuple(params),
        )
        if not rows:
            return None

        results: dict[int, dict[tuple[str, str, str, str], dict[str, float]]] = {
            hours: {} for hours in ordered_hours
        }
        for row in rows:
            key = (
                str(row["platform"]),
                str(row["fiat"]),
                str(row["asset"]),
                str(row["side"]),
            )
            results[int(row["window_hours"])][key] = {
                "unique_ads": float(row["unique_ads"] or 0),
                "unique_merchants": float(row["unique_merchants"] or 0),
            }
        return results

    async def _recent_activity_from_raw_orders(
        self,
        windows_hours: tuple[int, ...] = (1, 2, 3, 24),
        *,
        asset: str | None = None,
        fiats: tuple[str, ...] | None = None,
    ) -> dict[int, dict[tuple[str, str, str, str], dict[str, float]]]:
        if not windows_hours:
            return {}

        ordered_hours = tuple(sorted(set(windows_hours)))
        now = datetime.now(timezone.utc)
        cutoffs = {hours: (now - timedelta(hours=hours)).isoformat() for hours in ordered_hours}
        max_window = max(ordered_hours)

        clauses = ["scraped_at >= ?"]
        params: list[object] = [cutoffs[max_window]]
        if asset is not None:
            clauses.append("asset = ?")
            params.append(asset)
        if fiats:
            placeholders = ",".join("?" for _ in fiats)
            clauses.append(f"fiat IN ({placeholders})")
            params.extend(fiats)

        select_parts = [
            "platform",
            "asset",
            "fiat",
            "side",
        ]
        aggregate_params: list[object] = []
        for hours in ordered_hours:
            cutoff = cutoffs[hours]
            select_parts.extend(
                [
                    f"COUNT(DISTINCT CASE WHEN scraped_at >= ? THEN order_id END) AS unique_ads_{hours}",
                    f"COUNT(DISTINCT CASE WHEN scraped_at >= ? THEN NULLIF(merchant_id, '') END) AS unique_merchants_{hours}",
                ]
            )
            aggregate_params.extend([cutoff, cutoff])

        sql = f"""
            SELECT
                {', '.join(select_parts)}
            FROM raw_orders
            WHERE {' AND '.join(clauses)}
            GROUP BY platform, asset, fiat, side
        """
        rows = await self.db.fetchall(sql, tuple(aggregate_params + params))

        results: dict[int, dict[tuple[str, str, str, str], dict[str, float]]] = {
            hours: {} for hours in ordered_hours
        }
        for row in rows:
            key = (
                str(row["platform"]),
                str(row["fiat"]),
                str(row["asset"]),
                str(row["side"]),
            )
            for hours in ordered_hours:
                results[hours][key] = {
                    "unique_ads": float(row[f"unique_ads_{hours}"] or 0),
                    "unique_merchants": float(row[f"unique_merchants_{hours}"] or 0),
                }
        return results


class OpportunityRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def save_many(self, opportunities: list[ArbitrageOpportunity]) -> int:
        if not opportunities:
            return 0
        rows = [
            (
                opportunity.type,
                float(opportunity.spread_pct),
                float(opportunity.estimated_profit_usd),
                float(opportunity.volume_usdt),
                opportunity.buy_order.platform,
                opportunity.sell_order.platform,
                opportunity.buy_order.fiat,
                opportunity.sell_order.fiat,
                opportunity.rail_status,
                opportunity.liquidity_status,
                float(opportunity.gross_profit_usd),
                float(opportunity.total_fees_usd),
                opportunity.note,
                json.dumps(opportunity.snapshot(), ensure_ascii=False),
                opportunity.detected_at.isoformat(),
                opportunity.status,
            )
            for opportunity in opportunities
        ]
        return await self.db.executemany(
            """
            INSERT INTO opportunities (
                type, spread_pct, estimated_profit, volume_usdt,
                buy_platform, sell_platform, buy_fiat, sell_fiat,
                rail_status, liquidity_status, gross_profit_usd, total_fees_usd,
                note, payload_json, detected_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    async def list_recent(self, limit: int = 20) -> list[dict[str, object]]:
        rows = await self.db.fetchall(
            """
            SELECT *
            FROM opportunities
            ORDER BY detected_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in rows]


class SignalJournalRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def create_from_opportunity(self, opportunity: ArbitrageOpportunity) -> int:
        now = opportunity.detected_at.isoformat()
        return await self.db.execute(
            """
            INSERT INTO signal_journal (
                alert_fingerprint, opportunity_type, route,
                buy_platform, sell_platform, buy_fiat, sell_fiat,
                volume_usdt, estimated_profit_usd, gross_profit_usd, fees_usd,
                rail_status, liquidity_status, status, note,
                payload_json, inventory_applied,
                actual_profit_usd, actual_volume_usdt, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                opportunity.alert_fingerprint,
                opportunity.type,
                opportunity.note,
                opportunity.buy_order.platform,
                opportunity.sell_order.platform,
                opportunity.buy_order.fiat,
                opportunity.sell_order.fiat,
                float(opportunity.volume_usdt),
                float(opportunity.estimated_profit_usd),
                float(opportunity.gross_profit_usd),
                float(opportunity.total_fees_usd),
                opportunity.rail_status,
                opportunity.liquidity_status,
                "new",
                "",
                json.dumps(opportunity.snapshot(), ensure_ascii=False),
                0,
                None,
                None,
                now,
                now,
            ),
        )

    async def mark_status(self, signal_id: int, status: str, note: str | None = None) -> int:
        row = await self.get(signal_id)
        if row is None:
            return 0
        final_note = note if note is not None else str(row.get("note") or "")
        return await self.db.execute(
            """
            UPDATE signal_journal
            SET status = ?, note = ?, updated_at = ?
            WHERE route = ?
              AND buy_platform = ?
              AND sell_platform = ?
              AND buy_fiat = ?
              AND sell_fiat = ?
              AND status = 'new'
            """,
            (
                status,
                final_note,
                datetime.now(timezone.utc).isoformat(),
                str(row["route"]),
                str(row["buy_platform"]),
                str(row["sell_platform"]),
                str(row["buy_fiat"]),
                str(row["sell_fiat"]),
            ),
        )

    async def set_status_by_id(self, signal_id: int, status: str, note: str | None = None) -> int:
        row = await self.get(signal_id)
        if row is None:
            return 0
        final_note = note if note is not None else str(row.get("note") or "")
        return await self.db.execute(
            """
            UPDATE signal_journal
            SET status = ?, note = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                status,
                final_note,
                datetime.now(timezone.utc).isoformat(),
                signal_id,
            ),
        )

    async def set_actual(
        self,
        signal_id: int,
        actual_profit_usd: float,
        actual_volume_usdt: float | None = None,
        note: str | None = None,
    ) -> int:
        row = await self.get(signal_id)
        if row is None:
            return 0
        final_note = note if note is not None else str(row.get("note") or "")
        volume_value = actual_volume_usdt if actual_volume_usdt is not None else row["volume_usdt"]
        return await self.db.execute(
            """
            UPDATE signal_journal
            SET status = ?, actual_profit_usd = ?, actual_volume_usdt = ?, note = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                "done",
                actual_profit_usd,
                volume_value,
                final_note,
                datetime.now(timezone.utc).isoformat(),
                signal_id,
            ),
        )

    async def append_note(self, signal_id: int, note: str) -> int:
        row = await self.get(signal_id)
        if row is None:
            return 0
        existing = str(row.get("note") or "").strip()
        final_note = note if not existing else f"{existing} | {note}"
        return await self.db.execute(
            """
            UPDATE signal_journal
            SET note = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                final_note,
                datetime.now(timezone.utc).isoformat(),
                signal_id,
            ),
        )

    async def get(self, signal_id: int) -> dict[str, object] | None:
        row = await self.db.fetchone(
            "SELECT * FROM signal_journal WHERE id = ?",
            (signal_id,),
        )
        return dict(row) if row is not None else None

    async def finalize_signal_execution(
        self,
        signal_id: int,
        *,
        inventory_repository: InventoryRepository,
        actual_profit_usd: Decimal | None = None,
        actual_volume_usdt: Decimal | None = None,
        note: str | None = None,
        close_matching_route: bool = False,
    ) -> dict[str, object] | None:
        return await self.db.run_in_transaction(
            lambda conn: self._finalize_signal_execution_sync(
                conn,
                signal_id,
                inventory_repository=inventory_repository,
                actual_profit_usd=actual_profit_usd,
                actual_volume_usdt=actual_volume_usdt,
                note=note,
                close_matching_route=close_matching_route,
            )
        )

    def _finalize_signal_execution_sync(
        self,
        conn: sqlite3.Connection,
        signal_id: int,
        *,
        inventory_repository: InventoryRepository,
        actual_profit_usd: Decimal | None,
        actual_volume_usdt: Decimal | None,
        note: str | None,
        close_matching_route: bool,
    ) -> dict[str, object] | None:
        row = self._get_sync(conn, signal_id)
        if row is None:
            return None

        now = datetime.now(timezone.utc).isoformat()
        final_note = note if note is not None else str(row.get("note") or "")
        route_peer_already_applied = False
        profit_value = row.get("actual_profit_usd")
        volume_value = row.get("actual_volume_usdt")

        if str(row.get("status") or "") != "new":
            route_peer_already_applied = self._matching_route_inventory_applied_sync(
                conn,
                row,
                exclude_signal_id=signal_id,
            )

        if actual_profit_usd is not None:
            profit_value = float(actual_profit_usd)
        if actual_volume_usdt is not None:
            volume_value = float(actual_volume_usdt)
        elif actual_profit_usd is not None:
            volume_value = float(Decimal(str(row.get("volume_usdt") or "0")))

        if close_matching_route:
            conn.execute(
                """
                UPDATE signal_journal
                SET status = ?, note = ?, updated_at = ?
                WHERE route = ?
                  AND buy_platform = ?
                  AND sell_platform = ?
                  AND buy_fiat = ?
                  AND sell_fiat = ?
                  AND status = 'new'
                """,
                (
                    "done",
                    final_note,
                    now,
                    str(row["route"]),
                    str(row["buy_platform"]),
                    str(row["sell_platform"]),
                    str(row["buy_fiat"]),
                    str(row["sell_fiat"]),
                ),
            )

        conn.execute(
            """
            UPDATE signal_journal
            SET status = ?, actual_profit_usd = ?, actual_volume_usdt = ?, note = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                "done",
                profit_value,
                volume_value,
                final_note,
                now,
                signal_id,
            ),
        )

        updated = self._get_sync(conn, signal_id)
        if updated is None:
            return None
        if int(updated.get("inventory_applied") or 0) == 0:
            if not route_peer_already_applied:
                inventory_repository._apply_signal_execution_sync(
                    conn,
                    updated,
                    actual_profit_usd=actual_profit_usd,
                    actual_volume_usdt=actual_volume_usdt,
                )
            conn.execute(
                """
                UPDATE signal_journal
                SET inventory_applied = 1, updated_at = ?
                WHERE id = ?
                """,
                (now, signal_id),
            )
            updated = self._get_sync(conn, signal_id)
        return updated

    @staticmethod
    def _get_sync(conn: sqlite3.Connection, signal_id: int) -> dict[str, object] | None:
        row = conn.execute(
            "SELECT * FROM signal_journal WHERE id = ?",
            (signal_id,),
        ).fetchone()
        return dict(row) if row is not None else None

    @staticmethod
    def _matching_route_inventory_applied_sync(
        conn: sqlite3.Connection,
        row: dict[str, object],
        *,
        exclude_signal_id: int,
    ) -> bool:
        match = conn.execute(
            """
            SELECT id
            FROM signal_journal
            WHERE route = ?
              AND buy_platform = ?
              AND sell_platform = ?
              AND buy_fiat = ?
              AND sell_fiat = ?
              AND inventory_applied = 1
              AND id != ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (
                str(row["route"]),
                str(row["buy_platform"]),
                str(row["sell_platform"]),
                str(row["buy_fiat"]),
                str(row["sell_fiat"]),
                exclude_signal_id,
            ),
        ).fetchone()
        return match is not None

    async def mark_inventory_applied(self, signal_id: int) -> int:
        return await self.db.execute(
            """
            UPDATE signal_journal
            SET inventory_applied = 1, updated_at = ?
            WHERE id = ?
            """,
            (
                datetime.now(timezone.utc).isoformat(),
                signal_id,
            ),
        )

    async def list_recent(self, limit: int = 20) -> list[dict[str, object]]:
        rows = await self.db.fetchall(
            """
            SELECT *
            FROM signal_journal
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in rows]

    async def latest_for_route(
        self,
        *,
        route: str,
        buy_platform: str,
        sell_platform: str,
        buy_fiat: str,
        sell_fiat: str,
        since_iso: str,
    ) -> dict[str, object] | None:
        row = await self.db.fetchone(
            """
            SELECT *
            FROM signal_journal
            WHERE route = ?
              AND buy_platform = ?
              AND sell_platform = ?
              AND buy_fiat = ?
              AND sell_fiat = ?
              AND created_at >= ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (
                route,
                buy_platform,
                sell_platform,
                buy_fiat,
                sell_fiat,
                since_iso,
            ),
        )
        return dict(row) if row is not None else None

    async def has_open_route(
        self,
        *,
        route: str,
        buy_platform: str,
        sell_platform: str,
        buy_fiat: str,
        sell_fiat: str,
    ) -> bool:
        row = await self.db.fetchone(
            """
            SELECT id
            FROM signal_journal
            WHERE route = ?
              AND buy_platform = ?
              AND sell_platform = ?
              AND buy_fiat = ?
              AND sell_fiat = ?
              AND status = 'new'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (
                route,
                buy_platform,
                sell_platform,
                buy_fiat,
                sell_fiat,
            ),
        )
        return row is not None

    async def list_open_routes(self) -> list[dict[str, object]]:
        rows = await self.db.fetchall(
            """
            SELECT *
            FROM signal_journal
            WHERE status = 'new'
            ORDER BY created_at DESC
            """
        )
        return [dict(row) for row in rows]

    async def expire_open_routes_except(
        self,
        active_routes: set[tuple[str, str, str, str, str]],
        *,
        note: str = "expired automatically: route no longer executable",
        stale_after_sec: int = 120,
    ) -> int:
        open_rows = await self.list_open_routes()
        expired = 0
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat()
        for row in open_rows:
            route_key = (
                str(row["route"]),
                str(row["buy_platform"]),
                str(row["sell_platform"]),
                str(row["buy_fiat"]),
                str(row["sell_fiat"]),
            )
            if route_key in active_routes:
                await self.db.execute(
                    """
                    UPDATE signal_journal
                    SET updated_at = ?
                    WHERE id = ?
                      AND status = 'new'
                    """,
                    (
                        now,
                        int(row["id"]),
                    ),
                )
                continue
            last_seen_raw = str(row.get("updated_at") or row.get("created_at") or now)
            try:
                last_seen = datetime.fromisoformat(last_seen_raw)
            except ValueError:
                last_seen = now_dt
            if (now_dt - last_seen).total_seconds() < stale_after_sec:
                continue
            existing_note = str(row.get("note") or "").strip()
            final_note = note if not existing_note else f"{existing_note} | {note}"
            await self.db.execute(
                """
                UPDATE signal_journal
                SET status = 'expired', note = ?, updated_at = ?
                WHERE id = ?
                  AND status = 'new'
                """,
                (
                    final_note,
                    now,
                    int(row["id"]),
                ),
            )
            expired += 1
        return expired

    async def period_summary(self, days: int = 7) -> dict[str, object]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        row = await self.db.fetchone(
            """
            SELECT
                COUNT(*) AS total_signals,
                COALESCE(SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END), 0) AS done_count,
                COALESCE(SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END), 0) AS skipped_count,
                COALESCE(SUM(CASE WHEN status = 'problem' THEN 1 ELSE 0 END), 0) AS problem_count,
                COALESCE(
                    SUM(
                        CASE
                            WHEN status = 'done' THEN COALESCE(actual_volume_usdt, volume_usdt)
                            ELSE 0
                        END
                    ),
                    0
                ) AS total_volume_usdt,
                COALESCE(
                    SUM(
                        CASE
                            WHEN status = 'done' THEN COALESCE(actual_profit_usd, estimated_profit_usd)
                            ELSE 0
                        END
                    ),
                    0
                ) AS total_profit_usd
            FROM signal_journal
            WHERE created_at >= ?
            """,
            (cutoff,),
        )
        return dict(row) if row is not None else {
            "total_signals": 0,
            "done_count": 0,
            "skipped_count": 0,
            "problem_count": 0,
            "total_volume_usdt": 0.0,
            "total_profit_usd": 0.0,
        }


class TradeRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def upsert(self, trade: Trade) -> int:
        return await self.db.execute(
            """
            INSERT INTO trades (
                platform, trade_id, side, fiat, price, volume_usdt, volume_fiat,
                profit_usd, counterparty_id, counterparty_rating, status, opened_at, closed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_id) DO UPDATE SET
                status = excluded.status,
                profit_usd = excluded.profit_usd,
                closed_at = excluded.closed_at
            """,
            (
                trade.platform,
                trade.trade_id,
                trade.side,
                trade.fiat,
                float(trade.price),
                float(trade.volume_usdt),
                float(trade.volume_fiat),
                float(trade.profit_usd),
                trade.counterparty_id,
                float(trade.counterparty_rating),
                trade.status,
                trade.opened_at.isoformat(),
                trade.closed_at.isoformat() if trade.closed_at else None,
            ),
        )

    async def daily_summary(self, day: date) -> dict[str, object]:
        row = await self.db.fetchone(
            """
            SELECT
                COUNT(*) AS total_trades,
                COALESCE(SUM(volume_usdt), 0) AS total_volume_usdt,
                COALESCE(SUM(profit_usd), 0) AS net_profit_usd
            FROM trades
            WHERE substr(opened_at, 1, 10) = ?
            """,
            (day.isoformat(),),
        )
        if row is None:
            return {
                "total_trades": 0,
                "total_volume_usdt": 0.0,
                "net_profit_usd": 0.0,
            }
        return dict(row)


class InventoryRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def list_positions(self) -> list[dict[str, object]]:
        rows = await self.db.fetchall(
            """
            SELECT location, asset, amount, updated_at
            FROM inventory_positions
            ORDER BY location, asset
            """
        )
        return [dict(row) for row in rows]

    async def get_position(self, location: str, asset: str) -> dict[str, object] | None:
        row = await self.db.fetchone(
            """
            SELECT location, asset, amount, updated_at
            FROM inventory_positions
            WHERE location = ? AND asset = ?
            """,
            (location, asset.upper()),
        )
        return dict(row) if row is not None else None

    async def set_balance(self, location: str, asset: str, amount: Decimal, reason: str = "manual_set") -> None:
        current = await self.get_position(location, asset)
        current_amount = Decimal(str((current or {}).get("amount") or "0"))
        delta = amount - current_amount
        await self.apply_delta(location, asset, delta, reason)

    async def apply_delta(
        self,
        location: str,
        asset: str,
        delta: Decimal,
        reason: str,
        signal_id: int | None = None,
    ) -> None:
        await self.db.run_in_transaction(
            lambda conn: self._apply_delta_sync(conn, location, asset, delta, reason, signal_id)
        )

    @classmethod
    def _apply_delta_sync(
        cls,
        conn: sqlite3.Connection,
        location: str,
        asset: str,
        delta: Decimal,
        reason: str,
        signal_id: int | None = None,
    ) -> None:
        asset = asset.upper()
        now = datetime.now(timezone.utc).isoformat()
        current = cls._get_position_sync(conn, location, asset)
        current_amount = Decimal(str((current or {}).get("amount") or "0"))
        new_amount = current_amount + delta
        if current is None:
            conn.execute(
                """
                INSERT INTO inventory_positions (location, asset, amount, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (location, asset, float(new_amount), now),
            )
        else:
            conn.execute(
                """
                UPDATE inventory_positions
                SET amount = ?, updated_at = ?
                WHERE location = ? AND asset = ?
                """,
                (float(new_amount), now, location, asset),
            )
        conn.execute(
            """
            INSERT INTO inventory_ledger (location, asset, delta, reason, signal_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (location, asset, float(delta), reason, signal_id, now),
        )

    async def apply_signal_execution(
        self,
        signal: dict[str, Any],
        *,
        actual_profit_usd: Decimal | None = None,
        actual_volume_usdt: Decimal | None = None,
    ) -> None:
        await self.db.run_in_transaction(
            lambda conn: self._apply_signal_execution_sync(
                conn,
                signal,
                actual_profit_usd=actual_profit_usd,
                actual_volume_usdt=actual_volume_usdt,
            )
        )

    @classmethod
    def _apply_signal_execution_sync(
        cls,
        conn: sqlite3.Connection,
        signal: dict[str, Any],
        *,
        actual_profit_usd: Decimal | None = None,
        actual_volume_usdt: Decimal | None = None,
    ) -> None:
        payload_raw = signal.get("payload_json") or "{}"
        if isinstance(payload_raw, bytes):
            payload_raw = payload_raw.decode("utf-8", errors="ignore")
        payload = json.loads(str(payload_raw) or "{}")
        signal_id = int(signal["id"])
        start_volume = actual_volume_usdt if actual_volume_usdt is not None else Decimal(
            str(signal.get("actual_volume_usdt") or signal.get("volume_usdt") or payload.get("volume_usdt") or "0")
        )
        profit = actual_profit_usd if actual_profit_usd is not None else Decimal(
            str(signal.get("actual_profit_usd") or signal.get("estimated_profit_usd") or payload.get("estimated_profit_usd") or "0")
        )
        final_usdt = start_volume + profit

        buy = payload.get("buy_order") or {}
        sell = payload.get("sell_order") or {}
        buy_price = Decimal(str(buy.get("price") or "0"))
        sell_price = Decimal(str(sell.get("price") or "0"))
        sell_fiat = str(sell.get("fiat") or "").upper()
        buy_fiat = str(buy.get("fiat") or "").upper()
        sell_platform = str(sell.get("platform") or "").lower()
        buy_platform = str(buy.get("platform") or "").lower()
        buy_rail = str(payload.get("buy_rail") or "")
        sell_rail = str(payload.get("sell_rail") or "")
        fx_rail = str(payload.get("fx_rail") or "")
        base_asset = str(buy.get("asset") or "USDT").upper()

        start_platform_wallet = f"{sell_platform}:wallet"
        end_platform_wallet = f"{buy_platform}:wallet"
        sell_location = cls._location_for_rail(sell_rail, sell_platform)
        buy_location = cls._location_for_rail(buy_rail, buy_platform)
        fx_location = cls._location_for_rail(fx_rail, sell_platform) if fx_rail else buy_location

        sell_notional = (start_volume * sell_price).quantize(Decimal("0.01")) if sell_price > 0 else Decimal("0")
        rebuy_budget = (final_usdt * buy_price).quantize(Decimal("0.01")) if buy_price > 0 else Decimal("0")

        cls._apply_delta_sync(conn, start_platform_wallet, base_asset, -start_volume, "signal_start_usdt", signal_id)
        if sell_fiat and sell_notional > 0:
            cls._apply_delta_sync(conn, sell_location, sell_fiat, sell_notional, "signal_sell_receive", signal_id)

        if sell_fiat and sell_fiat != buy_fiat and sell_notional > 0:
            if sell_location != fx_location:
                cls._apply_delta_sync(conn, sell_location, sell_fiat, -sell_notional, "signal_move_to_fx", signal_id)
                cls._apply_delta_sync(conn, fx_location, sell_fiat, sell_notional, "signal_move_to_fx", signal_id)
            cls._apply_delta_sync(conn, fx_location, sell_fiat, -sell_notional, "signal_fx_source", signal_id)
            cls._apply_delta_sync(conn, fx_location, buy_fiat, rebuy_budget, "signal_fx_target", signal_id)
        else:
            if sell_location != buy_location and sell_fiat and sell_notional > 0:
                cls._apply_delta_sync(conn, sell_location, sell_fiat, -sell_notional, "signal_move_to_buy", signal_id)
                cls._apply_delta_sync(conn, buy_location, sell_fiat, sell_notional, "signal_move_to_buy", signal_id)

        if buy_fiat and rebuy_budget > 0:
            if fx_location != buy_location and sell_fiat != buy_fiat:
                cls._apply_delta_sync(conn, fx_location, buy_fiat, -rebuy_budget, "signal_move_from_fx", signal_id)
                cls._apply_delta_sync(conn, buy_location, buy_fiat, rebuy_budget, "signal_move_from_fx", signal_id)
            cls._apply_delta_sync(conn, buy_location, buy_fiat, -rebuy_budget, "signal_buy_spend", signal_id)

        cls._apply_delta_sync(conn, end_platform_wallet, base_asset, final_usdt, "signal_finish_usdt", signal_id)

    @staticmethod
    def _get_position_sync(conn: sqlite3.Connection, location: str, asset: str) -> dict[str, object] | None:
        row = conn.execute(
            """
            SELECT location, asset, amount, updated_at
            FROM inventory_positions
            WHERE location = ? AND asset = ?
            """,
            (location, asset.upper()),
        ).fetchone()
        return dict(row) if row is not None else None

    @staticmethod
    def _location_for_rail(rail: str, platform: str) -> str:
        normalized = (rail or "").lower()
        if normalized == "fiat_balance":
            return f"{platform}:fiat_balance"
        if normalized.startswith("revolut_"):
            return "revolut"
        if normalized.startswith("wise_"):
            return "wise"
        if normalized in {"bank_transfer", "bank_fx", "blik", "sepa", "zen"}:
            return "bank"
        return f"{platform}:external"


class ExecutionSessionRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def create(
        self,
        *,
        signal_id: int | None,
        route: str,
        status: str,
        mode: str,
        buy_platform: str,
        sell_platform: str,
        buy_fiat: str,
        sell_fiat: str,
        volume_usdt: Decimal,
        estimated_profit_usd: Decimal,
        treasury_provider: str = "",
        payload: dict[str, Any] | None = None,
        error_text: str = "",
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        return await self.db.execute(
            """
            INSERT INTO execution_sessions (
                signal_id, route, status, mode,
                buy_platform, sell_platform, buy_fiat, sell_fiat,
                volume_usdt, estimated_profit_usd,
                treasury_provider, payload_json, error_text, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal_id,
                route,
                status,
                mode,
                buy_platform,
                sell_platform,
                buy_fiat,
                sell_fiat,
                float(volume_usdt),
                float(estimated_profit_usd),
                treasury_provider,
                json.dumps(payload or {}, ensure_ascii=False),
                error_text,
                now,
                now,
            ),
        )

    async def get(self, session_id: int) -> dict[str, Any] | None:
        row = await self.db.fetchone(
            """
            SELECT *
            FROM execution_sessions
            WHERE id = ?
            """,
            (session_id,),
        )
        return dict(row) if row is not None else None

    async def list_recent(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = await self.db.fetchall(
            """
            SELECT *
            FROM execution_sessions
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in rows]

    async def mark_status(
        self,
        session_id: int,
        status: str,
        *,
        buy_exchange_order_id: str | None = None,
        sell_exchange_order_id: str | None = None,
        error_text: str | None = None,
    ) -> int:
        current = await self.get(session_id)
        if current is None:
            return 0
        now = datetime.now(timezone.utc).isoformat()
        return await self.db.execute(
            """
            UPDATE execution_sessions
            SET status = ?,
                buy_exchange_order_id = ?,
                sell_exchange_order_id = ?,
                error_text = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                status,
                buy_exchange_order_id if buy_exchange_order_id is not None else str(current.get("buy_exchange_order_id") or ""),
                sell_exchange_order_id if sell_exchange_order_id is not None else str(current.get("sell_exchange_order_id") or ""),
                error_text if error_text is not None else str(current.get("error_text") or ""),
                now,
                session_id,
            ),
        )

    async def append_event(
        self,
        session_id: int,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> int:
        return await self.db.execute(
            """
            INSERT INTO execution_session_events (session_id, event_type, payload_json, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                session_id,
                event_type,
                json.dumps(payload or {}, ensure_ascii=False),
                datetime.now(timezone.utc).isoformat(),
            ),
        )

    async def list_events(self, session_id: int) -> list[dict[str, Any]]:
        rows = await self.db.fetchall(
            """
            SELECT *
            FROM execution_session_events
            WHERE session_id = ?
            ORDER BY created_at ASC
            """,
            (session_id,),
        )
        return [dict(row) for row in rows]


class DailyPnlRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def upsert(
        self,
        pnl_date: str,
        total_volume_usd: float,
        total_trades: int,
        gross_profit: float,
        fees: float,
    ) -> int:
        net_profit = gross_profit - fees
        return await self.db.execute(
            """
            INSERT INTO daily_pnl (
                date, total_volume_usd, total_trades, gross_profit, fees, net_profit
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                total_volume_usd = excluded.total_volume_usd,
                total_trades = excluded.total_trades,
                gross_profit = excluded.gross_profit,
                fees = excluded.fees,
                net_profit = excluded.net_profit
            """,
            (
                pnl_date,
                total_volume_usd,
                total_trades,
                gross_profit,
                fees,
                net_profit,
            ),
        )
