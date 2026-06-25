from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Callable, Iterable, Sequence, TypeVar


T = TypeVar("T")


SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS raw_orders (
        id INTEGER PRIMARY KEY,
        platform TEXT NOT NULL,
        order_id TEXT NOT NULL,
        side TEXT NOT NULL,
        asset TEXT NOT NULL,
        fiat TEXT NOT NULL,
        price REAL NOT NULL,
        min_amount REAL NOT NULL,
        max_amount REAL NOT NULL,
        available REAL NOT NULL,
        payment_methods TEXT NOT NULL,
        merchant_id TEXT NOT NULL,
        merchant_rating REAL NOT NULL,
        merchant_orders INTEGER NOT NULL,
        merchant_days INTEGER NOT NULL,
        scraped_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS opportunities (
        id INTEGER PRIMARY KEY,
        type TEXT NOT NULL,
        spread_pct REAL NOT NULL,
        estimated_profit REAL NOT NULL,
        volume_usdt REAL NOT NULL,
        buy_platform TEXT NOT NULL,
        sell_platform TEXT NOT NULL,
        detected_at TEXT NOT NULL,
        status TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY,
        platform TEXT NOT NULL,
        trade_id TEXT NOT NULL UNIQUE,
        side TEXT NOT NULL,
        fiat TEXT NOT NULL,
        price REAL NOT NULL,
        volume_usdt REAL NOT NULL,
        volume_fiat REAL NOT NULL,
        profit_usd REAL NOT NULL,
        counterparty_id TEXT NOT NULL,
        counterparty_rating REAL NOT NULL,
        status TEXT NOT NULL,
        opened_at TEXT NOT NULL,
        closed_at TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS signal_journal (
        id INTEGER PRIMARY KEY,
        alert_fingerprint TEXT NOT NULL,
        opportunity_type TEXT NOT NULL,
        route TEXT NOT NULL,
        buy_platform TEXT NOT NULL,
        sell_platform TEXT NOT NULL,
        buy_fiat TEXT NOT NULL,
        sell_fiat TEXT NOT NULL,
        volume_usdt REAL NOT NULL,
        estimated_profit_usd REAL NOT NULL,
        gross_profit_usd REAL NOT NULL,
        fees_usd REAL NOT NULL,
        rail_status TEXT NOT NULL,
        liquidity_status TEXT NOT NULL,
        status TEXT NOT NULL,
        note TEXT NOT NULL DEFAULT '',
        payload_json TEXT NOT NULL DEFAULT '{}',
        inventory_applied INTEGER NOT NULL DEFAULT 0,
        actual_profit_usd REAL,
        actual_volume_usdt REAL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS inventory_positions (
        id INTEGER PRIMARY KEY,
        location TEXT NOT NULL,
        asset TEXT NOT NULL,
        amount REAL NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(location, asset)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS inventory_ledger (
        id INTEGER PRIMARY KEY,
        location TEXT NOT NULL,
        asset TEXT NOT NULL,
        delta REAL NOT NULL,
        reason TEXT NOT NULL,
        signal_id INTEGER,
        created_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS daily_pnl (
        date TEXT PRIMARY KEY,
        total_volume_usd REAL NOT NULL,
        total_trades INTEGER NOT NULL,
        gross_profit REAL NOT NULL,
        fees REAL NOT NULL,
        net_profit REAL NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS execution_sessions (
        id INTEGER PRIMARY KEY,
        signal_id INTEGER,
        route TEXT NOT NULL,
        status TEXT NOT NULL,
        mode TEXT NOT NULL,
        buy_platform TEXT NOT NULL,
        sell_platform TEXT NOT NULL,
        buy_fiat TEXT NOT NULL,
        sell_fiat TEXT NOT NULL,
        volume_usdt REAL NOT NULL,
        estimated_profit_usd REAL NOT NULL,
        buy_exchange_order_id TEXT NOT NULL DEFAULT '',
        sell_exchange_order_id TEXT NOT NULL DEFAULT '',
        treasury_provider TEXT NOT NULL DEFAULT '',
        payload_json TEXT NOT NULL DEFAULT '{}',
        error_text TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS execution_session_events (
        id INTEGER PRIMARY KEY,
        session_id INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        payload_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS market_activity_rollups (
        id INTEGER PRIMARY KEY,
        window_hours INTEGER NOT NULL,
        platform TEXT NOT NULL,
        asset TEXT NOT NULL,
        fiat TEXT NOT NULL,
        side TEXT NOT NULL,
        unique_ads INTEGER NOT NULL,
        unique_merchants INTEGER NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(window_hours, platform, asset, fiat, side)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_raw_orders_pair ON raw_orders(platform, fiat, side, scraped_at);",
    "CREATE INDEX IF NOT EXISTS idx_raw_orders_pair_asset ON raw_orders(platform, asset, fiat, side, scraped_at);",
    "CREATE INDEX IF NOT EXISTS idx_raw_orders_scraped_asset_fiat_side_platform ON raw_orders(scraped_at, asset, fiat, side, platform);",
    "CREATE INDEX IF NOT EXISTS idx_opportunities_detected_at ON opportunities(detected_at);",
    "CREATE INDEX IF NOT EXISTS idx_trades_opened_at ON trades(opened_at);",
    "CREATE INDEX IF NOT EXISTS idx_signal_journal_created_at ON signal_journal(created_at);",
    "CREATE INDEX IF NOT EXISTS idx_signal_journal_status ON signal_journal(status, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_inventory_positions_location_asset ON inventory_positions(location, asset);",
    "CREATE INDEX IF NOT EXISTS idx_inventory_ledger_signal_id ON inventory_ledger(signal_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_execution_sessions_status_created_at ON execution_sessions(status, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_execution_sessions_signal_id ON execution_sessions(signal_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_execution_session_events_session_id ON execution_session_events(session_id, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_market_activity_rollups_window_asset_fiat ON market_activity_rollups(window_hours, asset, fiat, updated_at);",
)


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize_sync)

    def _initialize_sync(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            for statement in SCHEMA:
                if statement.lstrip().upper().startswith("CREATE INDEX"):
                    continue
                conn.execute(statement)
            self._ensure_column(conn, "raw_orders", "asset", "TEXT NOT NULL DEFAULT 'USDT'")
            self._ensure_column(conn, "raw_orders", "merchant_id", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "opportunities", "buy_fiat", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "opportunities", "sell_fiat", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "opportunities", "rail_status", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "opportunities", "liquidity_status", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "opportunities", "gross_profit_usd", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(conn, "opportunities", "total_fees_usd", "REAL NOT NULL DEFAULT 0")
            self._ensure_column(conn, "opportunities", "note", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "opportunities", "payload_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column(conn, "signal_journal", "payload_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column(conn, "signal_journal", "inventory_applied", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "execution_sessions", "signal_id", "INTEGER")
            self._ensure_column(conn, "execution_sessions", "payload_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column(conn, "execution_sessions", "error_text", "TEXT NOT NULL DEFAULT ''")
            for statement in SCHEMA:
                if statement.lstrip().upper().startswith("CREATE INDEX"):
                    conn.execute(statement)
            conn.commit()

    def _ensure_column(
        self,
        conn: sqlite3.Connection,
        table: str,
        column: str,
        definition: str,
    ) -> None:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        existing = {str(row[1]) for row in rows}
        if column not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    async def run_in_transaction(self, operation: Callable[[sqlite3.Connection], T]) -> T:
        return await asyncio.to_thread(self._run_in_transaction_sync, operation)

    def _run_in_transaction_sync(self, operation: Callable[[sqlite3.Connection], T]) -> T:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                result = operation(conn)
            except Exception:
                conn.rollback()
                raise
            conn.commit()
            return result

    async def execute(self, sql: str, params: Sequence[object] = ()) -> int:
        return await asyncio.to_thread(self._execute_sync, sql, params)

    def _execute_sync(self, sql: str, params: Sequence[object]) -> int:
        with self._connect() as conn:
            cursor = conn.execute(sql, tuple(params))
            conn.commit()
            return int(cursor.lastrowid or 0)

    async def execute_rowcount(self, sql: str, params: Sequence[object] = ()) -> int:
        return await asyncio.to_thread(self._execute_rowcount_sync, sql, params)

    def _execute_rowcount_sync(self, sql: str, params: Sequence[object]) -> int:
        with self._connect() as conn:
            cursor = conn.execute(sql, tuple(params))
            conn.commit()
            return int(cursor.rowcount or 0)

    async def executemany(self, sql: str, rows: Iterable[Sequence[object]]) -> int:
        return await asyncio.to_thread(self._executemany_sync, sql, list(rows))

    def _executemany_sync(self, sql: str, rows: list[Sequence[object]]) -> int:
        with self._connect() as conn:
            cursor = conn.executemany(sql, rows)
            conn.commit()
            return cursor.rowcount

    async def fetchall(self, sql: str, params: Sequence[object] = ()) -> list[sqlite3.Row]:
        return await asyncio.to_thread(self._fetchall_sync, sql, params)

    def _fetchall_sync(self, sql: str, params: Sequence[object]) -> list[sqlite3.Row]:
        with self._connect() as conn:
            cursor = conn.execute(sql, tuple(params))
            return list(cursor.fetchall())

    async def fetchone(self, sql: str, params: Sequence[object] = ()) -> sqlite3.Row | None:
        return await asyncio.to_thread(self._fetchone_sync, sql, params)

    def _fetchone_sync(self, sql: str, params: Sequence[object]) -> sqlite3.Row | None:
        with self._connect() as conn:
            cursor = conn.execute(sql, tuple(params))
            return cursor.fetchone()
