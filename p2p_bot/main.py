from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path

import aiohttp

from p2p_bot.config import Settings
from p2p_bot.db.database import Database
from p2p_bot.db.repositories import (
    DailyPnlRepository,
    ExecutionSessionRepository,
    InventoryRepository,
    OpportunityRepository,
    RawOrderRepository,
    SignalJournalRepository,
    TradeRepository,
)
from p2p_bot.modules.analyzer import Analyzer
from p2p_bot.modules.data_maintenance import DataMaintenanceService
from p2p_bot.modules.execution_engine import ExchangeExecutionEngine
from p2p_bot.modules.market_research import MarketResearchService
from p2p_bot.modules.order_manager import OrderManager
from p2p_bot.modules.risk_guard import RiskGuard
from p2p_bot.modules.scanner import Scanner
from p2p_bot.modules.strategy import StrategyEngine
from p2p_bot.modules.telegram_bot import TelegramBotService
from p2p_bot.state import AppState
from p2p_bot.utils.binance_p2p import BinanceP2PClient
from p2p_bot.utils.bingx_api import BingXAPIClient
from p2p_bot.utils.bingx_p2p import BingXP2PClient
from p2p_bot.utils.bybit_p2p import BybitP2PClient
from p2p_bot.utils.forex import ForexClient
from p2p_bot.utils.internal_quotes import InternalQuoteClient
from p2p_bot.utils.logger import setup_logging


async def async_main() -> None:
    settings = Settings.from_env(Path.cwd())
    setup_logging(settings.log_path, settings.log_level)
    logger = logging.getLogger("p2p_bot")

    db = Database(settings.db_path)
    await db.initialize()
    logger.info("Database initialized at %s", settings.db_path)

    raw_order_repository = RawOrderRepository(db, settings=settings)
    opportunity_repository = OpportunityRepository(db)
    trade_repository = TradeRepository(db)
    signal_journal_repository = SignalJournalRepository(db)
    inventory_repository = InventoryRepository(db)
    execution_session_repository = ExecutionSessionRepository(db)
    _ = DailyPnlRepository(db)

    state = AppState()
    risk_guard = RiskGuard(settings, state)
    scan_queue: asyncio.Queue = asyncio.Queue()
    alert_queue: asyncio.Queue = asyncio.Queue()
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    def _spawn(coro: asyncio.Future, name: str) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)

        def _log_failure(done_task: asyncio.Task) -> None:
            if done_task.cancelled():
                return
            exc = done_task.exception()
            if exc is not None:
                logger.exception("Background task %s crashed", name, exc_info=exc)

        task.add_done_callback(_log_failure)
        return task

    timeout = aiohttp.ClientTimeout(total=30)
    headers = {"User-Agent": "p2p-bot/0.1"}
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        binance_client = BinanceP2PClient(session, settings.binance, logging.getLogger("p2p_bot.binance"))
        bingx_api_client = BingXAPIClient(session, settings.bingx, logging.getLogger("p2p_bot.bingx.api"))
        bingx_client = BingXP2PClient(session, settings.bingx, logging.getLogger("p2p_bot.bingx.p2p"))
        bybit_client = BybitP2PClient(session, settings.bybit, logging.getLogger("p2p_bot.bybit"))
        forex_client = ForexClient(
            session,
            logging.getLogger("p2p_bot.forex"),
            cache_minutes=settings.forex_update_interval_min,
        )
        internal_quote_client = InternalQuoteClient(
            session,
            settings,
            logging.getLogger("p2p_bot.internal_quotes"),
        )
        bybit_private_access_reason: str | None = None
        if settings.bybit.has_credentials:
            private_access_ok, bybit_private_access_reason = await bybit_client.has_private_p2p_access()
            if not private_access_ok:
                logger.warning(
                    "Bybit private P2P API unavailable: %s. Market scan can still run via public endpoints.",
                    bybit_private_access_reason,
                )
        if settings.bingx.has_credentials:
            bingx_access_ok, bingx_access_reason = await bingx_api_client.verify_private_access()
            if bingx_access_ok:
                logger.info("BingX API ready: %s", bingx_api_client.capability_snapshot())
            else:
                logger.warning("BingX API unavailable: %s", bingx_access_reason)
        else:
            logger.info("BingX API client prepared, but credentials are not configured.")

        scanner = Scanner(
            settings=settings,
            state=state,
            risk_guard=risk_guard,
            raw_order_repository=raw_order_repository,
            binance_client=binance_client,
            bingx_client=bingx_client,
            bybit_client=bybit_client,
            scan_queue=scan_queue,
            alert_queue=alert_queue,
            logger=logging.getLogger("p2p_bot.scanner"),
        )
        analyzer = Analyzer(
            settings=settings,
            state=state,
            risk_guard=risk_guard,
            raw_order_repository=raw_order_repository,
            opportunity_repository=opportunity_repository,
            signal_journal_repository=signal_journal_repository,
            inventory_repository=inventory_repository,
            forex_client=forex_client,
            internal_quote_client=internal_quote_client,
            scan_queue=scan_queue,
            alert_queue=alert_queue,
            logger=logging.getLogger("p2p_bot.analyzer"),
        )
        maintenance = DataMaintenanceService(
            settings=settings,
            raw_order_repository=raw_order_repository,
            logger=logging.getLogger("p2p_bot.maintenance"),
        )
        order_manager = OrderManager(
            settings=settings,
            state=state,
            risk_guard=risk_guard,
            trade_repository=trade_repository,
            bybit_client=bybit_client,
            alert_queue=alert_queue,
            logger=logging.getLogger("p2p_bot.order_manager"),
        )
        strategy_engine = StrategyEngine(
            settings=settings,
            state=state,
            risk_guard=risk_guard,
            order_manager=order_manager,
            alert_queue=alert_queue,
            logger=logging.getLogger("p2p_bot.strategy"),
        )
        market_research = MarketResearchService(
            settings=settings,
            state=state,
            risk_guard=risk_guard,
            raw_order_repository=raw_order_repository,
            analyzer=analyzer,
            binance_client=binance_client,
            bingx_client=bingx_client,
            bybit_client=bybit_client,
            logger=logging.getLogger("p2p_bot.market"),
        )
        execution_engine = ExchangeExecutionEngine(
            settings=settings,
            signal_journal_repository=signal_journal_repository,
            execution_session_repository=execution_session_repository,
            inventory_repository=inventory_repository,
            bybit_client=bybit_client,
            binance_client=binance_client,
            logger=logging.getLogger("p2p_bot.execution"),
        )
        telegram_bot = TelegramBotService(
            session=session,
            settings=settings,
            state=state,
            risk_guard=risk_guard,
            order_manager=order_manager,
            execution_engine=execution_engine,
            market_research=market_research,
            raw_order_repository=raw_order_repository,
            trade_repository=trade_repository,
            signal_journal_repository=signal_journal_repository,
            inventory_repository=inventory_repository,
            alert_queue=alert_queue,
            logger=logging.getLogger("p2p_bot.telegram"),
        )
        logger.info("Exchange execution scaffold ready: %s", execution_engine.capability_snapshot())

        tasks = [
            _spawn(maintenance.run(stop_event), "maintenance"),
            _spawn(scanner.run(stop_event), "scanner"),
            _spawn(analyzer.run(stop_event), "analyzer"),
        ]
        if order_manager.enabled:
            tasks.append(_spawn(order_manager.monitor_pending_orders(stop_event), "order_monitor"))
            tasks.append(_spawn(order_manager.price_optimizer_loop(stop_event), "price_optimizer"))
            if strategy_engine.enabled:
                tasks.append(_spawn(strategy_engine.run(stop_event), "strategy"))
        else:
            if settings.bybit.has_credentials:
                logger.warning(
                    "Bybit order manager disabled: %s",
                    bybit_private_access_reason or "private P2P advertiser API access is unavailable",
                )
            else:
                logger.warning("Bybit order manager disabled: missing API credentials.")
        if telegram_bot.enabled:
            tasks.append(_spawn(telegram_bot.run(stop_event), "telegram"))
        else:
            logger.warning("Telegram disabled: missing TELEGRAM_BOT_TOKEN.")

        try:
            await stop_event.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("Shutting down.")


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
