from __future__ import annotations

import asyncio
import html
import json
import logging
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import quote

import aiohttp

from p2p_bot.config import Settings, TelegramConfig
from p2p_bot.db.repositories import InventoryRepository, RawOrderRepository, SignalJournalRepository, TradeRepository
from p2p_bot.models.events import Event
from p2p_bot.models.opportunity import ArbitrageOpportunity
from p2p_bot.models.order import P2POrder
from p2p_bot.modules.execution_engine import ExchangeExecutionEngine
from p2p_bot.modules.market_research import MarketResearchService
from p2p_bot.modules.order_manager import OrderManager
from p2p_bot.modules.risk_guard import RiskGuard
from p2p_bot.state import AppState
from p2p_bot.utils.canonical_side import bybit_web_action, order_action_url as build_order_action_url
from p2p_bot.utils.http import request_json


class TelegramBotService:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        settings: Settings,
        state: AppState,
        risk_guard: RiskGuard,
        order_manager: OrderManager,
        execution_engine: ExchangeExecutionEngine | None,
        market_research: MarketResearchService,
        raw_order_repository: RawOrderRepository,
        trade_repository: TradeRepository,
        signal_journal_repository: SignalJournalRepository,
        inventory_repository: InventoryRepository,
        alert_queue: asyncio.Queue[Event],
        logger: logging.Logger,
    ) -> None:
        self.session = session
        self.settings = settings
        self.config: TelegramConfig = settings.telegram
        self.state = state
        self.risk_guard = risk_guard
        self.order_manager = order_manager
        self.execution_engine = execution_engine
        self.market_research = market_research
        self.raw_order_repository = raw_order_repository
        self.trade_repository = trade_repository
        self.signal_journal_repository = signal_journal_repository
        self.inventory_repository = inventory_repository
        self.alert_queue = alert_queue
        self.logger = logger
        self._update_offset = 0
        self._discovered_chat_id: str | None = None

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    async def run(self, stop_event: asyncio.Event) -> None:
        if not self.enabled:
            self.logger.warning("Telegram bot is disabled: no TELEGRAM_BOT_TOKEN")
            return
        if not self.config.chat_id:
            self.logger.warning(
                "Telegram bot started without TELEGRAM_CHAT_ID. Commands are disabled until /start is received and TELEGRAM_CHAT_ID is configured."
            )
        tasks = [asyncio.create_task(self._consume_alerts(stop_event))]
        if self.config.polling_enabled:
            tasks.append(asyncio.create_task(self._poll_updates(stop_event)))
        else:
            self.logger.info("Telegram polling disabled: send-only mode enabled.")
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _poll_updates(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                response = await self._api(
                    "getUpdates",
                    {
                        "offset": self._update_offset,
                        "timeout": self.config.poll_timeout_sec,
                        "allowed_updates": ["message", "callback_query"],
                    },
                    timeout_sec=float(self.config.poll_timeout_sec + 15),
                    retries=2,
                    backoff=(1.0, 3.0),
                )
                for update in response.get("result", []):
                    self._update_offset = int(update["update_id"]) + 1
                    await self._handle_update(update)
            except Exception as exc:
                self.logger.warning(
                    "Telegram polling failed (%s): %r",
                    type(exc).__name__,
                    exc,
                )
                await asyncio.sleep(3)

    async def _consume_alerts(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                event = await asyncio.wait_for(self.alert_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self._dispatch_event(event)
            except Exception:
                self.logger.exception("Alert dispatch failed for event %s", event.type)

    async def _handle_update(self, update: dict[str, Any]) -> None:
        if "message" in update:
            await self._handle_message(update["message"])
        elif "callback_query" in update:
            await self._handle_callback(update["callback_query"])

    async def _handle_message(self, message: dict[str, Any]) -> None:
        chat_id = str(((message.get("chat") or {}).get("id")))

        text = str(message.get("text") or "").strip()
        if not text:
            return

        if self.config.chat_id is None and text.startswith("/start"):
            self._discovered_chat_id = chat_id
            self.logger.warning("Telegram discovery chat_id observed via /start: %s", chat_id)
            await self.send_message(
                "Telegram chat is not authorized yet.\n"
                f"chat_id={chat_id}\n"
                "Set TELEGRAM_CHAT_ID to this value and restart the bot.",
                chat_id_override=chat_id,
            )
            return

        if not self._is_authorized_chat(chat_id):
            self.logger.warning("Ignored Telegram message from unauthorized chat_id=%s", chat_id)
            return

        if text.startswith("/start"):
            await self.send_message(
                "P2P bot online.\n"
                f"chat_id={chat_id}\n"
                f"last_scan={self.state.last_scan_at.isoformat() if self.state.last_scan_at else 'n/a'}"
            )
            return

        if text.startswith("/status"):
            await self.send_message(await self._status_text())
            return

        if text.startswith("/pause"):
            self.risk_guard.pause("Manual pause via Telegram")
            paused = await self.order_manager.pause_all_active_orders() if self.order_manager.enabled else 0
            await self.send_message(f"Paused. Ads paused: {paused}")
            return

        if text.startswith("/resume"):
            self.risk_guard.resume()
            resumed = await self.order_manager.resume_paused_orders() if self.order_manager.enabled else 0
            await self.send_message(f"Resumed. Ads restored: {resumed}")
            return

        if text.startswith("/report"):
            signal_summary = await self.signal_journal_repository.period_summary(1)
            summary = await self.trade_repository.daily_summary(date.today())
            await self.send_message(
                "Сегодня:\n"
                f"сигналов={signal_summary['total_signals']}\n"
                f"сделано={signal_summary['done_count']}\n"
                f"пропущено={signal_summary['skipped_count']}\n"
                f"проблема={signal_summary['problem_count']}\n"
                f"оборот_usdt={signal_summary['total_volume_usdt']}\n"
                f"прибыль_usd={signal_summary['total_profit_usd']}\n"
                f"booked_trades={summary['total_trades']}\n"
                f"booked_volume_usdt={summary['total_volume_usdt']}\n"
                f"booked_profit_usd={summary['net_profit_usd']}"
            )
            return

        if text.startswith("/spreads"):
            if not self.state.latest_spreads:
                await self.send_message("No spread data yet.")
                return
            lines = ["Current spreads:"]
            for key, value in sorted(self.state.latest_spreads.items()):
                lines.append(f"{key} = {value:.4f}%")
            await self.send_message("\n".join(lines))
            return

        if text.startswith("/opps"):
            actionable = [item for item in self.state.latest_opportunities if self._is_alert_candidate(item)]
            if not actionable:
                await self.send_message("No opportunities yet.")
                return
            lines = ["Top confirmed opportunities:"]
            for item in actionable[:5]:
                lines.append(
                    f"{item.type} | {item.rail_status}/{item.liquidity_status} | "
                    f"{self._route_text(item)} | {item.spread_pct:.4f}% | profit≈{item.estimated_profit_usd}"
                )
            await self.send_message("\n".join(lines))
            return

        if text.startswith("/signals"):
            items = await self.signal_journal_repository.list_recent(limit=10)
            if not items:
                await self.send_message("История сигналов пока пустая.")
                return
            lines = ["Последние сигналы:"]
            for item in items:
                route = self._signal_route_text(item)
                lines.append(
                    f"#{item['id']} | {item['status']} | {route} | "
                    f"profit≈{item['estimated_profit_usd']} USD"
                )
            await self.send_message("\n".join(lines))
            return

        if text.startswith("/execs"):
            if self.execution_engine is None:
                await self.send_message("Execution engine пока не подключён.")
                return
            sessions = await self.execution_engine.execution_session_repository.list_recent(limit=10)
            if not sessions:
                await self.send_message("Execution sessions пока пусты.")
                return
            lines = ["Последние execution sessions:"]
            for item in sessions:
                lines.append(
                    f"#{item['id']} | {item['status']} | {str(item['sell_platform']).upper()}->{str(item['buy_platform']).upper()} "
                    f"{item['sell_fiat']}->{item['buy_fiat']} | {item['volume_usdt']} {self.settings.base_asset} | "
                    f"profit≈{item['estimated_profit_usd']} USD"
                )
            await self.send_message("\n".join(lines))
            return

        if text.startswith("/rebalance"):
            if self.execution_engine is None:
                await self.send_message("Execution engine пока не подключён.")
                return
            snapshot = await self.execution_engine.rebalance_snapshot()
            await self.send_message(self._rebalance_text(snapshot))
            return

        if text.startswith("/statement"):
            parts = text.split()
            days = 7
            if len(parts) == 2:
                try:
                    days = max(1, int(parts[1]))
                except ValueError:
                    await self.send_message("Usage: /statement 7")
                    return
            summary = await self.signal_journal_repository.period_summary(days)
            await self.send_message(
                f"Выписка за {days} дн.:\n"
                f"сигналов={summary['total_signals']}\n"
                f"сделано={summary['done_count']}\n"
                f"пропущено={summary['skipped_count']}\n"
                f"проблема={summary['problem_count']}\n"
                f"оборот_usdt={summary['total_volume_usdt']}\n"
                f"прибыль_usd={summary['total_profit_usd']}"
            )
            return

        if text.startswith("/inventory_help") or text.startswith("/inventory_init"):
            await self.send_message(self._inventory_help_text())
            return

        if text.startswith("/inventory"):
            positions = await self.inventory_repository.list_positions()
            if not positions:
                await self.send_message(
                    "Инвентарь пока пуст.\n\n"
                    "Для инициализации используйте /set_balance или откройте /inventory_help."
                )
                return
            grouped: dict[str, list[dict[str, object]]] = {}
            for item in positions:
                grouped.setdefault(str(item["location"]), []).append(item)
            lines = ["Инвентарь:"]
            for location in sorted(grouped):
                lines.append(f"\n{location}")
                for item in sorted(grouped[location], key=lambda row: str(row["asset"])):
                    lines.append(f"{item['asset']} = {Decimal(str(item['amount'])):.4f}")
            await self.send_message("\n".join(lines))
            return

        if text.startswith("/set_balance"):
            parts = text.split()
            if len(parts) != 4:
                await self.send_message(self._inventory_usage_text())
                return
            try:
                location = self._normalize_inventory_location(parts[1].strip())
                asset = parts[2].strip().upper()
                amount = Decimal(parts[3])
            except (InvalidOperation, ValueError):
                await self.send_message(self._inventory_usage_text())
                return
            if amount < 0:
                await self.send_message("Для /set_balance укажите неотрицательный остаток.")
                return
            await self.inventory_repository.set_balance(location, asset, amount, "telegram_set_balance")
            await self.send_message(f"Баланс обновлён: {location} {asset} = {self._fmt_decimal(amount, 4)}")
            return

        if text.startswith("/actual"):
            parts = text.split()
            if len(parts) not in {3, 4}:
                await self.send_message("Usage: /actual 123 31.5 or /actual 123 31.5 2800")
                return
            try:
                signal_id = int(parts[1])
                actual_profit = Decimal(parts[2])
                actual_volume = Decimal(parts[3]) if len(parts) == 4 else None
            except (ValueError, InvalidOperation):
                await self.send_message("Usage: /actual 123 31.5 or /actual 123 31.5 2800")
                return
            signal = await self.signal_journal_repository.get(signal_id)
            if signal is None:
                await self.send_message(f"Сигнал #{signal_id} не найден.")
                return
            await self.signal_journal_repository.finalize_signal_execution(
                signal_id,
                inventory_repository=self.inventory_repository,
                actual_profit_usd=actual_profit,
                actual_volume_usdt=actual_volume,
            )
            await self.send_message(
                f"Сигнал #{signal_id} обновлен.\n"
                f"actual_profit_usd={actual_profit}\n"
                f"actual_volume_usdt={actual_volume or signal['volume_usdt']}"
            )
            return

        if text.startswith("/note"):
            parts = text.split(maxsplit=2)
            if len(parts) != 3:
                await self.send_message("Usage: /note 123 текст")
                return
            try:
                signal_id = int(parts[1])
            except ValueError:
                await self.send_message("Usage: /note 123 текст")
                return
            note = parts[2].strip()
            signal = await self.signal_journal_repository.get(signal_id)
            if signal is None:
                await self.send_message(f"Сигнал #{signal_id} не найден.")
                return
            await self.signal_journal_repository.append_note(signal_id, note)
            await self.send_message(f"Заметка для сигнала #{signal_id} сохранена.")
            return

        if text.startswith("/filters"):
            lines = [
                "Alert filters:",
                f"default_min_spread_alert_pct={self.settings.min_spread_alert_pct}",
                f"default_min_alert_profit_usd={self.settings.min_alert_profit_usd}",
                f"max_alerts_per_scan={self.settings.max_alerts_per_scan}",
            ]
            for fiat in self.settings.fiats:
                min_spread, min_profit = self.settings.alert_thresholds_for(fiat, self.settings.base_asset)
                lines.append(f"{fiat}: spread>={min_spread}% profit>={min_profit} USD")
            await self.send_message("\n".join(lines))
            return

        if text.startswith("/rails"):
            lines = ["Configured rails:"]
            for fiat in self.settings.fiats:
                config = self.settings.order_config_for(fiat, self.settings.base_asset)
                lines.append(f"{fiat}: {', '.join(config.preferred_payments)}")
            await self.send_message("\n".join(lines))
            return

        if text.startswith("/fees"):
            lines = [
                "Fee profiles:",
                f"fx_conversion_rail={self.settings.fx_conversion_rail}",
            ]
            for rail, profile in sorted(self.settings.rail_fee_profiles.items()):
                lines.append(
                    f"{rail}: send={profile.send_pct}%+{profile.send_fixed_usd}USD | "
                    f"receive={profile.receive_pct}%+{profile.receive_fixed_usd}USD | "
                    f"fx={profile.fx_pct}%+{profile.fx_fixed_usd}USD | "
                    f"fx_rate_markup={profile.fx_rate_markup_pct}%"
                )
            await self.send_message("\n".join(lines))
            return

        if text.startswith("/market_refresh"):
            await self.send_message("Обновляю market research. Это может занять до пары минут.")
            overview = await self.market_research.refresh(force=True)
            await self.send_message(self._market_text(overview))
            return

        if text.startswith("/market"):
            if not self.state.market_overview:
                await self.send_message("Собираю market research. Это может занять до пары минут.")
            overview = await self.market_research.refresh(force=False)
            await self.send_message(self._market_text(overview))
            return

        if text.startswith("/liquidity"):
            await self.send_message(await self._liquidity_text())
            return

        if text.startswith("/config"):
            await self.send_message(self.settings.serialize_public())
            return

        if text.startswith("/set_spread"):
            parts = text.split()
            if len(parts) != 3:
                await self.send_message("Usage: /set_spread EUR 1.5")
                return
            fiat = parts[1].upper()
            try:
                spread = Decimal(parts[2])
            except InvalidOperation:
                await self.send_message("Spread must be a number, e.g. /set_spread EUR 1.5")
                return
            self.settings.set_min_spread(fiat, spread)
            await self.send_message(f"Updated {fiat}_{self.settings.base_asset} min spread to {spread}%")
            return

        if text.startswith("/set_alert_spread"):
            parts = text.split()
            if len(parts) not in {2, 3}:
                await self.send_message("Usage: /set_alert_spread 2.0 or /set_alert_spread EUR 2.0")
                return
            fiat: str | None = None
            raw_value = parts[1]
            if len(parts) == 3:
                fiat = parts[1].upper()
                raw_value = parts[2]
            try:
                spread = Decimal(raw_value)
            except InvalidOperation:
                await self.send_message("Spread must be a number, e.g. /set_alert_spread 2.0")
                return
            self.settings.set_alert_spread(spread, fiat=fiat)
            if fiat is None:
                await self.send_message(f"Updated default alert spread to {spread}%")
            else:
                await self.send_message(f"Updated {fiat} alert spread to {spread}%")
            return

        if text.startswith("/set_alert_profit"):
            parts = text.split()
            if len(parts) not in {2, 3}:
                await self.send_message("Usage: /set_alert_profit 5 or /set_alert_profit EUR 25")
                return
            fiat: str | None = None
            raw_value = parts[1]
            if len(parts) == 3:
                fiat = parts[1].upper()
                raw_value = parts[2]
            try:
                profit = Decimal(raw_value)
            except InvalidOperation:
                await self.send_message("Profit must be a number, e.g. /set_alert_profit 5")
                return
            self.settings.set_alert_profit(profit, fiat=fiat)
            if fiat is None:
                await self.send_message(f"Updated default alert profit floor to {profit} USD")
            else:
                await self.send_message(f"Updated {fiat} alert profit floor to {profit} USD")
            return

        await self.send_message("Unknown command.")

    async def _handle_callback(self, callback: dict[str, Any]) -> None:
        callback_id = str(callback.get("id") or "")
        data = str(callback.get("data") or "")
        chat_id = str((((callback.get("message") or {}).get("chat")) or {}).get("id"))
        if not self._is_authorized_chat(chat_id):
            self.logger.warning("Ignored Telegram callback from unauthorized chat_id=%s", chat_id)
            return

        try:
            action, order_id = data.split(":", 1)
        except ValueError:
            await self._api("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Bad action"})
            return

        if action == "confirm":
            try:
                await self.order_manager.release_trade(order_id)
                await self.send_message(f"Order {order_id} confirmed and released.")
                await self._api("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Released"})
            except Exception as exc:
                await self.send_message(f"Failed to release order {order_id}: {exc}")
                await self._api("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Failed"})
            return

        if action == "sigdone":
            signal_id = int(order_id)
            signal = await self.signal_journal_repository.get(signal_id)
            if signal is None:
                await self._api("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Signal not found"})
                return
            await self.signal_journal_repository.finalize_signal_execution(
                signal_id,
                inventory_repository=self.inventory_repository,
                close_matching_route=True,
            )
            await self.send_message(
                f"Сигнал #{signal_id} отмечен как сделан.\n"
                f"Если факт отличается от оценки, используйте: /actual {signal_id} profit [volume]"
            )
            await self._api("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Отмечено: сделано"})
            return

        if action == "sigreserve":
            signal_id = int(order_id)
            if self.execution_engine is None:
                await self._api(
                    "answerCallbackQuery",
                    {"callback_query_id": callback_id, "text": "Execution engine не подключён"},
                )
                return
            try:
                session_id = await self.execution_engine.reserve_signal(signal_id)
                session = await self.execution_engine.execution_session_repository.get(session_id)
                events = await self.execution_engine.execution_session_repository.list_events(session_id)
                if session is None:
                    raise RuntimeError("Execution session was not created")
                keyboard = {
                    "inline_keyboard": [
                        [
                            {"text": "🔄 Обновить", "callback_data": f"execdetail:{session_id}"},
                            {"text": "📋 Детали сигнала", "callback_data": f"sigdetail:{signal_id}"},
                        ]
                    ]
                }
                await self.send_message(
                    self._execution_session_text(session, events=events),
                    parse_mode="HTML",
                    reply_markup=keyboard,
                )
                await self._api(
                    "answerCallbackQuery",
                    {"callback_query_id": callback_id, "text": f"Session #{session_id} создана"},
                )
            except Exception as exc:
                await self.send_message(f"Не удалось подготовить execution session для сигнала #{signal_id}: {exc}")
                await self._api("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Ошибка"})
            return

        if action == "sigskip":
            signal_id = int(order_id)
            signal = await self.signal_journal_repository.get(signal_id)
            if signal is None:
                await self._api("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Signal not found"})
                return
            await self.signal_journal_repository.mark_status(signal_id, "skipped")
            await self.send_message(f"Сигнал #{signal_id} отмечен как пропущенный.")
            await self._api("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Отмечено: пропуск"})
            return

        if action == "sigprob":
            signal_id = int(order_id)
            signal = await self.signal_journal_repository.get(signal_id)
            if signal is None:
                await self._api("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Signal not found"})
                return
            await self.signal_journal_repository.mark_status(signal_id, "problem")
            await self.send_message(
                f"Сигнал #{signal_id} отмечен как проблема.\n"
                f"Можете добавить комментарий: /note {signal_id} текст"
            )
            await self._api("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Отмечено: проблема"})
            return

        if action == "sigdetail":
            signal_id = int(order_id)
            signal = await self.signal_journal_repository.get(signal_id)
            if signal is None:
                await self._api("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Signal not found"})
                return
            await self.send_message(self._signal_detail_text(signal))
            await self._api("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Показал детали"})
            return

        if action == "execdetail":
            session_id = int(order_id)
            if self.execution_engine is None:
                await self._api(
                    "answerCallbackQuery",
                    {"callback_query_id": callback_id, "text": "Execution engine не подключён"},
                )
                return
            session = await self.execution_engine.execution_session_repository.get(session_id)
            if session is None:
                await self._api(
                    "answerCallbackQuery",
                    {"callback_query_id": callback_id, "text": "Session not found"},
                )
                return
            events = await self.execution_engine.execution_session_repository.list_events(session_id)
            await self.send_message(self._execution_session_text(session, events=events), parse_mode="HTML")
            await self._api("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Показал session"})
            return

        if action == "paid":
            try:
                await self.order_manager.mark_trade_as_paid(order_id)
                await self.send_message(f"Order {order_id} marked as paid.")
                await self._api("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Marked paid"})
            except Exception as exc:
                await self.send_message(f"Failed to mark order {order_id} as paid: {exc}")
                await self._api("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Failed"})
            return

        if action == "dispute":
            self.risk_guard.record_dispute()
            await self.send_message(
                f"Order {order_id} marked for dispute review. Cooldown started for 2 hours."
            )
            await self._api("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Cooldown started"})
            return

        await self._api("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Unknown action"})

    async def _dispatch_event(self, event: Event) -> None:
        if event.type == "opportunity":
            signal_id = await self.signal_journal_repository.create_from_opportunity(event.payload)
            keyboard = {
                "inline_keyboard": [
                    [
                        {"text": "📋 Детали", "callback_data": f"sigdetail:{signal_id}"},
                    ],
                    [
                        {"text": "✅ Исполнил", "callback_data": f"sigdone:{signal_id}"},
                        {"text": "⏭ Пропустить", "callback_data": f"sigskip:{signal_id}"},
                    ],
                    [
                        {"text": "⚠️ Проблема", "callback_data": f"sigprob:{signal_id}"},
                    ],
                ]
            }
            try:
                await self.send_message(
                    self._opportunity_text(event.payload, signal_id=signal_id),
                    parse_mode="HTML",
                    reply_markup=keyboard,
                )
            except Exception as exc:
                await self.signal_journal_repository.set_status_by_id(
                    signal_id,
                    "delivery_failed",
                    note=f"telegram delivery failed: {type(exc).__name__}: {exc}",
                )
                raise
            return

        if event.type == "payment_pending":
            detail = event.payload
            order_id = str(detail.get("id") or "")
            text = (
                "Buyer marked payment as sent.\n"
                f"order_id={order_id}\n"
                f"fiat={detail.get('currencyId')}\n"
                f"price={detail.get('price')}\n"
                f"amount={detail.get('amount')}\n"
                f"buyer={detail.get('buyerRealName')}\n"
                "Confirm manually only after real bank settlement."
            )
            keyboard = {
                "inline_keyboard": [
                    [
                        {"text": "✅ Получил, подтвердить", "callback_data": f"confirm:{order_id}"},
                        {"text": "❌ Не получил, спор", "callback_data": f"dispute:{order_id}"},
                    ]
                ]
            }
            await self.send_message(text, reply_markup=keyboard)
            return

        if event.type == "payment_required":
            detail = event.payload
            order_id = str(detail.get("id") or "")
            pay_term = (detail.get("confirmedPayTerm") or {})
            pay_name = ((pay_term.get("paymentConfigVo") or {}).get("paymentName")) or pay_term.get("bankName") or detail.get("paymentType")
            account_no = pay_term.get("accountNo") or ""
            text = (
                "You need to pay seller manually.\n"
                f"order_id={order_id}\n"
                f"fiat={detail.get('currencyId')}\n"
                f"price={detail.get('price')}\n"
                f"amount={detail.get('amount')}\n"
                f"seller={detail.get('sellerRealName')}\n"
                f"payment_method={pay_name}\n"
                f"account={account_no}\n"
                "After real bank transfer, press the button below."
            )
            keyboard = {
                "inline_keyboard": [
                    [
                        {"text": "✅ Оплатил, отметить", "callback_data": f"paid:{order_id}"},
                        {"text": "❌ Спор / проблема", "callback_data": f"dispute:{order_id}"},
                    ]
                ]
            }
            await self.send_message(text, reply_markup=keyboard)
            return

        if event.type == "kill_switch":
            reason = event.payload.get("reason") if isinstance(event.payload, dict) else str(event.payload)
            await self.send_message(f"Kill switch triggered: {reason}")
            return

        if event.type == "warning":
            message = event.payload.get("message") if isinstance(event.payload, dict) else str(event.payload)
            await self.send_message(f"Warning: {message}")
            return

    async def send_message(
        self,
        text: str,
        *,
        parse_mode: str | None = None,
        reply_markup: dict[str, Any] | None = None,
        chat_id_override: str | None = None,
    ) -> None:
        chat_id = chat_id_override or self.config.chat_id
        if not chat_id:
            self.logger.info("Telegram chat is not configured yet. Message skipped: %s", text)
            return
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        if parse_mode is not None:
            payload["parse_mode"] = parse_mode
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        await self._api(
            "sendMessage",
            payload,
            timeout_sec=20.0,
            retries=3,
            backoff=(1.0, 3.0, 8.0),
        )

    async def _status_text(self) -> str:
        status = self.risk_guard.status_snapshot()
        active_ads = []
        if self.order_manager.enabled:
            try:
                active_ads = await self.order_manager.get_active_orders()
            except Exception as exc:
                self.logger.warning("Unable to fetch active orders for status: %s", exc)

        top_opportunity = self.state.latest_opportunities[0] if self.state.latest_opportunities else None
        top_confirmed = next(
            (item for item in self.state.latest_opportunities if self._is_alert_candidate(item)),
            None,
        )
        top_opportunity_text = (
            f"{top_opportunity.type}/{top_opportunity.rail_status} {top_opportunity.spread_pct:.4f}% "
            f"{self._route_text(top_opportunity)}"
            if top_opportunity
            else "n/a"
        )
        top_confirmed_text = (
            f"{top_confirmed.type} {top_confirmed.spread_pct:.4f}% "
            f"{self._route_text(top_confirmed)}"
            if top_confirmed
            else "n/a"
        )

        return (
            f"paused={status['paused']}\n"
            f"kill_reason={status['kill_switch_reason'] or 'n/a'}\n"
            f"last_scan={self.state.last_scan_at.isoformat() if self.state.last_scan_at else 'n/a'}\n"
            f"eur_pln={self.state.last_forex_rate_eur_pln or 'n/a'}\n"
            f"base_asset={self.settings.base_asset}\n"
            f"fiats={','.join(self.settings.fiats)}\n"
            f"daily_volume_usd={status['daily_volume_usd']}\n"
            f"daily_trades={status['daily_trades']}\n"
            f"api_errors={status['consecutive_api_errors']}\n"
            f"active_ads={len(active_ads)}\n"
            f"execution={self._execution_snapshot_text()}\n"
            f"top_opp={top_opportunity_text}\n"
            f"top_confirmed={top_confirmed_text}"
        )

    def _opportunity_text(self, opportunity: ArbitrageOpportunity, *, signal_id: int | None = None) -> str:
        expected_final_usdt = (
            opportunity.gross_return_usdt - opportunity.total_fees_usd
        ).quantize(Decimal("0.0001"))
        expected_return_usdt = self._fmt_decimal(expected_final_usdt, 4)
        route_text = self._route_text(opportunity)
        profit = self._fmt_decimal(opportunity.estimated_profit_usd, 2)
        spread = self._fmt_decimal(opportunity.spread_pct, 2)
        volume = self._fmt_decimal(opportunity.volume_usdt, 4)
        fees = self._fmt_decimal(opportunity.total_fees_usd, 2)
        buy_platform = self._platform_open_text(opportunity.buy_order)
        sell_platform = self._platform_open_text(opportunity.sell_order)
        buy_platform_name = html.escape(opportunity.buy_order.platform.upper())
        buy_price = self._fmt_decimal(opportunity.buy_order.price, 4)
        sell_price = self._fmt_decimal(opportunity.sell_order.price, 4)
        buy_currency = html.escape(opportunity.buy_order.fiat)
        sell_currency = html.escape(opportunity.sell_order.fiat)
        sell_notional = self._fmt_decimal(opportunity.sell_fiat_amount, 2)
        rebuy_budget = self._fmt_decimal(opportunity.rebuy_fiat_amount, 2)
        step3_gross_return_usdt = self._fmt_decimal(opportunity.gross_return_usdt, 4)
        buy_method = html.escape(opportunity.buy_user_method or opportunity.buy_payment_summary)
        sell_method = html.escape(opportunity.sell_user_method or opportunity.sell_payment_summary)
        buy_merchant = html.escape(self.risk_guard.merchant_summary(opportunity.buy_order))
        sell_merchant = html.escape(self.risk_guard.merchant_summary(opportunity.sell_order))
        execution_steps = self._execution_steps_text(
            opportunity=opportunity,
            buy_platform=buy_platform,
            buy_platform_name=buy_platform_name,
            sell_platform=sell_platform,
            buy_price=buy_price,
            sell_price=sell_price,
            buy_currency=buy_currency,
            sell_currency=sell_currency,
            sell_notional=sell_notional,
            rebuy_budget=rebuy_budget,
            buy_method=buy_method,
            sell_method=sell_method,
            buy_merchant=buy_merchant,
            sell_merchant=sell_merchant,
            input_volume=volume,
            step3_gross_return_usdt=step3_gross_return_usdt,
        )
        transfer_text = self._transfer_text(opportunity)
        lifetime = self._lifetime_text(opportunity)
        return (
            f"{lifetime} | <b>{html.escape(route_text)}</b>\n\n"
            f"💰 <b>+${profit}</b>\n"
            f"📊 <b>{spread}%</b>\n"
            f"🔁 <b>{volume} {html.escape(self.settings.base_asset)}</b>\n"
            f"📉 <b>${fees}</b> комиссии\n\n\n"
            f"{execution_steps}\n"
            f"\n✅ <b>Итог:</b> <b>{expected_return_usdt} {html.escape(self.settings.base_asset)}</b>  💵 <b>+${profit}</b>\n"
            f"⚡ {html.escape(transfer_text)}\n"
            f"{self._signal_id_suffix(signal_id)}"
        )

    @staticmethod
    def _fiat_notional(volume_usdt: Decimal, price: Decimal) -> Decimal:
        return (volume_usdt * price).quantize(Decimal("0.01"))

    def _fmt_notional(self, volume_usdt: Decimal, price: Decimal) -> str:
        return self._fmt_decimal(self._fiat_notional(volume_usdt, price), 2)

    @staticmethod
    def _fmt_decimal(value: Decimal, places: int) -> str:
        rendered = f"{value:.{places}f}"
        if "." not in rendered:
            return rendered
        return rendered.rstrip("0").rstrip(".")

    def _inventory_usage_text(self) -> str:
        asset = self.settings.base_asset
        return (
            "Usage: /set_balance location asset amount\n"
            "Примеры:\n"
            f"/set_balance binance {asset} 3000\n"
            f"/set_balance bingx {asset} 1500\n"
            f"/set_balance bybit {asset} 2000\n"
            "/set_balance revolut EUR 500\n"
            "/set_balance wise GBP 250\n"
            "/set_balance bank PLN 10000\n"
            "/set_balance binance_fiat EUR 300\n"
            "/set_balance bingx_fiat EUR 0\n"
            "/set_balance bybit_fiat RON 0\n\n"
            "Подсказки: /inventory_help"
        )

    def _inventory_help_text(self) -> str:
        asset = self.settings.base_asset
        return (
            "Инициализация остатков через /set_balance\n\n"
            "Поддерживаемые location:\n"
            "binance -> binance:wallet\n"
            "bingx -> bingx:wallet\n"
            "bybit -> bybit:wallet\n"
            "binance_fiat -> binance:fiat_balance\n"
            "bingx_fiat -> bingx:fiat_balance\n"
            "bybit_fiat -> bybit:fiat_balance\n"
            "revolut -> revolut\n"
            "wise -> wise\n"
            "bank -> bank\n\n"
            "Готовый шаблон:\n"
            f"/set_balance binance {asset} 3000\n"
            f"/set_balance bingx {asset} 1500\n"
            f"/set_balance bybit {asset} 2000\n"
            "/set_balance binance_fiat EUR 0\n"
            "/set_balance bingx_fiat EUR 0\n"
            "/set_balance bybit_fiat RON 0\n"
            "/set_balance revolut EUR 0\n"
            "/set_balance wise GBP 0\n"
            "/set_balance bank PLN 0\n\n"
            "После этого проверьте остатки через /inventory"
        )

    @staticmethod
    def _normalize_inventory_location(raw: str) -> str:
        value = raw.strip().lower().replace("-", "_")
        aliases = {
            "binance": "binance:wallet",
            "binance_wallet": "binance:wallet",
            "binance:wallet": "binance:wallet",
            "binance_fiat": "binance:fiat_balance",
            "binance:fiat": "binance:fiat_balance",
            "binance:fiat_balance": "binance:fiat_balance",
            "bingx": "bingx:wallet",
            "bingx_wallet": "bingx:wallet",
            "bingx:wallet": "bingx:wallet",
            "bingx_fiat": "bingx:fiat_balance",
            "bingx:fiat": "bingx:fiat_balance",
            "bingx:fiat_balance": "bingx:fiat_balance",
            "bybit": "bybit:wallet",
            "bybit_wallet": "bybit:wallet",
            "bybit:wallet": "bybit:wallet",
            "bybit_fiat": "bybit:fiat_balance",
            "bybit:fiat": "bybit:fiat_balance",
            "bybit:fiat_balance": "bybit:fiat_balance",
            "revolut": "revolut",
            "wise": "wise",
            "bank": "bank",
        }
        normalized = aliases.get(value)
        if normalized is None:
            raise ValueError(f"Unknown inventory location: {raw}")
        return normalized

    @staticmethod
    def _rail_label(rail: str) -> str:
        return {
            "bank_transfer": "Банковский перевод",
            "bank_fx": "банк (внутр. обмен)",
            "fiat_balance": "баланс биржи",
            "revolut_balance": "Revolut",
            "revolut_bank_transfer": "Revolut",
            "revolut_card": "Revolut",
            "wise_balance": "Wise",
            "wise_bank_transfer": "Wise",
            "wise_card": "Wise",
            "zen": "ZEN",
            "blik": "BLIK",
            "sepa": "SEPA",
            "": "unknown",
        }.get(rail, rail or "unknown")

    def _fx_rail_suffix(self, opportunity: ArbitrageOpportunity) -> str:
        if not opportunity.fx_rail:
            return ""
        return f" | FX <b>{html.escape(self._rail_label(opportunity.fx_rail))}</b>"

    @staticmethod
    def _signal_id_suffix(signal_id: int | None) -> str:
        if signal_id is None:
            return ""
        return f"\n\n<b>ID сигнала:</b> <b>{signal_id}</b>"

    def _route_text(self, opportunity: ArbitrageOpportunity) -> str:
        if opportunity.type == "internal_graph":
            raw = opportunity.sell_order.raw or {}
            chain = raw.get("chain") or []
            if chain:
                return " → ".join(str(item) for item in chain)
            return f"{opportunity.sell_order.platform.upper()} EXCHANGE"
        if opportunity.type == "cross_currency":
            if opportunity.buy_order.fiat == opportunity.sell_order.fiat:
                return (
                    f"{self.settings.base_asset} → {opportunity.sell_order.fiat} → "
                    f"{self.settings.base_asset}"
                )
            return (
                f"{self.settings.base_asset} → {opportunity.sell_order.fiat} → "
                f"{opportunity.buy_order.fiat} → {self.settings.base_asset}"
            )
        if opportunity.type == "cross_platform":
            return f"{opportunity.buy_order.fiat} | {opportunity.sell_order.platform.upper()} → {opportunity.buy_order.platform.upper()}"
        return f"{opportunity.buy_order.platform.upper()} • {opportunity.buy_order.fiat}"

    def _platform_open_text(self, order: Any) -> str:
        label = html.escape(str(order.platform).upper())
        url = self._order_action_url(order)
        if not url:
            return label
        link_text = html.escape(self._order_action_link_text(order))
        return f'{label} <a href="{html.escape(url, quote=True)}">{link_text}</a>'

    @staticmethod
    def _order_action_url(order: Any) -> str:
        if isinstance(order, P2POrder):
            return build_order_action_url(order)
        return build_order_action_url(
            P2POrder(
                platform=str(getattr(order, "platform", "") or ""),
                order_id=str(getattr(order, "order_id", "") or ""),
                side=str(getattr(order, "side", "") or ""),
                asset=str(getattr(order, "asset", "USDT") or "USDT"),
                fiat=str(getattr(order, "fiat", "") or ""),
                price=Decimal("0"),
                min_amount=Decimal("0"),
                max_amount=Decimal("0"),
                available=Decimal("0"),
                payment_methods=list(getattr(order, "payment_methods", []) or []),
                merchant_id="",
                merchant_rating=0.0,
                merchant_orders=0,
                merchant_days=0,
                raw=getattr(order, "raw", {}) or {},
            )
        )

    @staticmethod
    def _order_action_link_text(order: Any) -> str:
        platform = str(getattr(order, "platform", "") or "").strip().lower()
        asset = str(getattr(order, "asset", "USDT") or "USDT").strip().upper()
        side = str(getattr(order, "side", "") or "").strip().lower()
        if platform == "bybit" and side in {"sell", "buy"}:
            side = bybit_web_action(side, asset=asset)
        if side == "sell":
            return "ОТКРЫТЬ SELL"
        if side == "buy":
            return "ОТКРЫТЬ BUY"
        return "ОТКРЫТЬ"

    def _execution_steps_text(
        self,
        *,
        opportunity: ArbitrageOpportunity,
        buy_platform: str,
        buy_platform_name: str,
        sell_platform: str,
        buy_price: str,
        sell_price: str,
        buy_currency: str,
        sell_currency: str,
        sell_notional: str,
        rebuy_budget: str,
        buy_method: str,
        sell_method: str,
        buy_merchant: str,
        sell_merchant: str,
        input_volume: str,
        step3_gross_return_usdt: str,
    ) -> str:
        prefunded = self._is_prefunded_route(opportunity)
        sell_bucket, fx_bucket, buy_bucket = self._route_buckets(opportunity)
        if opportunity.type == "internal_graph":
            raw = opportunity.sell_order.raw or {}
            chain = raw.get("chain") or []
            quotes = raw.get("quotes") or []
            if chain and quotes:
                lines = []
                for index, quote in enumerate(quotes, start=1):
                    lines.append(
                        f"{index}. <b>{html.escape(str(quote.get('from_asset', '')))} → {html.escape(str(quote.get('to_asset', '')))}</b> "
                        f"через {html.escape(str(quote.get('venue', 'internal')))} "
                        f"(rate {html.escape(str(quote.get('rate', '0')))})"
                    )
                lines.append(
                    f"Ожидаемо получишь: <b>{step3_gross_return_usdt} {html.escape(self.settings.base_asset)}</b>"
                )
                return (
                    f"<b>ШАГ 1 — ВНУТРЕННИЙ МАРШРУТ НА {html.escape(opportunity.sell_order.platform.upper())}</b>\n"
                    f"{self._quote_block(lines)}"
                )
        sell_merchant_suffix = f"\n{sell_merchant}" if sell_merchant else ""
        buy_merchant_suffix = f"\n{buy_merchant}" if buy_merchant else ""
        sell_order_method_suffix = self._order_method_suffix(
            user_method=sell_method,
            payment_summary=opportunity.sell_payment_summary,
        )
        buy_order_method_suffix = self._order_method_suffix(
            user_method=buy_method,
            payment_summary=opportunity.buy_payment_summary,
        )
        step1_lines = [
            f"{sell_platform} @ <code>{sell_price} {sell_currency}/{html.escape(self.settings.base_asset)}</code>",
            f"Продай: <b>{input_volume} {html.escape(self.settings.base_asset)}</b>",
            f"Получи: <b>{html.escape(sell_notional)} {sell_currency}</b>",
            self._receive_step_text(
                rail=opportunity.sell_rail,
                currency=sell_currency,
                user_method=sell_method,
                order_method_suffix=sell_order_method_suffix,
            ),
        ]
        if sell_merchant:
            step1_lines.append(sell_merchant)
        steps = [
            (
                f"<b>ШАГ 1 — ПРОДАЙ {html.escape(self.settings.base_asset)}</b>\n"
                f"{self._quote_block(step1_lines)}"
            )
        ]
        next_step_no = 2
        if opportunity.type == "cross_currency" and buy_currency != sell_currency:
            step2_extra = " (ОТДЕЛЬНЫЙ КАРМАН)" if prefunded and sell_bucket != fx_bucket else ""
            fx_line = html.escape(self._rail_label(opportunity.fx_rail))
            if prefunded and sell_bucket != fx_bucket:
                fx_line = f"{fx_line} (prefunded)"
            step2_lines = [
                f"Через: {fx_line}",
                f"{self._fx_instruction_text(opportunity)}",
                f"Подготовь для шага 3: <b>{html.escape(rebuy_budget)} {buy_currency}</b>",
            ]
            steps.append(
                (
                    f"<b>ШАГ 2 — ОБМЕНЯЙ {sell_currency} → {buy_currency}{step2_extra}</b>\n"
                    f"{self._quote_block(step2_lines)}"
                )
            )
            next_step_no = 3
        if opportunity.buy_rail == "fiat_balance":
            prep_lines = [
                f"На {buy_platform_name} сначала обменяй часть своего <b>{html.escape(self.settings.base_asset)}</b> в <b>{buy_currency}</b>",
                f"Цель: чтобы на фиатном балансе было <b>{html.escape(rebuy_budget)} {buy_currency}</b>",
                "Это отдельный карман на бирже, не деньги из шага 1",
            ]
            steps.append(
                (
                    f"<b>ШАГ {next_step_no} — ПОДГОТОВЬ ФИАТ НА {buy_platform_name}</b>\n"
                    f"{self._quote_block(prep_lines)}"
                )
            )
            next_step_no += 1
        step3_extra = ""
        if prefunded:
            if opportunity.type == "cross_currency" and buy_currency != sell_currency and fx_bucket != buy_bucket:
                step3_extra = " (ОТДЕЛЬНЫЙ КАРМАН)"
            elif opportunity.type != "cross_currency" and sell_bucket != buy_bucket:
                step3_extra = " (ОТДЕЛЬНЫЙ КАРМАН)"
        step3_lines = [
            f"{buy_platform} @ <code>{buy_price} {buy_currency}/{html.escape(self.settings.base_asset)}</code>",
            f"Потрать: <b>{html.escape(rebuy_budget)} {buy_currency}</b>",
            f"Ожидаемо получишь: <b>{step3_gross_return_usdt} {html.escape(self.settings.base_asset)}</b>",
            f"Платишь из: {buy_method}{buy_order_method_suffix}",
        ]
        if opportunity.manual_execution_hint:
            step3_lines.append(f"Ручной шаг: {html.escape(opportunity.manual_execution_hint)}")
        if buy_merchant:
            step3_lines.append(buy_merchant)
        steps.append(
            (
                f"<b>ШАГ {next_step_no} — КУПИ {html.escape(self.settings.base_asset)} ОБРАТНО{step3_extra}</b>\n"
                f"{self._quote_block(step3_lines)}"
            )
        )
        if opportunity.post_trade_rebalance_source and opportunity.post_trade_rebalance_target:
            rebalance_amount = html.escape(sell_notional)
            rebalance_currency = sell_currency
            rebalance_source = html.escape(self._bucket_label(opportunity.post_trade_rebalance_source))
            rebalance_target = html.escape(self._bucket_label(opportunity.post_trade_rebalance_target))
            step4_lines = [
                f"После исполнения переведи: <b>{rebalance_amount} {rebalance_currency}</b>",
                f"Из: {rebalance_source}",
                f"В: {rebalance_target}",
            ]
            if opportunity.buy_order.fiat.upper() != opportunity.sell_order.fiat.upper():
                step4_lines.append(
                    f"Цель: вернуть карману {rebalance_target} исходный баланс для будущих сделок"
                )
            steps.append(
                (
                    f"<b>ШАГ {next_step_no + 1} — РЕБАЛАНС ПОСЛЕ КРУГА</b>\n"
                    f"{self._quote_block(step4_lines)}"
                )
            )
        return "\n\n".join(steps)

    @staticmethod
    def _quote_block(lines: list[str]) -> str:
        content = "\n".join(line for line in lines if line)
        return f"<blockquote>{content}</blockquote>"

    @staticmethod
    def _order_method_suffix(*, user_method: str, payment_summary: str) -> str:
        summary = (payment_summary or "").strip()
        if not summary:
            return ""
        normalized_user = " ".join(user_method.lower().split())
        normalized_summary = " ".join(summary.lower().split())
        if normalized_user == normalized_summary:
            return ""
        if normalized_summary in normalized_user:
            return ""
        return f"\nМетод в ордере: {html.escape(summary)}"

    def _fx_instruction_text(self, opportunity: ArbitrageOpportunity) -> str:
        source = opportunity.sell_order.fiat.upper()
        target = opportunity.buy_order.fiat.upper()
        if " @ FX " not in opportunity.note:
            return f"Цель FX: <code>1 {html.escape(source)} ≥ текущий курс в {html.escape(target)}</code>"
        fx_info = opportunity.note.split(" @ FX ", 1)[1].strip()
        _, _, raw_rate = fx_info.rpartition(" ")
        if not raw_rate:
            return f"Цель FX: <code>1 {html.escape(source)} ≥ курс в {html.escape(target)}</code>"
        try:
            rate = Decimal(raw_rate)
            rendered_rate = self._fmt_decimal(rate, 6)
        except InvalidOperation:
            rendered_rate = raw_rate
        return f"Цель FX: <code>1 {html.escape(source)} ≥ {html.escape(rendered_rate)} {html.escape(target)}</code>"

    @staticmethod
    def _bucket_label(bucket: str) -> str:
        if not bucket:
            return "unknown rail"
        if bucket == "revolut":
            return "Revolut"
        if bucket == "wise":
            return "Wise"
        if bucket == "bank":
            return "Твой банковский счёт"
        if bucket.endswith(":fiat_balance"):
            platform = bucket.split(":", 1)[0]
            return f"Фиатный баланс {platform.upper()}"
        if bucket.endswith(":wallet"):
            platform = bucket.split(":", 1)[0]
            return f"{platform.upper()} wallet"
        return bucket

    @staticmethod
    def _receive_step_text(*, rail: str, currency: str, user_method: str, order_method_suffix: str) -> str:
        destination = TelegramBotService._receive_destination_label(rail=rail, currency=currency, user_method=user_method)
        return f"Поступит в: {destination}{order_method_suffix}"

    @staticmethod
    def _receive_destination_label(*, rail: str, currency: str, user_method: str) -> str:
        normalized = (rail or "").lower()
        currency_upper = (currency or "").upper()
        if normalized.startswith("revolut_"):
            return "Revolut"
        if normalized.startswith("wise_"):
            return "Wise"
        if normalized == "sepa":
            return "Твой EUR счёт"
        if normalized in {"bank_transfer", "blik", "zen"}:
            if normalized == "blik":
                return "Твой PKO Bank"
            if normalized == "zen":
                return "Твой польский счёт / ZEN"
            if currency_upper == "PLN":
                return "Твой польский банковский счёт"
            return "Твой банковский счёт"
        if normalized == "fiat_balance":
            return "Фиатный баланс биржи"
        return user_method

    def _execution_text(self, opportunity: ArbitrageOpportunity) -> str:
        if opportunity.buy_order.platform == opportunity.sell_order.platform:
            return opportunity.sell_order.platform.upper()
        return f"{opportunity.sell_order.platform.upper()} → {opportunity.buy_order.platform.upper()}"

    def _transfer_text(self, opportunity: ArbitrageOpportunity) -> str:
        if opportunity.type == "internal_graph":
            return "внутренние балансы биржи"
        if opportunity.type == "transfer_route":
            return f"требуется перевод {self.settings.base_asset} между биржами"
        if opportunity.post_trade_rebalance_source and opportunity.post_trade_rebalance_target:
            receive_label = self._bucket_label(opportunity.post_trade_rebalance_source)
            execute_label = self._bucket_label(opportunity.post_trade_rebalance_target)
            base = f"Получаешь: {receive_label} | Исполняешь из: {execute_label}"
            if opportunity.fx_rail:
                base = f"{base} | FX {self._rail_label(opportunity.fx_rail)}"
            return (
                f"{base} | Ребаланс после: "
                f"{self._bucket_label(opportunity.post_trade_rebalance_source)} → "
                f"{self._bucket_label(opportunity.post_trade_rebalance_target)}"
            )
        if self._is_prefunded_route(opportunity):
            labels: list[str] = []
            for rail in (
                opportunity.sell_rail,
                opportunity.fx_rail if opportunity.buy_order.fiat.upper() != opportunity.sell_order.fiat.upper() else "",
                opportunity.buy_rail,
            ):
                label = self._rail_label(rail or "")
                if rail and label not in labels:
                    labels.append(label)
            return " | ".join(labels) if labels else "prefunded"
        sell_label = self._rail_label(opportunity.sell_rail)
        buy_label = self._rail_label(opportunity.buy_rail)
        if sell_label == buy_label:
            base = f"Кошелёк: {sell_label}"
        else:
            base = f"{sell_label} → {buy_label}"
        if opportunity.fx_rail:
            return f"{base} | FX {self._rail_label(opportunity.fx_rail)}"
        return base

    def _lifetime_text(self, opportunity: ArbitrageOpportunity) -> str:
        speed = self._route_speed(opportunity)
        return {
            "instant": "⚡ <b>Мгновенный</b>",
            "fast": "🟢 <b>Быстрый</b>",
            "slow": "🔵 <b>Медленный</b>",
        }.get(speed, "🔵 <b>Медленный</b>")

    def _strategy_type_label(self, opportunity: ArbitrageOpportunity) -> str:
        if opportunity.type == "internal_graph":
            return "EXCHANGE-ONLY"
        if self._is_prefunded_route(opportunity):
            return "PREFUNDED"
        if opportunity.type == "transfer_route":
            return "ПЕРЕВОД МЕЖДУ БИРЖАМИ"
        if opportunity.type == "cross_platform":
            return "МЕЖБИРЖЕВОЙ"
        if opportunity.type == "cross_currency":
            if opportunity.buy_order.platform != opportunity.sell_order.platform:
                return "СЛОЖНЫЙ"
            return "P2P + FX"
        if opportunity.buy_rail == "fiat_balance" and opportunity.sell_rail == "fiat_balance":
            return "ВНУТРЕННИЙ"
        return "P2P"

    def _strategy_reason_text(self, opportunity: ArbitrageOpportunity) -> str:
        if opportunity.type == "internal_graph":
            return f"внутренний многошаговый цикл внутри биржи даёт лучший возврат в {self.settings.base_asset}"
        if self._is_prefunded_route(opportunity):
            return f"маршрут использует заранее разложенные карманы, итог считается в {self.settings.base_asset}"
        if opportunity.type == "transfer_route":
            return (
                f"маршрут требует реального перевода {self.settings.base_asset} "
                "между биржами, но после этого всё ещё остаётся выгодным"
            )
        if opportunity.type == "cross_platform":
            return (
                f"дорогая продажа на {opportunity.sell_order.platform.upper()} "
                f"и более дешёвый выкуп на {opportunity.buy_order.platform.upper()}"
            )
        if opportunity.type == "cross_currency":
            sell_fiat = opportunity.sell_order.fiat
            buy_fiat = opportunity.buy_order.fiat
            if sell_fiat == buy_fiat:
                return f"высокий курс продажи и дешёвый выкуп в {sell_fiat}"
            return f"высокий курс продажи в {sell_fiat} и дешёвый выкуп в {buy_fiat}"
        if opportunity.buy_order.platform == opportunity.sell_order.platform:
            return f"высокий курс продажи и дешёвый выкуп в {opportunity.buy_order.fiat}"
        return "рыночный перекос между лучшей продажей и лучшим выкупом"

    @staticmethod
    def _internal_alt_text(opportunity: ArbitrageOpportunity) -> str:
        if opportunity.type != "cross_currency":
            return "не требуется"
        if opportunity.internal_quote_expected_usdt > 0:
            return "проверена ботом"
        return "не проверялась"

    @staticmethod
    def _kind_label(opportunity_type: str) -> str:
        return {
            "internal": "внутри биржи",
            "internal_graph": "внутренний граф",
            "transfer_route": "с переводом между биржами",
            "cross_platform": "между биржами",
            "cross_currency": "межвалютный",
        }.get(opportunity_type, opportunity_type)

    def _route_buckets(self, opportunity: ArbitrageOpportunity) -> tuple[str, str, str]:
        sell_bucket = self._rail_bucket(
            rail=opportunity.sell_rail,
            platform=opportunity.sell_order.platform,
            fiat=opportunity.sell_order.fiat,
        )
        buy_bucket = self._rail_bucket(
            rail=opportunity.buy_rail,
            platform=opportunity.buy_order.platform,
            fiat=opportunity.buy_order.fiat,
        )
        fx_bucket = ""
        if opportunity.buy_order.fiat.upper() != opportunity.sell_order.fiat.upper():
            fx_bucket = self._rail_bucket(
                rail=opportunity.fx_rail,
                platform=opportunity.sell_order.platform,
                fiat=opportunity.sell_order.fiat,
            )
        return sell_bucket, fx_bucket, buy_bucket

    def _is_prefunded_route(self, opportunity: ArbitrageOpportunity) -> bool:
        if not self.settings.prefunded_mode or opportunity.type == "internal_graph":
            return False
        sell_bucket, fx_bucket, buy_bucket = self._route_buckets(opportunity)
        if opportunity.buy_order.fiat.upper() != opportunity.sell_order.fiat.upper():
            if not sell_bucket or not fx_bucket or not buy_bucket:
                return False
            if sell_bucket != fx_bucket:
                return True
            return not self._bucket_transition_allowed(
                source_bucket=fx_bucket,
                target_bucket=buy_bucket,
                target_rail=opportunity.buy_rail,
                target_fiat=opportunity.buy_order.fiat,
            )
        if not sell_bucket or not buy_bucket:
            return False
        return not self._bucket_transition_allowed(
            source_bucket=sell_bucket,
            target_bucket=buy_bucket,
            target_rail=opportunity.buy_rail,
            target_fiat=opportunity.buy_order.fiat,
        )

    def _bucket_transition_allowed(
        self,
        *,
        source_bucket: str,
        target_bucket: str,
        target_rail: str,
        target_fiat: str,
    ) -> bool:
        if not source_bucket or not target_bucket:
            return False
        if source_bucket == target_bucket:
            return True
        normalized_target_rail = (target_rail or "").lower()
        if (
            normalized_target_rail == "sepa"
            and target_fiat.upper() == "EUR"
            and source_bucket in {"revolut", "wise", "bank"}
            and target_bucket == "bank"
        ):
            return True
        if (
            normalized_target_rail == "bank_transfer"
            and target_bucket == "bank"
        ):
            target_fiat_upper = target_fiat.upper()
            if source_bucket == "revolut":
                return target_fiat_upper in {fiat.upper() for fiat in self.settings.revolut_bank_transfer_fiats}
            if source_bucket == "wise":
                return target_fiat_upper in {fiat.upper() for fiat in self.settings.wise_bank_transfer_fiats}
            if source_bucket == "bank":
                return target_fiat_upper in {fiat.upper() for fiat in self.settings.local_bank_fiats}
        return False

    def _route_speed(self, opportunity: ArbitrageOpportunity) -> str:
        receive_method = self._route_method_text(
            opportunity.sell_user_method,
            opportunity.sell_payment_summary,
            opportunity.sell_order.payment_methods,
        )
        send_method = self._route_method_text(
            opportunity.buy_user_method,
            opportunity.buy_payment_summary,
            opportunity.buy_order.payment_methods,
        )
        sell_bucket, fx_bucket, buy_bucket = self._route_buckets(opportunity)
        prefunded = self._is_prefunded_route(opportunity)
        receive_currency = opportunity.sell_order.fiat.upper()
        send_currency = opportunity.buy_order.fiat.upper()
        revolut_local = {fiat.upper() for fiat in self.settings.revolut_bank_transfer_fiats}
        wise_receive_local = {fiat.upper() for fiat in self.settings.wise_receive_bank_fiats}
        wise_send_slow = {fiat.upper() for fiat in self.settings.wise_bank_transfer_slow_fiats}
        wise_send_local = {
            fiat.upper()
            for fiat in self.settings.wise_bank_transfer_fiats
            if fiat.upper() not in wise_send_slow
        }
        receive_fast_currencies = revolut_local | wise_receive_local
        send_fast_currencies = revolut_local | wise_send_local
        explicit_revolut_receive = "revolut" in receive_method
        explicit_wise_receive = "wise" in receive_method
        explicit_revolut_send = "revolut" in send_method
        explicit_wise_send = "wise" in send_method
        receive_mismatch = self._method_currency_mismatch(receive_method, receive_currency)
        send_mismatch = self._method_currency_mismatch(send_method, send_currency)
        has_ladder = self._has_multi_merchant_leg(opportunity)
        blik_manual = self._is_blik_manual_route(opportunity, sell_bucket=sell_bucket, fx_bucket=fx_bucket)
        zen_manual = self._is_zen_manual_route(opportunity, sell_bucket=sell_bucket, fx_bucket=fx_bucket)

        if prefunded:
            return "slow"
        if (
            not has_ladder
            and
            sell_bucket == "revolut"
            and buy_bucket == "revolut"
            and (not fx_bucket or fx_bucket == "revolut")
            and explicit_revolut_receive
            and explicit_revolut_send
        ):
            return "instant"
        if (
            not has_ladder
            and
            sell_bucket == "wise"
            and buy_bucket == "wise"
            and (not fx_bucket or fx_bucket == "wise")
            and explicit_wise_receive
            and explicit_wise_send
        ):
            return "instant"
        if opportunity.buy_rail == "wise_bank_transfer" and send_currency in wise_send_slow:
            return "slow"
        if (
            not has_ladder
            and opportunity.sell_rail == "blik"
            and opportunity.buy_rail == "blik"
            and sell_bucket == "bank"
            and buy_bucket == "bank"
        ):
            return "instant"
        if not has_ladder and any(keyword in receive_method for keyword in ("pko", "bank polski")):
            return "instant"
        if not has_ladder and "sepa instant" in receive_method:
            return "instant"
        if "swift" in receive_method or "swift" in send_method or receive_mismatch or send_mismatch:
            return "slow"

        receive_fast = (
            receive_currency in receive_fast_currencies
            or any(keyword in receive_method for keyword in ("sepa", "interac", "rbc", "td bank", "otp", "zen"))
        )
        send_fast = send_currency in send_fast_currencies or blik_manual or zen_manual
        if receive_fast and send_fast:
            return "fast"
        if receive_currency not in receive_fast_currencies and not (explicit_revolut_receive or explicit_wise_receive):
            return "slow"
        if send_currency not in send_fast_currencies and not (explicit_revolut_send or explicit_wise_send) and not blik_manual and not zen_manual:
            return "slow"
        return "fast"

    @staticmethod
    def _is_blik_manual_route(
        opportunity: ArbitrageOpportunity,
        *,
        sell_bucket: str,
        fx_bucket: str,
    ) -> bool:
        source_bucket = fx_bucket or sell_bucket
        return (
            opportunity.buy_rail == "blik"
            and opportunity.buy_order.fiat.upper() == "PLN"
            and source_bucket in {"revolut", "wise", "bank"}
        )

    @staticmethod
    def _is_zen_manual_route(
        opportunity: ArbitrageOpportunity,
        *,
        sell_bucket: str,
        fx_bucket: str,
    ) -> bool:
        source_bucket = fx_bucket or sell_bucket
        return (
            opportunity.buy_rail == "zen"
            and opportunity.buy_order.fiat.upper() == "PLN"
            and source_bucket in {"revolut", "wise", "bank"}
        )

    @staticmethod
    def _has_multi_merchant_leg(opportunity: ArbitrageOpportunity) -> bool:
        for order in (opportunity.buy_order, opportunity.sell_order):
            raw = order.raw or {}
            components = raw.get("components") if isinstance(raw, dict) else None
            if isinstance(components, list) and len(components) >= 2:
                return True
        return False

    @staticmethod
    def _route_method_text(user_method: str, payment_summary: str, payment_methods: tuple[str, ...] | list[str]) -> str:
        parts = [user_method or "", payment_summary or "", *[str(item) for item in payment_methods]]
        return " | ".join(part.strip().lower() for part in parts if str(part).strip())

    @staticmethod
    def _method_currency_mismatch(method_text: str, currency: str) -> bool:
        hint = TelegramBotService._method_country_hint(method_text)
        if hint is None:
            return False
        country_currency = {
            "gb": "GBP",
            "ca": "CAD",
            "au": "AUD",
            "nz": "NZD",
            "sg": "SGD",
            "ph": "PHP",
            "us": "USD",
            "hu": "HUF",
            "ro": "RON",
            "pl": "PLN",
        }.get(hint)
        return bool(country_currency and currency.upper() != country_currency)

    @staticmethod
    def _method_country_hint(method_text: str) -> str | None:
        text = (method_text or "").lower()
        hints = (
            ("gb", ("faster payments", "uk bank transfer", "sort code", "barclays", "lloyds", "natwest", "hsbc", "halifax", "monzo", "starling")),
            ("ca", ("interac", "rbc", "royal bank", "td bank", "scotiabank", "cibc", "bmo")),
            ("au", ("bsb", "payid", "commonwealth", "westpac", "nab", "australia and new zealand")),
            ("nz", ("kiwibank", "anz nz", "asb", "bnz", "new zealand")),
            ("sg", ("fast", "paynow", "dbs", "ocbc", "uob", "singapore")),
            ("ph", ("instapay", "pesonet", "wise pilipinas", "gcash", "maya", "bpi", "bdo", "philippines")),
            ("us", ("ach", "wire", "zelle", "routing", "aba", "chase", "bofa", "bank of america", "wells fargo", "citibank")),
            ("hu", ("azonnali", "otp bank", "otp", "k&h", "raiffeisen hu")),
            ("ro", ("brd", "bcr", "bank transilvania", "bt pay", "btpay", "raiffeisen bank aval")),
            ("pl", ("pko", "blik", "zen", "santander poland", "millennium", "mbank", "ing poland")),
        )
        for country, keywords in hints:
            if any(keyword in text for keyword in keywords):
                return country
        return None

    @staticmethod
    def _rail_bucket(*, rail: str, platform: str, fiat: str) -> str:
        normalized = (rail or "").lower()
        fiat_upper = fiat.upper()
        if normalized == "fiat_balance":
            return f"{platform}:fiat_balance"
        if normalized.startswith("revolut_"):
            return "revolut"
        if normalized.startswith("wise_"):
            return "wise"
        if normalized in {"bank_transfer", "bank_fx", "sepa", "blik", "zen"}:
            return "bank"
        if normalized in {"", "unknown"}:
            return ""
        return f"{normalized}:{fiat_upper}"

    def _signal_detail_text(self, signal: dict[str, object]) -> str:
        route = self._signal_route_text(signal)
        note = str(signal.get("note") or "").strip()
        lines = [
            f"Детали сигнала #{signal['id']}",
            f"Маршрут: {route}",
            (
                f"Если стартуете с {self.settings.base_asset}: "
                f"сначала продаёте {self.settings.base_asset}, потом делаете FX при необходимости, "
                f"потом выкупаете {self.settings.base_asset} обратно."
            ),
            f"Статус: {signal['status']}",
            f"Тип: {signal['opportunity_type']}",
            f"Платформы: {signal['sell_platform']} -> {signal['buy_platform']}",
            f"Валюты: {signal['sell_fiat']} -> {signal['buy_fiat']}",
            f"Оценка прибыли: {signal['estimated_profit_usd']} USD",
            f"Gross profit: {signal['gross_profit_usd']} USD",
            f"Комиссии: {signal['fees_usd']} USD",
            f"Объем: {signal['volume_usdt']} {self.settings.base_asset}",
            f"Ликвидность: {signal['liquidity_status']}",
            f"Создан: {signal['created_at']}",
            f"Обновлен: {signal['updated_at']}",
            "Подсказка: в короткой карточке бот показывает ваш совместимый метод, а не все банки контрагента.",
        ]
        if signal.get("actual_profit_usd") is not None:
            lines.append(f"Факт прибыль: {signal['actual_profit_usd']} USD")
        if signal.get("actual_volume_usdt") is not None:
            lines.append(f"Факт объем: {signal['actual_volume_usdt']} {self.settings.base_asset}")
        if note:
            lines.append(f"Заметка: {note}")
        return "\n".join(lines)

    def _execution_snapshot_text(self) -> str:
        if self.execution_engine is None:
            return "n/a"
        snapshot = self.execution_engine.capability_snapshot()
        return (
            f"enabled={snapshot['enabled']}, dry_run={snapshot['dry_run']}, "
            f"bybit={snapshot['bybit_private_ready']}, "
            f"binance={snapshot['binance_private_ready']}, "
            f"revolut={snapshot['revolut_business_ready']}"
        )

    def _execution_session_text(
        self,
        session: dict[str, object],
        *,
        events: list[dict[str, object]] | None = None,
    ) -> str:
        route = self._signal_route_text(
            {
                "opportunity_type": "cross_currency"
                if str(session.get("buy_fiat") or "") != str(session.get("sell_fiat") or "")
                else "cross_platform",
                "buy_fiat": session.get("buy_fiat"),
                "sell_fiat": session.get("sell_fiat"),
                "buy_platform": session.get("buy_platform"),
                "sell_platform": session.get("sell_platform"),
                "route": session.get("route"),
            }
        )
        status = html.escape(str(session.get("status") or "unknown"))
        mode = html.escape(str(session.get("mode") or "unknown"))
        sell_platform = html.escape(str(session.get("sell_platform") or "").upper())
        buy_platform = html.escape(str(session.get("buy_platform") or "").upper())
        sell_fiat = html.escape(str(session.get("sell_fiat") or ""))
        buy_fiat = html.escape(str(session.get("buy_fiat") or ""))
        volume = self._fmt_decimal(Decimal(str(session.get("volume_usdt") or "0")), 4)
        profit = self._fmt_decimal(Decimal(str(session.get("estimated_profit_usd") or "0")), 2)
        treasury = html.escape(str(session.get("treasury_provider") or "unknown"))
        error_text = str(session.get("error_text") or "").strip()
        payload_raw = str(session.get("payload_json") or "{}")
        try:
            payload = json.loads(payload_raw)
        except json.JSONDecodeError:
            payload = {}
        capabilities = payload.get("capabilities") or {}
        capability_lines = []
        if capabilities:
            capability_lines.extend(
                [
                    f"Bybit private: {'✅' if capabilities.get('bybit_private_ready') else '❌'}",
                    f"Binance private: {'✅' if capabilities.get('binance_private_ready') else '❌'}",
                    f"Revolut Business: {'✅' if capabilities.get('revolut_business_ready') else '❌'}",
                ]
            )
        revalidation = payload.get("revalidation") or {}
        revalidation_lines: list[str] = []
        if revalidation:
            revalidation_lines.append(
                f"Passed: {'✅' if revalidation.get('passed') else '❌'}"
            )
            if revalidation.get("max_price_drift_pct") is not None:
                revalidation_lines.append(
                    f"Max drift: {html.escape(str(revalidation.get('max_price_drift_pct')))}%"
                )
            reason = str(revalidation.get("reason") or "").strip()
            if reason:
                revalidation_lines.append(f"Reason: {html.escape(reason)}")
        rm = payload.get("rm") or {}
        rm_lines: list[str] = []
        if rm:
            auto_flag = "✅" if rm.get("eligible_for_full_auto") else "❌"
            rm_lines.extend(
                [
                    f"Full auto scope: {auto_flag}",
                    f"Recommended size: {html.escape(str(rm.get('recommended_volume_usdt') or '0'))} {html.escape(self.settings.base_asset)}",
                    f"Limit: {html.escape(str(rm.get('max_capital_pct') or '0'))}%",
                    f"Limiter: {html.escape(str(rm.get('limiting_factor') or 'n/a'))}",
                ]
            )
            scope_reason = str(rm.get("scope_reason") or "").strip()
            if scope_reason:
                rm_lines.append(f"Scope: {html.escape(scope_reason)}")
            rebalance = rm.get("rebalance") if isinstance(rm.get("rebalance"), dict) else {}
            actions = rebalance.get("actions") if isinstance(rebalance, dict) else []
            if actions:
                first = actions[0]
                rm_lines.append(
                    "Rebalance: "
                    f"{html.escape(str(first.get('from') or ''))} → {html.escape(str(first.get('to') or ''))} "
                    f"{html.escape(str(first.get('amount') or '0'))} {html.escape(str(first.get('asset') or 'USDT'))}"
                )
        event_lines: list[str] = []
        for event in (events or [])[-5:]:
            event_lines.append(
                f"• {html.escape(str(event.get('event_type') or 'event'))} — {html.escape(str(event.get('created_at') or ''))}"
            )

        sections = [
            f"⚡ <b>Execution Session #{session['id']}</b>",
            f"<b>Статус:</b> {status}",
            f"<b>Режим:</b> {mode}",
            f"<b>Маршрут:</b> {html.escape(route)}",
            f"<b>Биржи:</b> {sell_platform} → {buy_platform}",
            f"<b>Валюты:</b> {sell_fiat} → {buy_fiat}",
            f"<b>Объём:</b> {volume} {html.escape(self.settings.base_asset)}",
            f"<b>Оценка:</b> +${profit}",
            f"<b>Treasury:</b> {treasury}",
        ]
        if error_text:
            sections.append(f"<b>Комментарий:</b> {html.escape(error_text)}")
        if capability_lines:
            sections.append("<b>Готовность:</b>\n" + "\n".join(capability_lines))
        if revalidation_lines:
            sections.append("<b>Revalidation:</b>\n" + "\n".join(revalidation_lines))
        if rm_lines:
            sections.append("<b>RM:</b>\n" + "\n".join(rm_lines))
        if event_lines:
            sections.append("<b>События:</b>\n" + "\n".join(event_lines))
        return "\n".join(sections)

    def _rebalance_text(self, snapshot: dict[str, Any]) -> str:
        balances = snapshot.get("balances") if isinstance(snapshot.get("balances"), dict) else {}
        actions = snapshot.get("actions") if isinstance(snapshot.get("actions"), list) else []
        lines = [
            "Rebalance snapshot",
            f"Total exchange {self.settings.base_asset}: {snapshot.get('total_exchange_usdt', '0')}",
            f"Target per exchange: {snapshot.get('target_per_exchange_usdt', '0')}",
            f"Tolerance: {snapshot.get('tolerance_pct', '0')}%",
        ]
        if balances:
            lines.append(
                f"Balances: Binance={balances.get('binance_usdt', '0')} | Bybit={balances.get('bybit_usdt', '0')}"
            )
        if actions:
            lines.append("Actions:")
            for action in actions:
                lines.append(
                    f"- {action.get('from', '?')} -> {action.get('to', '?')} "
                    f"{action.get('amount', '0')} {action.get('asset', 'USDT')}"
                )
        else:
            lines.append("Actions: none")
        return "\n".join(lines)

    def _signal_route_text(self, signal: dict[str, object]) -> str:
        opportunity_type = str(signal.get("opportunity_type") or "")
        buy_fiat = str(signal.get("buy_fiat") or "")
        sell_fiat = str(signal.get("sell_fiat") or "")
        buy_platform = str(signal.get("buy_platform") or "")
        sell_platform = str(signal.get("sell_platform") or "")
        if opportunity_type == "cross_currency":
            if buy_fiat == sell_fiat:
                return f"{self.settings.base_asset} → {sell_fiat} → {self.settings.base_asset}"
            return f"{self.settings.base_asset} → {sell_fiat} → {buy_fiat} → {self.settings.base_asset}"
        if opportunity_type == "cross_platform":
            return f"{buy_fiat} | {sell_platform.upper()} → {buy_platform.upper()}"
        if buy_platform:
            return f"{buy_platform.upper()} • {buy_fiat}"
        return str(signal.get("route") or "").split(" @ FX ")[0].replace("->", " → ")

    def _is_alert_candidate(self, opportunity: ArbitrageOpportunity) -> bool:
        min_spread, min_profit = self.settings.alert_thresholds_for(
            opportunity.buy_order.fiat,
            opportunity.buy_order.asset,
        )
        return (
            opportunity.rail_status == "confirmed"
            and (
                opportunity.liquidity_status == "liquid"
                or (
                    self.settings.alert_allow_tradable_liquidity
                    and opportunity.liquidity_status == "tradable"
                )
            )
            and opportunity.spread_pct >= min_spread
            and opportunity.estimated_profit_usd >= min_profit
        )

    def _market_text(self, overview: list[dict[str, object]]) -> str:
        generated_at = self.state.market_last_run_at.isoformat() if self.state.market_last_run_at else "n/a"
        lines = [
            "Market research",
            f"updated_at={generated_at}",
        ]
        for item in overview[:10]:
            status = str(item["status"])
            fiat = str(item["fiat"])
            if status == "ACTIVE":
                lines.append(
                    f"{status} {fiat} | profit≈{item['best_profit_usd']} USD | spread={item['best_spread_pct']}% | "
                    f"depth≈{item['tradable_depth_usdt']} | ads 3h={item['ads_3h']} | {item['best_route']}"
                )
            else:
                lines.append(
                    f"{status} {fiat} | depth≈{item['tradable_depth_usdt']} | ads 3h={item['ads_3h']} | {item['reason']}"
                )
        return "\n".join(lines)

    async def _liquidity_text(self) -> str:
        history = await self.raw_order_repository.recent_activity(
            (1, 3),
            asset=self.settings.base_asset,
            fiats=self.settings.fiats,
        )
        current = self._current_market_snapshot()
        lines = [
            "Ликвидность рынка",
            "Это proxy по глубине и числу объявлений, не точный filled volume.",
        ]
        for fiat in self.settings.fiats:
            for platform in self.settings.enabled_platforms:
                buy_key = (platform, fiat, self.settings.base_asset, "buy")
                sell_key = (platform, fiat, self.settings.base_asset, "sell")
                buy_current = current.get(buy_key, {})
                sell_current = current.get(sell_key, {})
                if not buy_current and not sell_current:
                    continue
                buy_depth = Decimal(str(buy_current.get("depth_top5_usdt", 0)))
                sell_depth = Decimal(str(sell_current.get("depth_top5_usdt", 0)))
                buy_quotes = int(buy_current.get("quote_count", 0))
                sell_quotes = int(sell_current.get("quote_count", 0))
                ads_1h = min(self._hist_metric(history, 1, buy_key), self._hist_metric(history, 1, sell_key))
                ads_3h = min(self._hist_metric(history, 3, buy_key), self._hist_metric(history, 3, sell_key))
                lines.append(
                    f"{platform.upper()} {fiat} | depth buy/sell≈{self._fmt_decimal(buy_depth, 1)}/{self._fmt_decimal(sell_depth, 1)} "
                    f"| quotes {buy_quotes}/{sell_quotes} | ads 1h/3h {ads_1h}/{ads_3h}"
                )
        return "\n".join(lines)

    def _current_market_snapshot(self) -> dict[tuple[str, str, str, str], dict[str, Decimal | int]]:
        snapshot: dict[tuple[str, str, str, str], dict[str, Decimal | int]] = {}
        for platform in self.settings.enabled_platforms:
            for fiat in self.settings.fiats:
                for side in ("buy", "sell"):
                    orders = self.state.latest_orders.get(f"{platform}:{self.settings.base_asset}:{fiat}:{side}", [])
                    safe_orders = self.risk_guard.filter_orders(list(orders))
                    top_orders = sorted(safe_orders, key=lambda item: item.available, reverse=True)[:5]
                    snapshot[(platform, fiat, self.settings.base_asset, side)] = {
                        "quote_count": len(safe_orders),
                        "depth_top5_usdt": sum((order.available for order in top_orders), start=Decimal("0")),
                    }
        return snapshot

    @staticmethod
    def _hist_metric(
        history: dict[int, dict[tuple[str, str, str, str], dict[str, float]]],
        hours: int,
        key: tuple[str, str, str, str],
    ) -> int:
        return int(float((history.get(hours, {}).get(key, {}) or {}).get("unique_ads", 0)))

    @staticmethod
    def _liquidity_label(status: str) -> str:
        return {
            "liquid": "высокая",
            "tradable": "средняя",
            "illiquid": "низкая",
        }.get(status, status)

    def _is_authorized_chat(self, chat_id: str) -> bool:
        if not chat_id or chat_id == "None":
            return False
        self._discovered_chat_id = chat_id
        if self.config.chat_id is None:
            return False
        return chat_id == self.config.chat_id

    async def _api(
        self,
        method: str,
        payload: dict[str, Any],
        *,
        timeout_sec: float = 20.0,
        retries: int = 1,
        backoff: tuple[float, ...] = (2.0,),
    ) -> dict[str, Any]:
        return await request_json(
            self.session,
            "POST",
            f"https://api.telegram.org/bot{self.config.token}/{method}",
            json_body=payload,
            logger=self.logger,
            timeout=timeout_sec,
            retries=retries,
            backoff=backoff,
        )
