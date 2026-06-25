from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from p2p_bot.config import Settings
from p2p_bot.models.order import P2POrder
from p2p_bot.state import AppState


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class CounterpartyFilter:
    min_rating: float = 95.0
    min_completed_orders: int = 100
    min_account_days: int = 30
    max_single_trade_usd: float = 500.0
    block_new_accounts: bool = True
    require_online: bool = True
    require_kyc: bool = True
    max_last_active_minutes: int = 30
    allow_unknown_profile_fields: bool = True
    allow_unknown_payment_methods: bool = True


@dataclass(frozen=True)
class PaymentProfile:
    tags: tuple[str, ...]
    known_methods: tuple[str, ...]
    unknown_methods: tuple[str, ...]


@dataclass
class DailyMetrics:
    volume_usd: Decimal = Decimal("0")
    trades: int = 0
    loss_usd: Decimal = Decimal("0")
    open_usd: Decimal = Decimal("0")


class RiskGuard:
    def __init__(self, settings: Settings, state: AppState) -> None:
        filter_cfg = settings.counterparty_filter
        self.settings = settings
        self.state = state
        self.counterparty_filter = CounterpartyFilter(
            min_rating=float(filter_cfg.min_rating),
            min_completed_orders=filter_cfg.min_completed_orders,
            min_account_days=filter_cfg.min_account_days,
            max_single_trade_usd=float(filter_cfg.max_single_trade_usd),
            block_new_accounts=filter_cfg.block_new_accounts,
            require_online=filter_cfg.require_online,
            require_kyc=filter_cfg.require_kyc,
            max_last_active_minutes=filter_cfg.max_last_active_minutes,
            allow_unknown_profile_fields=filter_cfg.allow_unknown_profile_fields,
            allow_unknown_payment_methods=filter_cfg.allow_unknown_payment_methods,
        )
        self.daily_metrics = DailyMetrics()
        self.cooldown_until: datetime | None = None
        self.consecutive_api_errors = 0
        self.weekly_disputes = 0
        self.manual_pause = False

    def is_payment_method_allowed(self, order: P2POrder) -> tuple[bool, str]:
        raw_methods = [str(method).strip() for method in order.payment_methods if str(method).strip()]
        if not raw_methods:
            if self.counterparty_filter.allow_unknown_payment_methods:
                return True, "Payment methods unavailable"
            return False, "Payment methods missing"

        methods = [method.lower() for method in raw_methods]
        for method in methods:
            if any(blocked in method for blocked in self.settings.blocked_payment_keywords):
                return False, f"Blocked payment method: {method}"

        profile = self.payment_profile_for_order(order)
        blocked_wallets = self._blocked_wallet_families(order)
        resolved_tag = self._primary_order_tag(
            order,
            profile,
            blocked_wallets=blocked_wallets,
        )
        if profile.tags and resolved_tag is None and not any(self._tag_supported_for_fiat(tag, order.fiat) for tag in profile.tags):
            return False, f"Payment rail unsupported for {order.fiat}: {', '.join(order.payment_methods)}"
        preferred_payments = self.settings.order_config_for(order.fiat, order.asset).preferred_payments
        if not preferred_payments:
            preferred_payments = self.settings.target_payments

        if not profile.known_methods and not profile.tags:
            return False, f"Payment methods unresolved: {', '.join(order.payment_methods)}"

        if preferred_payments:
            if resolved_tag is None:
                return False, f"Payment methods not preferred: {', '.join(order.payment_methods)}"

        return True, "OK"

    def is_counterparty_safe(self, order: P2POrder) -> tuple[bool, str]:
        filter_cfg = self.counterparty_filter
        raw = order.raw or {}

        if order.merchant_rating > 0 and order.merchant_rating < filter_cfg.min_rating:
            return False, f"Rating too low: {order.merchant_rating:.2f}"
        if order.merchant_orders > 0 and order.merchant_orders < filter_cfg.min_completed_orders:
            return False, f"Too few orders: {order.merchant_orders}"
        if order.merchant_days > 0 and order.merchant_days < filter_cfg.min_account_days:
            return False, f"Account too new: {order.merchant_days} days"
        if raw.get("blocked") in {"Y", "y", True} or raw.get("ban") is True or raw.get("baned") is True:
            return False, "Merchant is blocked/banned"

        merchant_online = order.merchant_online
        if filter_cfg.require_online:
            if merchant_online is False:
                return False, "Merchant is offline"
            if merchant_online is None and not filter_cfg.allow_unknown_profile_fields:
                return False, "Merchant online status is unknown"

        merchant_kyc = order.merchant_kyc
        if filter_cfg.require_kyc:
            if merchant_kyc is False:
                return False, "Merchant is not KYC-verified"
            if merchant_kyc is None and not filter_cfg.allow_unknown_profile_fields:
                return False, "Merchant KYC status is unknown"

        if (
            order.merchant_last_active_minutes is not None
            and order.merchant_last_active_minutes > filter_cfg.max_last_active_minutes
            and filter_cfg.require_online
        ):
            return False, f"Merchant inactive for {order.merchant_last_active_minutes} minutes"

        if (
            not filter_cfg.allow_unknown_profile_fields
            and (
                order.merchant_orders <= 0
                or order.merchant_rating <= 0
                or (
                    filter_cfg.block_new_accounts
                    and order.merchant_days <= 0
                )
            )
        ):
            return False, "Counterparty profile is incomplete"

        return True, "OK"

    def is_order_safe(self, order: P2POrder) -> tuple[bool, str]:
        price_safe, price_reason = self.is_order_price_sane(order)
        if not price_safe:
            return False, price_reason
        live_safe, live_reason = self.is_order_live(order)
        if not live_safe:
            return False, live_reason
        terms_safe, terms_reason = self.is_order_terms_safe(order)
        if not terms_safe:
            return False, terms_reason
        payment_safe, payment_reason = self.is_payment_method_allowed(order)
        if not payment_safe:
            return False, payment_reason
        counterparty_safe, counterparty_reason = self.is_counterparty_safe(order)
        if not counterparty_safe:
            return False, counterparty_reason
        return True, "OK"

    def is_order_live(self, order: P2POrder) -> tuple[bool, str]:
        raw = order.raw or {}
        if order.platform == "binance":
            adv = raw.get("adv") or {}
            if adv.get("isTradable") is False:
                return False, "Ad is not tradable"
        if order.platform == "bybit":
            status = str(raw.get("status") or "").strip()
            if status in {"0", "-1"}:
                return False, f"Bybit ad inactive: {status}"
        if order.platform == "bingx":
            status = str(raw.get("status") or "").strip()
            if status and status != "0":
                return False, f"BingX ad inactive: {status}"
        return True, "OK"

    def is_order_price_sane(self, order: P2POrder) -> tuple[bool, str]:
        if order.asset.upper() != self.settings.base_asset.upper():
            return True, "OK"
        fiat_upper = order.fiat.upper()
        min_price, max_price = self.settings.p2p_price_bounds_for_fiat(fiat_upper)
        if min_price is not None and order.price < min_price:
            return (
                False,
                f"{fiat_upper}/{self.settings.base_asset.upper()} price {order.price} below minimum {min_price}",
            )
        if max_price is not None and order.price > max_price:
            return (
                False,
                f"{fiat_upper}/{self.settings.base_asset.upper()} price {order.price} above maximum {max_price}",
            )
        return True, "OK"

    def filter_orders(self, orders: list[P2POrder]) -> list[P2POrder]:
        return [order for order in orders if self.is_order_safe(order)[0]]

    def payment_profile_for_order(self, order: P2POrder) -> PaymentProfile:
        return self.payment_profile_for_methods(order.payment_methods)

    def payment_profile_for_methods(self, methods: tuple[str, ...] | list[str]) -> PaymentProfile:
        tags: set[str] = set()
        known_methods: list[str] = []
        unknown_methods: list[str] = []
        for raw in methods:
            method = str(raw).strip()
            if not method:
                continue
            canonical = self._canonical_payment_tag(method)
            if canonical is None:
                unknown_methods.append(method)
                continue
            known_methods.append(method)
            tags.add(canonical)
        return PaymentProfile(
            tags=tuple(sorted(tags)),
            known_methods=tuple(known_methods),
            unknown_methods=tuple(unknown_methods),
        )

    def is_order_terms_safe(self, order: P2POrder) -> tuple[bool, str]:
        terms = self._normalized_order_terms_text(order)
        if not terms:
            return True, "OK"
        for blocked in self.settings.blocked_order_terms_keywords:
            keyword = str(blocked).strip().lower()
            if keyword and keyword in terms:
                return False, f"Order terms blocked: {blocked}"
        return True, "OK"

    def _normalized_order_terms_text(self, order: P2POrder) -> str:
        raw = order.raw or {}
        parts: list[str] = []

        if order.platform == "binance":
            adv = raw.get("adv") or {}
            for key in ("remarks", "autoReplyMsg", "storeInformation"):
                value = adv.get(key)
                if value:
                    parts.append(str(value))
        elif order.platform == "bybit":
            for key in ("remark", "makerContact"):
                value = raw.get(key)
                if value:
                    parts.append(str(value))
        elif order.platform == "bingx":
            for key in ("termsDesc", "autoReplyMsg"):
                value = raw.get(key)
                if value:
                    parts.append(str(value))
            response_context = raw.get("_response_context") or {}
            hint = response_context.get("c2cPlatformExtraHint")
            if hint:
                parts.append(str(hint))

        trading_preferences = raw.get("tradingPreferenceSet")
        if isinstance(trading_preferences, (list, tuple)):
            parts.extend(str(value) for value in trading_preferences if value)

        return "\n".join(parts).strip().lower()

    def _blocked_wallet_families(self, order: P2POrder) -> frozenset[str]:
        terms = self._normalized_order_terms_text(order)
        if not terms:
            return frozenset()

        blocked: set[str] = set()
        for keyword in self.settings.blocked_revolut_terms_keywords:
            pattern = str(keyword).strip().lower()
            if pattern and pattern in terms:
                blocked.add("revolut")
                break
        for keyword in self.settings.blocked_wise_terms_keywords:
            pattern = str(keyword).strip().lower()
            if pattern and pattern in terms:
                blocked.add("wise")
                break
        return frozenset(blocked)

    def route_summary(self, buy_order: P2POrder, sell_order: P2POrder) -> tuple[str, str, str]:
        route_status, buy_summary, sell_summary, _, _, _, _ = self.route_details(buy_order, sell_order)
        return route_status, buy_summary, sell_summary

    def route_details(
        self,
        buy_order: P2POrder,
        sell_order: P2POrder,
    ) -> tuple[str, str, str, str | None, str | None, str, str]:
        buy_profile = self.payment_profile_for_order(buy_order)
        sell_profile = self.payment_profile_for_order(sell_order)
        sell_blocked_wallets = self._blocked_wallet_families(sell_order)
        buy_blocked_wallets = self._blocked_wallet_families(buy_order)
        sell_rail = self._primary_receive_tag(
            sell_order,
            sell_profile,
            blocked_wallets=sell_blocked_wallets,
        )
        buy_rail = self._primary_send_tag(
            buy_order,
            buy_profile,
            preferred_wallet=None,
            blocked_wallets=buy_blocked_wallets,
        )
        buy_status = "confirmed" if buy_rail is not None else "unknown"
        sell_status = "confirmed" if sell_rail is not None else "unknown"

        if buy_status == "confirmed" and sell_status == "confirmed":
            route_status = "confirmed"
        elif buy_status == "unknown" and sell_status == "unknown":
            route_status = "unknown"
        else:
            route_status = "mixed"

        return (
            route_status,
            self._payment_profile_summary(buy_profile),
            self._payment_profile_summary(sell_profile),
            buy_rail,
            sell_rail,
            self._user_method_label(buy_rail),
            self._user_method_label(sell_rail),
        )

    def merchant_summary(self, order: P2POrder) -> str:
        raw = order.raw or {}
        components = raw.get("components") if isinstance(raw, dict) else None
        prefix = "Мерчант"
        if components:
            prefix = f"Лестница ({len(components)} мерч.)"
        parts: list[str] = []
        merchant_name = order.resolved_merchant_name()
        if not merchant_name and isinstance(components, list):
            merchant_names: list[str] = []
            for component in components:
                if not isinstance(component, dict):
                    continue
                candidate = str(component.get("merchant_name") or "").strip()
                if candidate and candidate not in merchant_names:
                    merchant_names.append(candidate)
            if merchant_names:
                preview = merchant_names[:3]
                merchant_name = ", ".join(preview)
                if len(merchant_names) > len(preview):
                    merchant_name = f"{merchant_name} +{len(merchant_names) - len(preview)}"
        if merchant_name:
            parts.append(merchant_name)
        if order.merchant_rating > 0:
            parts.append(f"⭐ {order.merchant_rating:.1f}%")
        if order.merchant_orders > 0:
            parts.append(f"{order.merchant_orders} сделок")
        if order.merchant_days > 0:
            parts.append(f"{order.merchant_days} дн")
        if order.merchant_kyc is True:
            parts.append("KYC")
        elif order.merchant_kyc is False:
            parts.append("без KYC")
        if order.merchant_online is True:
            parts.append("🟢")
        elif order.merchant_online is False and order.merchant_last_active_minutes is not None:
            parts.append(f"🕒 {order.merchant_last_active_minutes} мин назад")
        elif order.merchant_online is False:
            parts.append("⚪")
        if not parts:
            return ""
        return f"{prefix}: " + " | ".join(parts)

    def max_safe_volume_usdt(self, *orders: P2POrder) -> Decimal:
        max_single = Decimal(str(self.counterparty_filter.max_single_trade_usd))
        limits = [max_single]
        for order in orders:
            limits.append(order.available)
            if order.price > 0:
                limits.append(order.max_amount / order.price)
        return max(min(limits), Decimal("0"))

    def pause(self, reason: str) -> None:
        self.manual_pause = True
        self.state.set_paused(True, reason)

    def resume(self) -> None:
        self.manual_pause = False
        self.state.set_paused(False)

    def trigger_kill_switch(self, reason: str) -> None:
        self.manual_pause = True
        self.state.set_paused(True, reason)

    def start_cooldown(self, hours: int, reason: str | None = None) -> None:
        self.cooldown_until = utc_now() + timedelta(hours=hours)
        if reason:
            self.state.kill_switch_reason = reason

    def can_trade(self) -> tuple[bool, str]:
        if self.state.paused or self.manual_pause:
            return False, self.state.kill_switch_reason or "Manual pause"
        if self.cooldown_until and utc_now() < self.cooldown_until:
            return False, f"Cooldown active until {self.cooldown_until.isoformat()}"
        limits = self.settings.daily_limits
        if self.daily_metrics.volume_usd >= limits.max_volume_usd:
            return False, "Max daily volume reached"
        if self.daily_metrics.trades >= limits.max_trades:
            return False, "Max daily trades reached"
        if self.daily_metrics.loss_usd >= limits.max_loss_usd:
            return False, "Max daily loss reached"
        if self.daily_metrics.open_usd >= limits.max_open_usd:
            return False, "Max open exposure reached"
        return True, "OK"

    def record_trade(self, volume_usd: Decimal, profit_usd: Decimal) -> None:
        self.daily_metrics.volume_usd += volume_usd
        self.daily_metrics.trades += 1
        if profit_usd < 0:
            self.daily_metrics.loss_usd += abs(profit_usd)

    def record_api_error(self) -> bool:
        self.consecutive_api_errors += 1
        if self.consecutive_api_errors > 10:
            self.trigger_kill_switch("API errors exceeded threshold")
            return True
        return False

    def record_api_success(self) -> None:
        self.consecutive_api_errors = 0

    def record_dispute(self) -> None:
        self.weekly_disputes += 1
        self.start_cooldown(2, "Cooldown after dispute")
        if self.weekly_disputes > 3:
            self.trigger_kill_switch("More than 3 disputes this week")

    def status_snapshot(self) -> dict[str, Any]:
        return {
            "paused": self.state.paused,
            "kill_switch_reason": self.state.kill_switch_reason,
            "cooldown_until": self.cooldown_until.isoformat() if self.cooldown_until else None,
            "daily_volume_usd": str(self.daily_metrics.volume_usd),
            "daily_trades": self.daily_metrics.trades,
            "daily_loss_usd": str(self.daily_metrics.loss_usd),
            "open_usd": str(self.daily_metrics.open_usd),
            "consecutive_api_errors": self.consecutive_api_errors,
        }

    def _payment_leg_status(self, order: P2POrder, profile: PaymentProfile) -> str:
        return "confirmed" if self._primary_order_tag(order, profile) is not None else "unknown"

    def _payment_profile_summary(self, profile: PaymentProfile) -> str:
        if profile.known_methods:
            return ", ".join(profile.known_methods[:3])
        if profile.unknown_methods:
            sample = ", ".join(profile.unknown_methods[:3])
            return f"unknown({sample})"
        return "n/a"

    def _matched_receive_method(
        self,
        order: P2POrder,
        profile: PaymentProfile,
    ) -> str:
        tag = self._primary_receive_tag(
            order,
            profile,
            blocked_wallets=self._blocked_wallet_families(order),
        )
        return self._user_method_label(tag)

    def _matched_send_method(
        self,
        order: P2POrder,
        profile: PaymentProfile,
        *,
        preferred_wallet: str | None = None,
    ) -> str:
        tag = self._primary_send_tag(
            order,
            profile,
            preferred_wallet=preferred_wallet,
            blocked_wallets=self._blocked_wallet_families(order),
        )
        return self._user_method_label(tag)

    def _matched_user_method(
        self,
        order: P2POrder,
        profile: PaymentProfile,
        *,
        preferred_wallet: str | None = None,
    ) -> str:
        tag = self._primary_order_tag(
            order,
            profile,
            preferred_wallet=preferred_wallet,
            blocked_wallets=self._blocked_wallet_families(order),
        )
        return self._user_method_label(tag)

    def _primary_receive_tag(
        self,
        order: P2POrder,
        profile: PaymentProfile,
        *,
        blocked_wallets: frozenset[str] = frozenset(),
    ) -> str | None:
        fiat_upper = order.fiat.upper()
        tags = set(profile.tags)
        methods_text = self._payment_methods_text(order.payment_methods, profile)
        if not tags:
            return None

        if "fiat_balance" in tags:
            return "fiat_balance"

        # Receive-side logic:
        # - explicit wallet methods always win
        # - generic external bank rails can still land on one of our receive
        #   methods on that exchange, as long as the currency is supported and
        #   the ad terms do not forbid that wallet
        # - this models the real flow where the counterparty's source bank does
        #   not have to match our destination wallet label exactly
        if (
            "revolut_balance" in tags
            and "revolut" not in blocked_wallets
            and fiat_upper in self.settings.revolut_wallet_fiats
        ):
            return "revolut_balance"
        if (
            "revolut_bank_transfer" in tags
            and "revolut" not in blocked_wallets
            and self._supports_revolut_receive_bank(
                fiat=fiat_upper,
                methods_text=methods_text,
            )
        ):
            return "revolut_bank_transfer"
        if "revolut_card" in tags and "revolut" not in blocked_wallets:
            return "revolut_card"
        if (
            "wise_balance" in tags
            and "wise" not in blocked_wallets
            and fiat_upper in self.settings.wise_wallet_fiats
        ):
            return "wise_balance"
        if (
            "wise_bank_transfer" in tags
            and "wise" not in blocked_wallets
            and self._supports_wise_receive_bank(
                fiat=fiat_upper,
                methods_text=methods_text,
            )
        ):
            return "wise_bank_transfer"
        if "wise_card" in tags and "wise" not in blocked_wallets:
            return "wise_card"

        if "sepa" in tags and fiat_upper == "EUR":
            return "sepa"
        if "bank_transfer" in tags:
            rail = self._preferred_receive_external_rail(
                platform=order.platform,
                fiat=fiat_upper,
                blocked_wallets=blocked_wallets,
                method_tag="bank_transfer",
                methods_text=methods_text,
            )
            if rail is not None:
                return rail
        if "sepa" in tags:
            rail = self._preferred_receive_external_rail(
                platform=order.platform,
                fiat=fiat_upper,
                blocked_wallets=blocked_wallets,
                method_tag="sepa",
                methods_text=methods_text,
            )
            if rail is not None:
                return rail
        if "blik" in tags and fiat_upper == "PLN":
            return "blik"
        if "zen" in tags:
            return "zen"
        return None

    def _preferred_receive_external_rail(
        self,
        *,
        platform: str,
        fiat: str,
        blocked_wallets: frozenset[str] = frozenset(),
        method_tag: str,
        methods_text: str = "",
    ) -> str | None:
        fiat_upper = fiat.upper()
        platform_lower = str(platform).lower()
        local_override = self._preferred_local_bank_wallet(
            fiat=fiat_upper,
            methods_text=methods_text,
            blocked_wallets=blocked_wallets,
        )
        if local_override is not None:
            return local_override

        # Exchange-specific receive methods that we actually expose.
        # Binance: no Revolut receive.
        # Bybit: Revolut receive is available and preferred where supported.
        if platform_lower == "bybit":
            if (
                "revolut" not in blocked_wallets
                and self._supports_revolut_receive_bank(
                    fiat=fiat_upper,
                    methods_text=methods_text,
                )
            ):
                return "revolut_bank_transfer"
            if (
                "wise" not in blocked_wallets
                and self._supports_wise_receive_bank(
                    fiat=fiat_upper,
                    methods_text=methods_text,
                )
            ):
                return "wise_bank_transfer"
        elif platform_lower == "binance":
            if (
                "wise" not in blocked_wallets
                and self._supports_wise_receive_bank(
                    fiat=fiat_upper,
                    methods_text=methods_text,
                )
            ):
                return "wise_bank_transfer"
        elif platform_lower == "bingx":
            if (
                "wise" not in blocked_wallets
                and self._supports_wise_receive_bank(
                    fiat=fiat_upper,
                    methods_text=methods_text,
                )
            ):
                return "wise_bank_transfer"

        if method_tag == "sepa" and fiat_upper == "EUR":
            return "sepa"
        if method_tag == "bank_transfer" and fiat_upper in self.settings.local_bank_fiats:
            return "bank_transfer"
        return None

    def _primary_send_tag(
        self,
        order: P2POrder,
        profile: PaymentProfile,
        *,
        preferred_wallet: str | None = None,
        blocked_wallets: frozenset[str] = frozenset(),
    ) -> str | None:
        fiat_upper = order.fiat.upper()
        tags = set(profile.tags)
        methods_text = self._payment_methods_text(order.payment_methods, profile)
        if not tags:
            return None

        if "fiat_balance" in tags:
            return "fiat_balance"

        if (
            "revolut_balance" in tags
            and "revolut" not in blocked_wallets
            and fiat_upper in self.settings.revolut_wallet_fiats
        ):
            return "revolut_balance"
        if (
            "revolut_bank_transfer" in tags
            and "revolut" not in blocked_wallets
            and fiat_upper in self.settings.revolut_bank_transfer_fiats
        ):
            return "revolut_bank_transfer"
        if "revolut_card" in tags and "revolut" not in blocked_wallets:
            return "revolut_card"

        if (
            "wise_balance" in tags
            and "wise" not in blocked_wallets
            and fiat_upper in self.settings.wise_wallet_fiats
        ):
            return "wise_balance"
        if (
            "wise_bank_transfer" in tags
            and "wise" not in blocked_wallets
            and self._supports_wise_send_bank(
                fiat=fiat_upper,
                methods_text=methods_text,
            )
        ):
            return "wise_bank_transfer"
        if "wise_card" in tags and "wise" not in blocked_wallets:
            return "wise_card"

        if "sepa" in tags:
            return self._preferred_external_bank_rail(
                fiat=fiat_upper,
                preferred_wallet=preferred_wallet,
                method_tag="sepa",
                blocked_wallets=blocked_wallets,
                methods_text=methods_text,
            )
        if "bank_transfer" in tags:
            return self._preferred_external_bank_rail(
                fiat=fiat_upper,
                preferred_wallet=preferred_wallet,
                method_tag="bank_transfer",
                blocked_wallets=blocked_wallets,
                methods_text=methods_text,
            )
        if "blik" in tags and fiat_upper == "PLN":
            if self._has_polish_bank_transfer_option(methods_text):
                rail = self._preferred_external_bank_rail(
                    fiat=fiat_upper,
                    preferred_wallet=preferred_wallet,
                    method_tag="bank_transfer",
                    blocked_wallets=blocked_wallets,
                    methods_text=methods_text,
                )
                if rail is not None:
                    return rail
            return "blik"

        if "zen" in tags:
            if fiat_upper == "PLN":
                rail = self._preferred_external_bank_rail(
                    fiat=fiat_upper,
                    preferred_wallet=preferred_wallet,
                    method_tag="bank_transfer",
                    blocked_wallets=blocked_wallets,
                    methods_text=methods_text,
                )
                if rail is not None:
                    return rail
            return "zen"
        return None

    def _primary_order_tag(
        self,
        order: P2POrder,
        profile: PaymentProfile,
        *,
        preferred_wallet: str | None = None,
        blocked_wallets: frozenset[str] = frozenset(),
    ) -> str | None:
        # Bot-normalized `order.side` always describes OUR action:
        # - side "sell": we sell USDT and RECEIVE fiat
        # - side "buy": we buy USDT and SEND fiat
        if str(order.side).lower() == "sell":
            return self._primary_receive_tag(
                order,
                profile,
                blocked_wallets=blocked_wallets,
            )
        return self._primary_send_tag(
            order,
            profile,
            preferred_wallet=preferred_wallet,
            blocked_wallets=blocked_wallets,
        )

    def _primary_payment_tag(
        self,
        order: P2POrder,
        profile: PaymentProfile,
        *,
        preferred_wallet: str | None = None,
        blocked_wallets: frozenset[str] = frozenset(),
    ) -> str | None:
        # Backwards-compatible alias for legacy callers; follow order-side semantics.
        return self._primary_order_tag(
            order,
            profile,
            preferred_wallet=preferred_wallet,
            blocked_wallets=blocked_wallets,
        )

    def _preferred_external_bank_rail(
        self,
        *,
        fiat: str,
        preferred_wallet: str | None,
        method_tag: str,
        blocked_wallets: frozenset[str] = frozenset(),
        methods_text: str = "",
    ) -> str | None:
        fiat_upper = fiat.upper()
        local_override = self._preferred_local_bank_wallet(
            fiat=fiat_upper,
            methods_text=methods_text,
            blocked_wallets=blocked_wallets,
        )
        if local_override is not None:
            return local_override
        if method_tag == "bank_transfer" and self._method_country_hint(methods_text) == "GE":
            # Georgian bank transfers are SWIFT-only in our treasury setup.
            # Do not treat them as a valid final send rail until dedicated GE rails exist.
            return None

        if (
            preferred_wallet == "revolut"
            and "revolut" not in blocked_wallets
            and fiat_upper in self.settings.revolut_bank_transfer_fiats
        ):
            return "revolut_bank_transfer"
        if (
            preferred_wallet == "revolut"
            and method_tag == "bank_transfer"
            and "revolut" not in blocked_wallets
            and self._supports_revolut_manual_card_send(
                fiat=fiat_upper,
                methods_text=methods_text,
            )
        ):
            return "revolut_card_manual"
        if (
            preferred_wallet == "wise"
            and "wise" not in blocked_wallets
            and self._supports_wise_send_bank(
                fiat=fiat_upper,
                methods_text=methods_text,
            )
        ):
            return "wise_bank_transfer"
        if preferred_wallet == "bank":
            if method_tag == "sepa" and fiat_upper == "EUR":
                return "sepa"
            if method_tag == "bank_transfer" and fiat_upper in self.settings.local_bank_fiats:
                return "bank_transfer"
        if preferred_wallet is not None:
            if method_tag == "sepa" and fiat_upper == "EUR":
                return "sepa"
            if method_tag == "bank_transfer" and fiat_upper in self.settings.local_bank_fiats:
                return "bank_transfer"

        if (
            "wise" not in blocked_wallets
            and self._supports_wise_send_bank(
                fiat=fiat_upper,
                methods_text=methods_text,
            )
        ):
            return "wise_bank_transfer"
        if "revolut" not in blocked_wallets and fiat_upper in self.settings.revolut_bank_transfer_fiats:
            return "revolut_bank_transfer"
        if (
            method_tag == "bank_transfer"
            and "revolut" not in blocked_wallets
            and self._supports_revolut_manual_card_send(
                fiat=fiat_upper,
                methods_text=methods_text,
            )
        ):
            return "revolut_card_manual"
        if method_tag == "sepa" and fiat_upper == "EUR":
            return "sepa"
        if method_tag == "bank_transfer" and fiat_upper in self.settings.local_bank_fiats:
            return "bank_transfer"
        return None

    @staticmethod
    def _payment_methods_text(methods: tuple[str, ...] | list[str], profile: PaymentProfile) -> str:
        parts = [*[str(item) for item in methods], *profile.known_methods, *profile.unknown_methods]
        return " | ".join(part.strip().lower() for part in parts if str(part).strip())

    @staticmethod
    def _method_country_hint(methods_text: str) -> str | None:
        text = (methods_text or "").lower()
        hints = (
            ("GB", ("faster payments", "uk bank transfer", "sort code", "barclays", "lloyds", "natwest", "hsbc", "halifax", "monzo", "starling")),
            ("CA", ("interac", "rbc", "royal bank", "td bank", "scotiabank", "cibc", "bmo")),
            ("AU", ("bsb", "payid", "commonwealth", "westpac", "nab", "australia and new zealand")),
            ("NZ", ("kiwibank", "anz nz", "asb", "bnz", "new zealand")),
            ("SG", ("fast", "paynow", "dbs", "ocbc", "uob", "singapore")),
            ("PH", ("instapay", "pesonet", "wise pilipinas", "gcash", "maya", "bpi", "bdo", "philippines")),
            ("US", ("ach", "wire", "zelle", "routing", "aba", "chase", "bofa", "bank of america", "wells fargo", "citibank")),
            ("HU", ("azonnali", "otp bank", "otp", "k&h", "raiffeisen hu")),
            ("RO", ("brd", "bcr", "bank transilvania", "bt pay", "btpay", "raiffeisen bank romania", "ing romania", "unicredit bank romania", "romania")),
            ("CZ", ("czech republic", "ceska sporitelna", "komercni banka", "air bank", "fio banka", "moneta", "csob")),
            ("SE", ("swedbank", "seb", "handelsbanken", "lansforsakringar", "ica banken", "sweden")),
            ("NO", ("dnb", "sparebank", "sbanken", "norway", "nordea no")),
            ("DK", ("danske bank", "jyske bank", "sydbank", "nykredit", "denmark", "nordea dk")),
            ("HK", ("hong kong", "hang seng", "hsbc hk", "bank of china hk")),
            ("IN", ("upi", "hdfc", "icici", "state bank of india", "sbi", "axis bank", "india")),
            ("IL", ("bank hapoalim", "bank leumi", "mizrahi", "discount bank", "mercantile", "israel")),
            ("CN", ("alipay", "wechat pay", "bank of china", "icbc", "china construction bank", "agricultural bank of china", "china")),
            ("GE", ("bank of georgia", "tbc bank", "liberty bank", "terabank", "credo bank", "basisbank", "cartu bank", "georgia")),
            ("PL", ("pko", "blik", "zen", "santander poland", "millennium", "mbank", "ing poland")),
        )
        for country, keywords in hints:
            if any(keyword in text for keyword in keywords):
                return country
        return None

    @classmethod
    def _method_currency_mismatch(cls, *, methods_text: str, fiat: str) -> bool:
        hint = cls._method_country_hint(methods_text)
        if hint is None:
            return False
        country_currency = {
            "GB": "GBP",
            "CA": "CAD",
            "AU": "AUD",
            "NZ": "NZD",
            "SG": "SGD",
            "PH": "PHP",
            "US": "USD",
            "HU": "HUF",
            "RO": "RON",
            "CZ": "CZK",
            "SE": "SEK",
            "NO": "NOK",
            "DK": "DKK",
            "HK": "HKD",
            "IN": "INR",
            "IL": "ILS",
            "CN": "CNY",
            "GE": "GEL",
            "PL": "PLN",
        }.get(hint)
        return bool(country_currency and fiat.upper() != country_currency)

    def _supports_revolut_receive_bank(
        self,
        *,
        fiat: str,
        methods_text: str,
    ) -> bool:
        fiat_upper = fiat.upper()
        if fiat_upper not in self.settings.revolut_receive_bank_fiats:
            return False
        return not self._method_currency_mismatch(methods_text=methods_text, fiat=fiat_upper)

    def _supports_revolut_manual_card_send(
        self,
        *,
        fiat: str,
        methods_text: str,
    ) -> bool:
        fiat_upper = fiat.upper()
        if fiat_upper not in self.settings.revolut_card_manual_fiats:
            return False
        if self._method_currency_mismatch(methods_text=methods_text, fiat=fiat_upper):
            return False
        required_country = {
            "AUD": "AU",
            "CAD": "CA",
            "SGD": "SG",
            "RON": "RO",
            "CZK": "CZ",
            "SEK": "SE",
            "NOK": "NO",
            "DKK": "DK",
        }.get(fiat_upper)
        if required_country is None:
            return False
        return self._method_country_hint(methods_text) == required_country

    def _supports_wise_receive_bank(
        self,
        *,
        fiat: str,
        methods_text: str,
    ) -> bool:
        fiat_upper = fiat.upper()
        if fiat_upper not in self.settings.wise_receive_bank_fiats:
            return False
        if self._method_currency_mismatch(methods_text=methods_text, fiat=fiat_upper):
            return False
        always_local = {"EUR"}
        country_required = {
            "GBP": "GB",
            "USD": "US",
            "AUD": "AU",
            "NZD": "NZ",
            "HUF": "HU",
            "SGD": "SG",
            "PHP": "PH",
            "CAD": "CA",
        }
        if fiat_upper in always_local:
            return True
        required_country = country_required.get(fiat_upper)
        if required_country is None:
            return False
        return self._method_country_hint(methods_text) == required_country

    def _supports_wise_send_bank(
        self,
        *,
        fiat: str,
        methods_text: str,
    ) -> bool:
        fiat_upper = fiat.upper()
        if fiat_upper not in self.settings.wise_bank_transfer_fiats:
            return False
        if self._method_currency_mismatch(methods_text=methods_text, fiat=fiat_upper):
            return False
        always_local = {"EUR"}
        country_required = {
            "GBP": "GB",
            "AUD": "AU",
            "HUF": "HU",
            "SGD": "SG",
            "RON": "RO",
            "PHP": "PH",
            "CZK": "CZ",
            "DKK": "DK",
            "HKD": "HK",
            "INR": "IN",
            "ILS": "IL",
            "CNY": "CN",
        }
        if fiat_upper in always_local:
            return True
        required_country = country_required.get(fiat_upper)
        if required_country is None:
            return False
        return self._method_country_hint(methods_text) == required_country

    def _preferred_local_bank_wallet(
        self,
        *,
        fiat: str,
        methods_text: str,
        blocked_wallets: frozenset[str],
    ) -> str | None:
        fiat_upper = fiat.upper()
        if self._method_currency_mismatch(methods_text=methods_text, fiat=fiat_upper):
            return None

        text = (methods_text or "").lower()
        if "interac" in text and fiat_upper == "CAD" and "wise" not in blocked_wallets:
            return "wise_bank_transfer"
        if any(keyword in text for keyword in ("otp bank", "otp")) and fiat_upper == "HUF" and "wise" not in blocked_wallets:
            return "wise_bank_transfer"
        if "zen" in text and fiat_upper == "PLN":
            return "zen"
        return None

    @staticmethod
    def _has_polish_bank_transfer_option(methods_text: str) -> bool:
        text = (methods_text or "").lower()
        return any(
            keyword in text
            for keyword in (
                "bank transfer",
                "pko",
                "bank pekao",
                "pekao",
                "mbank",
                "ing",
                "santander",
                "millennium",
                "bnp paribas",
            )
        )

    def _tag_supported_for_fiat(self, tag: str | None, fiat: str) -> bool:
        if not tag:
            return False
        fiat_upper = fiat.upper()
        if tag == "blik":
            return fiat_upper == "PLN"
        if tag == "sepa":
            return self._preferred_external_bank_rail(
                fiat=fiat_upper,
                preferred_wallet=None,
                method_tag="sepa",
            ) is not None
        if tag == "bank_transfer":
            return self._preferred_external_bank_rail(
                fiat=fiat_upper,
                preferred_wallet=None,
                method_tag="bank_transfer",
            ) is not None
        if tag == "revolut_balance":
            return fiat_upper in self.settings.revolut_wallet_fiats
        if tag == "revolut_bank_transfer":
            return fiat_upper in (
                set(self.settings.revolut_receive_bank_fiats) | set(self.settings.revolut_bank_transfer_fiats)
            )
        if tag == "revolut_card_manual":
            return fiat_upper in set(self.settings.revolut_card_manual_fiats)
        if tag == "wise_balance":
            return fiat_upper in self.settings.wise_wallet_fiats
        if tag == "wise_bank_transfer":
            return fiat_upper in (
                set(self.settings.wise_receive_bank_fiats) | set(self.settings.wise_bank_transfer_fiats)
            )
        return True

    @staticmethod
    def _user_method_label(tag: str | None) -> str:
        return {
            "revolut_balance": "Revolut",
            "revolut_bank_transfer": "Revolut",
            "revolut_card": "Revolut",
            "revolut_card_manual": "Revolut (карта)",
            "wise_balance": "Wise",
            "wise_bank_transfer": "Wise",
            "wise_card": "Wise",
            "zen": "ZEN",
            "blik": "BLIK",
            "fiat_balance": "Фиат внутри биржи",
            "sepa": "SEPA",
            "bank_transfer": "Банковский перевод",
            None: "Не определено",
        }.get(tag, tag or "Не определено")

    @staticmethod
    def _wallet_family_for_tag(tag: str | None) -> str | None:
        normalized = (tag or "").lower()
        if normalized.startswith("revolut_"):
            return "revolut"
        if normalized.startswith("wise_"):
            return "wise"
        if normalized in {"bank_transfer", "bank_fx", "sepa", "blik", "zen"}:
            return "bank"
        return None

    @staticmethod
    def _canonical_payment_tag(method: str) -> str | None:
        lowered = method.strip().lower()
        if not lowered:
            return None
        if not any(char.isalpha() for char in lowered):
            return None
        bankish = any(
            keyword in lowered
            for keyword in (
                "bank",
                "transfer",
                "sepa",
                "iban",
                "swift",
            )
        )
        cardish = any(keyword in lowered for keyword in ("card", "visa", "mastercard"))
        if "revolut" in lowered:
            if cardish:
                return "revolut_card"
            if bankish:
                return "revolut_bank_transfer"
            return "revolut_balance"
        if "blik" in lowered:
            return "blik"
        if "sepa" in lowered:
            return "sepa"
        if "zen" in lowered:
            return "zen"
        if "wise" in lowered:
            if cardish:
                return "wise_card"
            if bankish:
                return "wise_bank_transfer"
            return "wise_balance"
        if "interac" in lowered:
            return "bank_transfer"
        if "balance" in lowered:
            return "fiat_balance"
        if bankish or any(
            keyword in lowered
            for keyword in (
                "pko",
                "polski",
                "pekao",
                "alior",
                "santander",
                "mbank",
                "ing",
                "millennium",
                "n26",
                "paysera",
                "bunq",
                "monese",
                "bbva",
                "ziraat",
                "kuveyt",
                "vakif",
                "vakıf",
                "isbank",
                "işbank",
                "akbank",
                "deniz",
                "garanti",
                "teb",
                "qnb",
                "fiba",
                "yapi kredi",
                "yapikredi",
                "aval",
                "transilvania",
                "raiffeisenbank",
                "raiffeisen",
                "konto",
                "payid",
                "paynow",
                "fps",
                "upi",
                "imps",
                "alipay",
                "wechat",
                "wechat pay",
                "faster",
                "fast",
                "dbs",
                "ocbc",
                "otp",
                "bcr",
                "brd",
                "barclays",
                "lloyds",
                "natwest",
                "hsbc",
                "bt pay",
                "btpay",
            )
        ):
            return "bank_transfer"
        return None
