from __future__ import annotations

import os
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path


DEFAULT_TARGET_PAYMENTS = (
    "Revolut",
    "SEPA Transfer",
    "Wise",
    "Bank Transfer (Poland)",
    "mBank",
    "PKO",
    "Santander PL",
)

DEFAULT_BLOCKED_PAYMENT_KEYWORDS = (
    "sbp",
    "tinkoff",
    "qiwi",
    "yoomoney",
    "sber",
    "vtb",
    "mir",
)

DEFAULT_BLOCKED_ORDER_TERMS_KEYWORDS = (
    "same bank only",
    "only same bank",
    "local bank only",
    "only local bank",
    "russian banks only",
    "only russian bank",
    "only russian banks",
    "только россий",
    "только русский банк",
    "только на российский банк",
    "только местный банк",
    "только свой банк",
)

DEFAULT_BLOCK_REVOLUT_TERMS_KEYWORDS = (
    "no revolut",
    "without revolut",
    "do not use revolut",
    "don't use revolut",
    "revolut not accepted",
    "revolut not allowed",
    "без revolut",
    "не revolut",
    "без револют",
    "не револют",
    "не на revolut",
    "не на револют",
)

DEFAULT_BLOCK_WISE_TERMS_KEYWORDS = (
    "no wise",
    "without wise",
    "do not use wise",
    "don't use wise",
    "wise not accepted",
    "wise not allowed",
    "без wise",
    "не wise",
    "не на wise",
)


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def _bool_env(name: str, default: bool) -> bool:
    value = _env(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int) -> int:
    value = _env(name)
    return int(value) if value is not None else default


def _decimal_env(name: str, default: str) -> Decimal:
    value = _env(name)
    return Decimal(value) if value is not None else Decimal(default)


def _list_env(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    value = _env(name)
    if value is None:
        return default
    items = [item.strip() for item in value.split(",") if item.strip()]
    return tuple(items) if items else default


def _dict_env(name: str, default: dict[str, str]) -> dict[str, str]:
    value = _env(name)
    if value is None:
        return dict(default)
    result: dict[str, str] = {}
    for item in value.split(","):
        raw = item.strip()
        if not raw or ":" not in raw:
            continue
        key, mapped = raw.split(":", 1)
        key = key.strip()
        mapped = mapped.strip()
        if key and mapped:
            result[key] = mapped
    return result or dict(default)


def _decimal_dict_env(name: str, default: dict[str, Decimal]) -> dict[str, Decimal]:
    value = _env(name)
    if value is None:
        return dict(default)
    result: dict[str, Decimal] = {}
    for item in value.split(","):
        raw = item.strip()
        if not raw or ":" not in raw:
            continue
        key, raw_value = raw.split(":", 1)
        key = key.strip().upper()
        raw_value = raw_value.strip()
        if not key or not raw_value:
            continue
        result[key] = Decimal(raw_value)
    return result or dict(default)


@dataclass
class PairOrderConfig:
    pair: str
    min_spread_pct: Decimal
    target_spread_pct: Decimal
    alert_min_spread_pct: Decimal
    alert_min_profit_usd: Decimal
    max_single_order_usd: Decimal
    min_order_fiat: Decimal
    preferred_payments: tuple[str, ...]


@dataclass
class RailFeeProfile:
    send_pct: Decimal = Decimal("0")
    send_fixed_usd: Decimal = Decimal("0")
    send_fixed_amount: Decimal = Decimal("0")
    send_fixed_ccy: str = ""
    receive_pct: Decimal = Decimal("0")
    receive_fixed_usd: Decimal = Decimal("0")
    receive_fixed_amount: Decimal = Decimal("0")
    receive_fixed_ccy: str = ""
    fx_pct: Decimal = Decimal("0")
    fx_fixed_usd: Decimal = Decimal("0")
    fx_fixed_amount: Decimal = Decimal("0")
    fx_fixed_ccy: str = ""
    fx_rate_markup_pct: Decimal = Decimal("0")


@dataclass
class CounterpartyFilterConfig:
    min_rating: Decimal = Decimal("93")
    min_completed_orders: int = 50
    min_account_days: int = 30
    max_single_trade_usd: Decimal = Decimal("3000")
    block_new_accounts: bool = True
    require_online: bool = True
    require_kyc: bool = True
    max_last_active_minutes: int = 30
    allow_unknown_profile_fields: bool = True
    allow_unknown_payment_methods: bool = True


@dataclass
class DailyLimitsConfig:
    max_volume_usd: Decimal = Decimal("30000")
    max_trades: int = 20
    max_loss_usd: Decimal = Decimal("100")
    max_open_usd: Decimal = Decimal("4500")


@dataclass
class BybitConfig:
    api_key: str | None
    api_secret: str | None
    base_url: str
    legacy_public_scan_url: str
    cookies: str | None
    recv_window_ms: int
    use_official_p2p_api: bool
    use_legacy_public_scan: bool
    balance_account_type: str
    payment_code_map: dict[str, str]

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret)


@dataclass
class BinanceConfig:
    api_key: str | None
    api_secret: str | None
    base_url: str
    adv_search_path: str
    rows: int = 20
    rate_limit_max_calls: int = 1
    rate_limit_period_sec: float = 3.2


@dataclass
class BinanceExecutionConfig:
    api_key: str | None
    api_secret: str | None
    base_url: str
    reserve_timeout_sec: int
    mode: str

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret)


@dataclass
class BingXConfig:
    api_key: str | None
    api_secret: str | None
    base_url: str
    p2p_api_base: str
    fiat_site_url: str
    supported_payment_methods: tuple[str, ...]
    supported_fiats: tuple[str, ...]
    rows: int = 20
    rate_limit_max_calls: int = 4
    rate_limit_period_sec: float = 1.0
    p2p_app_version: str = "4.80.0"
    p2p_platform_id: int = 30
    p2p_main_app_id: int = 10009

    @property
    def has_credentials(self) -> bool:
        return bool(self.api_key and self.api_secret)


@dataclass
class RevolutBusinessConfig:
    client_id: str | None
    client_secret: str | None
    base_url: str
    webhook_secret: str | None
    enabled: bool

    @property
    def has_credentials(self) -> bool:
        return bool(self.client_id and self.client_secret)


@dataclass
class ExchangeExecutionConfig:
    enabled: bool
    dry_run: bool
    reserve_both_legs: bool
    require_manual_confirm: bool
    session_ttl_sec: int
    max_capital_per_signal_pct: Decimal
    rebalance_tolerance_pct: Decimal
    revalidation_max_price_drift_pct: Decimal
    allowed_platforms: tuple[str, ...]
    allowed_treasury_providers: tuple[str, ...]


@dataclass
class TelegramConfig:
    token: str | None
    chat_id: str | None
    poll_timeout_sec: int = 20
    polling_enabled: bool = True

    @property
    def enabled(self) -> bool:
        return bool(self.token)


@dataclass
class Settings:
    base_dir: Path
    db_path: Path
    log_path: Path
    scan_interval_sec: int
    scanner_fetch_timeout_sec: int
    price_update_interval_sec: int
    order_poll_interval_sec: int
    strategy_interval_sec: int
    forex_update_interval_min: int
    market_discovery_cache_ttl_min: int
    raw_orders_retention_hours: int
    raw_orders_prune_interval_sec: int
    market_activity_windows_hours: tuple[int, ...]
    liquidity_history_cache_ttl_sec: int
    enable_internal_graph: bool
    enable_cross_currency: bool
    internal_graph_interval_sec: int
    internal_quote_cache_ttl_sec: int
    internal_graph_quote_concurrency: int
    min_internal_spread_pct: Decimal
    min_cross_platform_spread_pct: Decimal
    min_spread_alert_pct: Decimal
    min_alert_profit_usd: Decimal
    min_alert_volume_usdt: Decimal
    max_alerts_per_scan: int
    alert_allow_tradable_liquidity: bool
    alert_route_cooldown_sec: int
    alert_route_min_profit_improvement_pct: Decimal
    allow_external_settlement_signals: bool
    cross_platform_fee_buffer_pct: Decimal
    cross_currency_fee_buffer_pct: Decimal
    opportunity_ttl_sec: int
    route_min_amount_buffer_pct: Decimal
    route_max_amount_headroom_pct: Decimal
    route_available_headroom_pct: Decimal
    candidate_book_scan_depth: int
    candidate_book_rank_start: int
    candidate_book_rank_end: int
    candidate_min_rating: Decimal
    candidate_min_completed_orders: int
    candidate_max_last_active_minutes: int
    candidate_min_available_base: Decimal
    candidate_median_deviation_pct: Decimal
    candidate_phantom_deviation_pct: Decimal
    max_candidates_per_side: int
    strict_single_merchant_mode: bool
    price_improvement_step_pct: Decimal
    enable_order_execution: bool
    auto_manage_buy_side: bool
    auto_manage_sell_side: bool
    log_level: str
    base_asset: str
    pairs: tuple[tuple[str, str], ...]
    fiats: tuple[str, ...]
    market_discovery_fiats: tuple[str, ...]
    enabled_platforms: tuple[str, ...]
    fx_conversion_rail: str
    fx_candidate_rails: tuple[str, ...]
    revolut_plan: str
    revolut_assume_allowance_used: bool
    revolut_standard_fair_usage_pct: Decimal
    revolut_standard_weekend_pct: Decimal
    wise_estimated_fee_safety_pct: Decimal
    inter_exchange_transfer_fee_usdt: Decimal
    inter_exchange_transfer_risk_pct: Decimal
    target_payments: tuple[str, ...]
    blocked_payment_keywords: tuple[str, ...]
    blocked_order_terms_keywords: tuple[str, ...]
    blocked_revolut_terms_keywords: tuple[str, ...]
    blocked_wise_terms_keywords: tuple[str, ...]
    local_bank_fiats: tuple[str, ...]
    bank_fx_fiats: tuple[str, ...]
    revolut_wallet_fiats: tuple[str, ...]
    revolut_receive_bank_fiats: tuple[str, ...]
    revolut_receive_fiats: tuple[str, ...]
    revolut_bank_transfer_fiats: tuple[str, ...]
    revolut_card_manual_fiats: tuple[str, ...]
    revolut_card_manual_send_pct_by_fiat: dict[str, Decimal]
    wise_wallet_fiats: tuple[str, ...]
    wise_receive_bank_fiats: tuple[str, ...]
    wise_bank_transfer_fiats: tuple[str, ...]
    wise_bank_transfer_slow_fiats: tuple[str, ...]
    wise_bank_transfer_send_pct_by_fiat: dict[str, Decimal]
    prefunded_mode: bool
    prefunded_exchange_usdt: Decimal
    default_pair_order_config: PairOrderConfig
    order_configs: dict[str, PairOrderConfig]
    rail_fee_profiles: dict[str, RailFeeProfile]
    min_usdt_balance_reserve: Decimal
    usd_p2p_min_price: Decimal
    usd_p2p_max_price: Decimal
    p2p_price_min_by_fiat: dict[str, Decimal]
    p2p_price_max_by_fiat: dict[str, Decimal]
    p2p_price_max_deviation_pct: Decimal
    p2p_price_max_deviation_pct_by_fiat: dict[str, Decimal]
    bybit_p2p_price_max_deviation_pct: Decimal
    bybit_p2p_price_max_deviation_pct_by_fiat: dict[str, Decimal]
    counterparty_filter: CounterpartyFilterConfig
    daily_limits: DailyLimitsConfig
    bybit: BybitConfig
    binance: BinanceConfig
    bingx: BingXConfig
    binance_execution: BinanceExecutionConfig
    revolut_business: RevolutBusinessConfig
    exchange_execution: ExchangeExecutionConfig
    telegram: TelegramConfig

    @classmethod
    def from_env(cls, base_dir: Path | None = None) -> "Settings":
        resolved_base = Path(base_dir or Path.cwd()).resolve()
        _load_dotenv(resolved_base / ".env")

        base_asset = (_env("P2P_ASSET", "USDT") or "USDT").upper()
        fiats = tuple(item.upper() for item in _list_env("P2P_FIATS", ("PLN", "USD", "EUR", "RON")))
        enabled_platforms = tuple(
            platform.lower()
            for platform in _list_env("P2P_PLATFORMS", ("BYBIT",))
            if platform.lower() in {"bybit", "binance", "bingx"}
        ) or ("bybit",)
        default_pair_order_config = PairOrderConfig(
            pair=f"DEFAULT_{base_asset}",
            min_spread_pct=_decimal_env("DEFAULT_MIN_SPREAD_PCT", "1.5"),
            target_spread_pct=_decimal_env("DEFAULT_TARGET_SPREAD_PCT", "2.0"),
            alert_min_spread_pct=_decimal_env("MIN_SPREAD_ALERT_PCT", "2.0"),
            alert_min_profit_usd=_decimal_env("MIN_ALERT_PROFIT_USD", "30"),
            max_single_order_usd=_decimal_env("DEFAULT_MAX_SINGLE_ORDER_USD", "1500"),
            min_order_fiat=_decimal_env("DEFAULT_MIN_ORDER_FIAT", "400"),
            preferred_payments=_list_env("DEFAULT_PREFERRED_PAYMENTS", DEFAULT_TARGET_PAYMENTS),
        )
        order_configs: dict[str, PairOrderConfig] = {}
        for fiat in fiats:
            min_spread = _decimal_env(f"MIN_SPREAD_{fiat}", str(default_pair_order_config.min_spread_pct))
            target_spread = _decimal_env(
                f"TARGET_SPREAD_{fiat}",
                str(default_pair_order_config.target_spread_pct),
            )
            alert_min_spread = _decimal_env(
                f"MIN_ALERT_SPREAD_{fiat}",
                str(default_pair_order_config.alert_min_spread_pct),
            )
            alert_min_profit = _decimal_env(
                f"MIN_ALERT_PROFIT_USD_{fiat}",
                str(default_pair_order_config.alert_min_profit_usd),
            )
            max_single = _decimal_env(
                f"MAX_SINGLE_ORDER_USD_{fiat}",
                str(default_pair_order_config.max_single_order_usd),
            )
            min_order_fiat = _decimal_env(
                f"MIN_ORDER_FIAT_{fiat}",
                str(default_pair_order_config.min_order_fiat),
            )
            preferred_payments = _list_env(f"TARGET_PAYMENTS_{fiat}", default_pair_order_config.preferred_payments)
            order_configs[f"{fiat}_{base_asset}"] = PairOrderConfig(
                pair=f"{fiat}_{base_asset}",
                min_spread_pct=min_spread,
                target_spread_pct=target_spread,
                alert_min_spread_pct=alert_min_spread,
                alert_min_profit_usd=alert_min_profit,
                max_single_order_usd=max_single,
                min_order_fiat=min_order_fiat,
                preferred_payments=preferred_payments,
            )

        rail_fee_defaults: dict[str, RailFeeProfile] = {
            "revolut_balance": RailFeeProfile(
                send_pct=Decimal("0"),
                send_fixed_usd=Decimal("0"),
                receive_pct=Decimal("0"),
                receive_fixed_usd=Decimal("0"),
                fx_pct=Decimal("0"),
                fx_fixed_usd=Decimal("0"),
            ),
            "revolut_bank_transfer": RailFeeProfile(
                send_pct=Decimal("0"),
                send_fixed_usd=Decimal("0"),
                receive_pct=Decimal("0"),
                receive_fixed_usd=Decimal("0"),
                fx_pct=Decimal("0"),
                fx_fixed_usd=Decimal("0"),
            ),
            "revolut_card": RailFeeProfile(
                send_pct=Decimal("0"),
                send_fixed_usd=Decimal("0"),
                receive_pct=Decimal("0"),
                receive_fixed_usd=Decimal("0"),
                fx_pct=Decimal("0"),
                fx_fixed_usd=Decimal("0"),
            ),
            "revolut_card_manual": RailFeeProfile(
                send_pct=Decimal("1.00"),
                send_fixed_usd=Decimal("0"),
                receive_pct=Decimal("0"),
                receive_fixed_usd=Decimal("0"),
                fx_pct=Decimal("0"),
                fx_fixed_usd=Decimal("0"),
            ),
            "wise_balance": RailFeeProfile(
                send_pct=Decimal("0"),
                send_fixed_usd=Decimal("0"),
                receive_pct=Decimal("0"),
                receive_fixed_usd=Decimal("0"),
                fx_pct=Decimal("0.50"),
                fx_fixed_usd=Decimal("0"),
            ),
            "wise_bank_transfer": RailFeeProfile(
                send_pct=Decimal("0.35"),
                send_fixed_usd=Decimal("0"),
                send_fixed_amount=Decimal("1.48"),
                send_fixed_ccy="EUR",
                receive_pct=Decimal("0"),
                receive_fixed_usd=Decimal("0"),
                fx_pct=Decimal("0.50"),
                fx_fixed_usd=Decimal("0"),
            ),
            "wise_card": RailFeeProfile(
                send_pct=Decimal("0"),
                send_fixed_usd=Decimal("0"),
                receive_pct=Decimal("0"),
                receive_fixed_usd=Decimal("0"),
                fx_pct=Decimal("2.00"),
                fx_fixed_usd=Decimal("0"),
            ),
            "bank_fx": RailFeeProfile(),
            "bank_transfer": RailFeeProfile(),
            "blik": RailFeeProfile(),
            "fiat_balance": RailFeeProfile(),
            "sepa": RailFeeProfile(),
            "zen": RailFeeProfile(),
        }
        rail_fee_profiles: dict[str, RailFeeProfile] = {}
        for rail, default_profile in rail_fee_defaults.items():
            prefix = rail.upper()
            rail_fee_profiles[rail] = RailFeeProfile(
                send_pct=_decimal_env(f"RAIL_FEE_{prefix}_SEND_PCT", str(default_profile.send_pct)),
                send_fixed_usd=_decimal_env(
                    f"RAIL_FEE_{prefix}_SEND_FIXED_USD",
                    str(default_profile.send_fixed_usd),
                ),
                send_fixed_amount=_decimal_env(
                    f"RAIL_FEE_{prefix}_SEND_FIXED_AMOUNT",
                    str(default_profile.send_fixed_amount),
                ),
                send_fixed_ccy=(_env(f"RAIL_FEE_{prefix}_SEND_FIXED_CCY", default_profile.send_fixed_ccy) or "").upper(),
                receive_pct=_decimal_env(
                    f"RAIL_FEE_{prefix}_RECEIVE_PCT",
                    str(default_profile.receive_pct),
                ),
                receive_fixed_usd=_decimal_env(
                    f"RAIL_FEE_{prefix}_RECEIVE_FIXED_USD",
                    str(default_profile.receive_fixed_usd),
                ),
                receive_fixed_amount=_decimal_env(
                    f"RAIL_FEE_{prefix}_RECEIVE_FIXED_AMOUNT",
                    str(default_profile.receive_fixed_amount),
                ),
                receive_fixed_ccy=(
                    _env(f"RAIL_FEE_{prefix}_RECEIVE_FIXED_CCY", default_profile.receive_fixed_ccy) or ""
                ).upper(),
                fx_pct=_decimal_env(f"RAIL_FEE_{prefix}_FX_PCT", str(default_profile.fx_pct)),
                fx_fixed_usd=_decimal_env(
                    f"RAIL_FEE_{prefix}_FX_FIXED_USD",
                    str(default_profile.fx_fixed_usd),
                ),
                fx_fixed_amount=_decimal_env(
                    f"RAIL_FEE_{prefix}_FX_FIXED_AMOUNT",
                    str(default_profile.fx_fixed_amount),
                ),
                fx_fixed_ccy=(_env(f"RAIL_FEE_{prefix}_FX_FIXED_CCY", default_profile.fx_fixed_ccy) or "").upper(),
                fx_rate_markup_pct=_decimal_env(
                    f"RAIL_FEE_{prefix}_FX_RATE_MARKUP_PCT",
                    str(default_profile.fx_rate_markup_pct),
                ),
            )

        return cls(
            base_dir=resolved_base,
            db_path=resolved_base / "data" / "p2p_bot.db",
            log_path=resolved_base / "logs" / "bot.log",
            scan_interval_sec=_int_env("SCAN_INTERVAL_SEC", 30),
            scanner_fetch_timeout_sec=_int_env("SCANNER_FETCH_TIMEOUT_SEC", 10),
            price_update_interval_sec=_int_env("PRICE_UPDATE_INTERVAL_SEC", 60),
            order_poll_interval_sec=_int_env("ORDER_POLL_INTERVAL_SEC", 30),
            strategy_interval_sec=_int_env("STRATEGY_INTERVAL_SEC", 45),
            forex_update_interval_min=_int_env("FOREX_UPDATE_INTERVAL_MIN", 5),
            market_discovery_cache_ttl_min=_int_env("MARKET_DISCOVERY_CACHE_TTL_MIN", 30),
            raw_orders_retention_hours=_int_env("RAW_ORDERS_RETENTION_HOURS", 6),
            raw_orders_prune_interval_sec=_int_env("RAW_ORDERS_PRUNE_INTERVAL_SEC", 600),
            market_activity_windows_hours=tuple(
                int(item)
                for item in _list_env("MARKET_ACTIVITY_WINDOWS_HOURS", ("1", "3"))
                if str(item).strip()
            ),
            liquidity_history_cache_ttl_sec=_int_env("LIQUIDITY_HISTORY_CACHE_TTL_SEC", 300),
            enable_internal_graph=_bool_env("ENABLE_INTERNAL_GRAPH", False),
            enable_cross_currency=_bool_env("ENABLE_CROSS_CURRENCY", False),
            internal_graph_interval_sec=_int_env("INTERNAL_GRAPH_INTERVAL_SEC", 180),
            internal_quote_cache_ttl_sec=_int_env("INTERNAL_QUOTE_CACHE_TTL_SEC", 15),
            internal_graph_quote_concurrency=_int_env("INTERNAL_GRAPH_QUOTE_CONCURRENCY", 12),
            min_internal_spread_pct=_decimal_env("MIN_INTERNAL_SPREAD_PCT", "1.5"),
            min_cross_platform_spread_pct=_decimal_env("MIN_CROSS_PLATFORM_SPREAD_PCT", "2.0"),
            min_spread_alert_pct=_decimal_env("MIN_SPREAD_ALERT_PCT", "2.0"),
            min_alert_profit_usd=_decimal_env("MIN_ALERT_PROFIT_USD", "30"),
            min_alert_volume_usdt=_decimal_env("MIN_ALERT_VOLUME_USDT", "400"),
            max_alerts_per_scan=_int_env("MAX_ALERTS_PER_SCAN", 3),
            alert_allow_tradable_liquidity=_bool_env("ALERT_ALLOW_TRADABLE_LIQUIDITY", True),
            alert_route_cooldown_sec=_int_env("ALERT_ROUTE_COOLDOWN_SEC", 900),
            alert_route_min_profit_improvement_pct=_decimal_env(
                "ALERT_ROUTE_MIN_PROFIT_IMPROVEMENT_PCT",
                "20",
            ),
            allow_external_settlement_signals=_bool_env(
                "ALLOW_EXTERNAL_SETTLEMENT_SIGNALS",
                False,
            ),
            cross_platform_fee_buffer_pct=_decimal_env("CROSS_PLATFORM_FEE_BUFFER_PCT", "0.2"),
            cross_currency_fee_buffer_pct=_decimal_env("CROSS_CURRENCY_FEE_BUFFER_PCT", "0.2"),
            opportunity_ttl_sec=_int_env("OPPORTUNITY_TTL_SEC", 120),
            route_min_amount_buffer_pct=_decimal_env("ROUTE_MIN_AMOUNT_BUFFER_PCT", "5"),
            route_max_amount_headroom_pct=_decimal_env("ROUTE_MAX_AMOUNT_HEADROOM_PCT", "10"),
            route_available_headroom_pct=_decimal_env("ROUTE_AVAILABLE_HEADROOM_PCT", "15"),
            candidate_book_scan_depth=_int_env("CANDIDATE_BOOK_SCAN_DEPTH", 20),
            candidate_book_rank_start=_int_env("CANDIDATE_BOOK_RANK_START", 2),
            candidate_book_rank_end=_int_env("CANDIDATE_BOOK_RANK_END", 15),
            candidate_min_rating=_decimal_env("CANDIDATE_MIN_RATING", "90"),
            candidate_min_completed_orders=_int_env("CANDIDATE_MIN_COMPLETED_ORDERS", 50),
            candidate_max_last_active_minutes=_int_env("CANDIDATE_MAX_LAST_ACTIVE_MINUTES", 15),
            candidate_min_available_base=_decimal_env("CANDIDATE_MIN_AVAILABLE_BASE", "500"),
            candidate_median_deviation_pct=_decimal_env("CANDIDATE_MEDIAN_DEVIATION_PCT", "5"),
            candidate_phantom_deviation_pct=_decimal_env("CANDIDATE_PHANTOM_DEVIATION_PCT", "3"),
            max_candidates_per_side=_int_env("MAX_CANDIDATES_PER_SIDE", 5),
            strict_single_merchant_mode=_bool_env("STRICT_SINGLE_MERCHANT_MODE", True),
            price_improvement_step_pct=_decimal_env("PRICE_IMPROVEMENT_STEP_PCT", "0.05"),
            enable_order_execution=_bool_env("ENABLE_ORDER_EXECUTION", False),
            auto_manage_buy_side=_bool_env("AUTO_MANAGE_BUY_SIDE", True),
            auto_manage_sell_side=_bool_env("AUTO_MANAGE_SELL_SIDE", True),
            log_level=_env("LOG_LEVEL", "INFO") or "INFO",
            base_asset=base_asset,
            pairs=tuple((base_asset, fiat) for fiat in fiats),
            fiats=fiats,
            market_discovery_fiats=tuple(
                item.upper()
                for item in _list_env(
                    "MARKET_DISCOVERY_FIATS",
                    ("PLN", "EUR", "USD", "RON", "GBP", "CZK", "HUF", "SEK", "DKK", "NOK", "IDR", "VND"),
                )
            ),
            enabled_platforms=enabled_platforms,
            fx_conversion_rail=cls._normalize_rail_name(
                (_env("FX_CONVERSION_RAIL", "REVOLUT_BALANCE") or "REVOLUT_BALANCE").strip().lower()
            ),
            fx_candidate_rails=tuple(
                cls._normalize_rail_name(item.strip().lower())
                for item in _list_env(
                    "FX_CANDIDATE_RAILS",
                    (
                        "REVOLUT_BALANCE",
                        "WISE_BALANCE",
                        "BANK_FX",
                        "WISE_BANK_TRANSFER",
                        "WISE_CARD",
                        "REVOLUT_BANK_TRANSFER",
                        "REVOLUT_CARD",
                    ),
                )
                if item.strip()
            ),
            revolut_plan=(_env("REVOLUT_PLAN", "STANDARD") or "STANDARD").strip().lower(),
            revolut_assume_allowance_used=_bool_env("REVOLUT_ASSUME_ALLOWANCE_USED", True),
            revolut_standard_fair_usage_pct=_decimal_env("REVOLUT_STANDARD_FAIR_USAGE_PCT", "1.0"),
            revolut_standard_weekend_pct=_decimal_env("REVOLUT_STANDARD_WEEKEND_PCT", "1.0"),
            wise_estimated_fee_safety_pct=_decimal_env("WISE_ESTIMATED_FEE_SAFETY_PCT", "0.60"),
            inter_exchange_transfer_fee_usdt=_decimal_env("INTER_EXCHANGE_TRANSFER_FEE_USDT", "1.0"),
            inter_exchange_transfer_risk_pct=_decimal_env("INTER_EXCHANGE_TRANSFER_RISK_PCT", "0.50"),
            target_payments=_list_env("TARGET_PAYMENTS", DEFAULT_TARGET_PAYMENTS),
            blocked_payment_keywords=_list_env(
                "BLOCKED_PAYMENT_KEYWORDS",
                DEFAULT_BLOCKED_PAYMENT_KEYWORDS,
            ),
            blocked_order_terms_keywords=_list_env(
                "BLOCKED_ORDER_TERMS_KEYWORDS",
                DEFAULT_BLOCKED_ORDER_TERMS_KEYWORDS,
            ),
            blocked_revolut_terms_keywords=_list_env(
                "BLOCKED_REVOLUT_TERMS_KEYWORDS",
                DEFAULT_BLOCK_REVOLUT_TERMS_KEYWORDS,
            ),
            blocked_wise_terms_keywords=_list_env(
                "BLOCKED_WISE_TERMS_KEYWORDS",
                DEFAULT_BLOCK_WISE_TERMS_KEYWORDS,
            ),
            local_bank_fiats=tuple(
                item.upper()
                for item in _list_env(
                    "LOCAL_BANK_FIATS",
                    ("PLN",),
                )
            ),
            bank_fx_fiats=tuple(
                item.upper()
                for item in _list_env(
                    "BANK_FX_FIATS",
                    ("PLN", "EUR", "USD"),
                )
            ),
            revolut_wallet_fiats=tuple(
                item.upper()
                for item in _list_env(
                    "REVOLUT_WALLET_FIATS",
                    (
                        "PLN",
                        "EUR",
                        "RON",
                        "GBP",
                        "CZK",
                        "HUF",
                        "SEK",
                        "NOK",
                        "ILS",
                        "USD",
                        "AED",
                        "CHF",
                        "DKK",
                        "SGD",
                        "AUD",
                        "CAD",
                        "JPY",
                        "THB",
                        "TRY",
                        "ZAR",
                        "IDR",
                        "VND",
                        "HKD",
                        "QAR",
                        "SAR",
                    ),
                )
            ),
            revolut_receive_bank_fiats=tuple(
                item.upper()
                for item in _list_env(
                    "REVOLUT_RECEIVE_BANK_FIATS",
                    ("EUR", "GBP", "PLN"),
                )
            ),
            revolut_receive_fiats=tuple(
                item.upper()
                for item in _list_env(
                    "REVOLUT_RECEIVE_FIATS",
                    (
                        "AED",
                        "AUD",
                        "CAD",
                        "CHF",
                        "CZK",
                        "DKK",
                        "EUR",
                        "GBP",
                        "HKD",
                        "HUF",
                        "ILS",
                        "JPY",
                        "NOK",
                        "PLN",
                        "QAR",
                        "RON",
                        "SAR",
                        "SEK",
                        "SGD",
                        "THB",
                        "TRY",
                        "USD",
                        "ZAR",
                    ),
                )
            ),
            revolut_bank_transfer_fiats=tuple(
                item.upper()
                for item in _list_env(
                    "REVOLUT_BANK_TRANSFER_FIATS",
                    (
                        "EUR",
                        "PLN",
                        "GBP",
                    ),
                )
            ),
            revolut_card_manual_fiats=tuple(
                item.upper()
                for item in _list_env(
                    "REVOLUT_CARD_MANUAL_FIATS",
                    ("RON", "CZK", "SEK", "NOK", "DKK", "SGD"),
                )
            ),
            revolut_card_manual_send_pct_by_fiat=_decimal_dict_env(
                "REVOLUT_CARD_MANUAL_SEND_PCT_BY_FIAT",
                {
                    "RON": Decimal("0.70"),
                    "CZK": Decimal("0.70"),
                    "SEK": Decimal("0.75"),
                    "NOK": Decimal("0.70"),
                    "DKK": Decimal("0.70"),
                    "SGD": Decimal("2.30"),
                },
            ),
            wise_wallet_fiats=tuple(
                item.upper()
                for item in _list_env(
                    "WISE_WALLET_FIATS",
                    (
                        "EUR",
                        "GBP",
                        "USD",
                        "AUD",
                        "NZD",
                        "HUF",
                        "SGD",
                        "PHP",
                        "CAD",
                        "PLN",
                        "RON",
                        "CZK",
                        "SEK",
                        "NOK",
                        "DKK",
                        "AED",
                        "CHF",
                        "ILS",
                        "ZAR",
                        "JPY",
                    ),
                )
            ),
            wise_receive_bank_fiats=tuple(
                item.upper()
                for item in _list_env(
                    "WISE_RECEIVE_BANK_FIATS",
                    ("EUR", "GBP", "USD", "NZD", "HUF", "SGD", "PHP", "CAD", "AUD"),
                )
            ),
            wise_bank_transfer_fiats=tuple(
                item.upper()
                for item in _list_env(
                    "WISE_BANK_TRANSFER_FIATS",
                    (
                        "EUR",
                        "GBP",
                        "AUD",
                        "HUF",
                        "SGD",
                        "PHP",
                        "DKK",
                        "HKD",
                        "INR",
                        "CNY",
                    ),
                )
            ),
            wise_bank_transfer_slow_fiats=tuple(
                item.upper()
                for item in _list_env(
                    "WISE_BANK_TRANSFER_SLOW_FIATS",
                    (),
                )
            ),
            wise_bank_transfer_send_pct_by_fiat=_decimal_dict_env(
                "WISE_BANK_TRANSFER_SEND_PCT_BY_FIAT",
                {},
            ),
            prefunded_mode=_bool_env("PREFUNDED_MODE", True),
            prefunded_exchange_usdt=_decimal_env("PREFUNDED_EXCHANGE_USDT", "1500"),
            default_pair_order_config=default_pair_order_config,
            order_configs=order_configs,
            rail_fee_profiles=rail_fee_profiles,
            min_usdt_balance_reserve=_decimal_env("MIN_USDT_BALANCE_RESERVE", "200"),
            usd_p2p_min_price=_decimal_env("USD_P2P_MIN_PRICE", "0.995"),
            usd_p2p_max_price=_decimal_env("USD_P2P_MAX_PRICE", "1.005"),
            p2p_price_min_by_fiat=_decimal_dict_env("P2P_PRICE_MIN_BY_FIAT", {}),
            p2p_price_max_by_fiat=_decimal_dict_env("P2P_PRICE_MAX_BY_FIAT", {}),
            p2p_price_max_deviation_pct=_decimal_env("P2P_PRICE_MAX_DEVIATION_PCT", "40"),
            p2p_price_max_deviation_pct_by_fiat=_decimal_dict_env(
                "P2P_PRICE_MAX_DEVIATION_PCT_BY_FIAT",
                {},
            ),
            bybit_p2p_price_max_deviation_pct=_decimal_env(
                "BYBIT_P2P_PRICE_MAX_DEVIATION_PCT",
                "10",
            ),
            bybit_p2p_price_max_deviation_pct_by_fiat=_decimal_dict_env(
                "BYBIT_P2P_PRICE_MAX_DEVIATION_PCT_BY_FIAT",
                {},
            ),
            counterparty_filter=CounterpartyFilterConfig(
                min_rating=_decimal_env("MIN_COUNTERPARTY_RATING", "93"),
                min_completed_orders=_int_env("MIN_COUNTERPARTY_COMPLETED_ORDERS", 50),
                min_account_days=_int_env("MIN_COUNTERPARTY_ACCOUNT_DAYS", 30),
                max_single_trade_usd=_decimal_env("MAX_SINGLE_TRADE_USD", "3000"),
                block_new_accounts=_bool_env("BLOCK_NEW_ACCOUNTS", True),
                require_online=_bool_env("REQUIRE_COUNTERPARTY_ONLINE", True),
                require_kyc=_bool_env("REQUIRE_COUNTERPARTY_KYC", True),
                max_last_active_minutes=_int_env("MAX_COUNTERPARTY_LAST_ACTIVE_MINUTES", 30),
                allow_unknown_profile_fields=_bool_env("ALLOW_UNKNOWN_PROFILE_FIELDS", True),
                allow_unknown_payment_methods=_bool_env("ALLOW_UNKNOWN_PAYMENT_METHODS", True),
            ),
            daily_limits=DailyLimitsConfig(
                max_volume_usd=_decimal_env("MAX_DAILY_VOLUME_USD", "30000"),
                max_trades=_int_env("MAX_DAILY_TRADES", 20),
                max_loss_usd=_decimal_env("MAX_DAILY_LOSS_USD", "100"),
                max_open_usd=_decimal_env("MAX_OPEN_USD", "4500"),
            ),
            bybit=BybitConfig(
                api_key=_env("BYBIT_API_KEY"),
                api_secret=_env("BYBIT_API_SECRET"),
                base_url=_env("BYBIT_API_BASE", "https://api.bybit.com") or "https://api.bybit.com",
                legacy_public_scan_url=_env(
                    "BYBIT_LEGACY_P2P_SCAN_URL",
                    "https://api2.bybit.com/fiat/otc/item/online",
                )
                or "https://api2.bybit.com/fiat/otc/item/online",
                cookies=_env("BYBIT_P2P_COOKIES"),
                recv_window_ms=_int_env("BYBIT_RECV_WINDOW_MS", 15000),
                use_official_p2p_api=_bool_env("BYBIT_USE_OFFICIAL_P2P_API", True),
                use_legacy_public_scan=_bool_env("BYBIT_USE_LEGACY_PUBLIC_SCAN", False),
                balance_account_type=(_env("BYBIT_BALANCE_ACCOUNT_TYPE", "FUND") or "FUND").upper(),
                payment_code_map=_dict_env(
                    "BYBIT_PAYMENT_CODE_MAP",
                    {
                        "416": "Balance",
                        "78": "Wise",
                        "65": "Revolut",
                        "118": "SEPA",
                        "14": "Bank Transfer",
                        "159": "PKO Bank",
                    },
                ),
            ),
            binance=BinanceConfig(
                api_key=_env("BINANCE_API_KEY"),
                api_secret=_env("BINANCE_API_SECRET"),
                base_url=_env("BINANCE_P2P_BASE", "https://p2p.binance.com")
                or "https://p2p.binance.com",
                adv_search_path=_env(
                    "BINANCE_P2P_SEARCH_PATH",
                    "/bapi/c2c/v2/friendly/c2c/adv/search",
                )
                or "/bapi/c2c/v2/friendly/c2c/adv/search",
                rows=_int_env("BINANCE_ROWS", 20),
                rate_limit_max_calls=_int_env("BINANCE_RATE_LIMIT_MAX_CALLS", 1),
                rate_limit_period_sec=float(_env("BINANCE_RATE_LIMIT_PERIOD_SEC", "3.2") or "3.2"),
            ),
            bingx=BingXConfig(
                api_key=_env("BINGX_API_KEY"),
                api_secret=_env("BINGX_API_SECRET"),
                base_url=_env("BINGX_API_BASE", "https://open-api.bingx.com")
                or "https://open-api.bingx.com",
                p2p_api_base=_env("BINGX_P2P_API_BASE", "https://api-base.bingx.com/api")
                or "https://api-base.bingx.com/api",
                fiat_site_url=_env("BINGX_FIAT_SITE_URL", "https://paycat.com")
                or "https://paycat.com",
                supported_payment_methods=tuple(
                    item
                    for item in _list_env(
                        "BINGX_P2P_PAYMENT_METHODS",
                        ("Revolut", "Wise", "SEPA", "BLIK", "PKO Bank"),
                    )
                    if item
                ),
                supported_fiats=tuple(
                    fiat.upper()
                    for fiat in _list_env(
                        "BINGX_P2P_SUPPORTED_FIATS",
                        (
                            "PLN",
                            "EUR",
                            "RON",
                            "GBP",
                            "HUF",
                            "ILS",
                            "USD",
                            "AED",
                            "DKK",
                            "AUD",
                            "JPY",
                            "THB",
                            "TRY",
                            "ZAR",
                            "IDR",
                            "VND",
                            "QAR",
                            "SAR",
                        ),
                    )
                ),
                rows=_int_env("BINGX_P2P_ROWS", 20),
                rate_limit_max_calls=_int_env("BINGX_P2P_RATE_LIMIT_MAX_CALLS", 4),
                rate_limit_period_sec=float(_env("BINGX_P2P_RATE_LIMIT_PERIOD_SEC", "1.0") or "1.0"),
                p2p_app_version=_env("BINGX_P2P_APP_VERSION", "4.80.0") or "4.80.0",
                p2p_platform_id=_int_env("BINGX_P2P_PLATFORM_ID", 30),
                p2p_main_app_id=_int_env("BINGX_P2P_MAIN_APP_ID", 10009),
            ),
            binance_execution=BinanceExecutionConfig(
                api_key=_env("BINANCE_EXECUTION_API_KEY"),
                api_secret=_env("BINANCE_EXECUTION_API_SECRET"),
                base_url=_env("BINANCE_EXECUTION_API_BASE", "https://api.binance.com")
                or "https://api.binance.com",
                reserve_timeout_sec=_int_env("BINANCE_EXECUTION_RESERVE_TIMEOUT_SEC", 900),
                mode=(_env("BINANCE_EXECUTION_MODE", "p2p") or "p2p").strip().lower(),
            ),
            revolut_business=RevolutBusinessConfig(
                client_id=_env("REVOLUT_BUSINESS_CLIENT_ID"),
                client_secret=_env("REVOLUT_BUSINESS_CLIENT_SECRET"),
                base_url=_env("REVOLUT_BUSINESS_API_BASE", "https://b2b.revolut.com/api/1.0")
                or "https://b2b.revolut.com/api/1.0",
                webhook_secret=_env("REVOLUT_BUSINESS_WEBHOOK_SECRET"),
                enabled=_bool_env("ENABLE_REVOLUT_BUSINESS_API", False),
            ),
            exchange_execution=ExchangeExecutionConfig(
                enabled=_bool_env("ENABLE_EXCHANGE_EXECUTION", False),
                dry_run=_bool_env("EXCHANGE_EXECUTION_DRY_RUN", True),
                reserve_both_legs=_bool_env("EXCHANGE_EXECUTION_RESERVE_BOTH_LEGS", True),
                require_manual_confirm=_bool_env("EXCHANGE_EXECUTION_REQUIRE_MANUAL_CONFIRM", True),
                session_ttl_sec=_int_env("EXCHANGE_EXECUTION_SESSION_TTL_SEC", 1800),
                max_capital_per_signal_pct=_decimal_env("EXECUTION_MAX_CAPITAL_PER_SIGNAL_PCT", "30"),
                rebalance_tolerance_pct=_decimal_env("EXECUTION_REBALANCE_TOLERANCE_PCT", "20"),
                revalidation_max_price_drift_pct=_decimal_env("EXECUTION_REVALIDATION_MAX_PRICE_DRIFT_PCT", "0.50"),
                allowed_platforms=tuple(
                    item.lower()
                    for item in _list_env("EXECUTION_ALLOWED_PLATFORMS", ("BINANCE", "BYBIT"))
                ),
                allowed_treasury_providers=tuple(
                    item.lower()
                    for item in _list_env("EXECUTION_ALLOWED_TREASURY_PROVIDERS", ("REVOLUT",))
                ),
            ),
            telegram=TelegramConfig(
                token=_env("TELEGRAM_BOT_TOKEN"),
                chat_id=_env("TELEGRAM_CHAT_ID"),
                poll_timeout_sec=_int_env("TELEGRAM_POLL_TIMEOUT_SEC", 20),
                polling_enabled=_bool_env("TELEGRAM_ENABLE_POLLING", True),
            ),
        )

    def order_config_for(self, fiat: str, asset: str = "USDT") -> PairOrderConfig:
        key = f"{fiat.upper()}_{asset.upper()}"
        if key in self.order_configs:
            return self.order_configs[key]
        return PairOrderConfig(
            pair=key,
            min_spread_pct=self.default_pair_order_config.min_spread_pct,
            target_spread_pct=self.default_pair_order_config.target_spread_pct,
            alert_min_spread_pct=self.default_pair_order_config.alert_min_spread_pct,
            alert_min_profit_usd=self.default_pair_order_config.alert_min_profit_usd,
            max_single_order_usd=self.default_pair_order_config.max_single_order_usd,
            min_order_fiat=self.default_pair_order_config.min_order_fiat,
            preferred_payments=self.default_pair_order_config.preferred_payments,
        )

    def alert_thresholds_for(self, fiat: str, asset: str = "USDT") -> tuple[Decimal, Decimal]:
        config = self.order_config_for(fiat, asset)
        return config.alert_min_spread_pct, config.alert_min_profit_usd

    def rail_fee_profile(self, rail: str | None) -> RailFeeProfile:
        if not rail:
            return RailFeeProfile()
        normalized = self._normalize_rail_name(rail.strip().lower())
        return self.rail_fee_profiles.get(normalized, RailFeeProfile())

    @staticmethod
    def _normalize_rail_name(rail: str) -> str:
        aliases = {
            "revolut": "revolut_balance",
            "wise": "wise_balance",
        }
        return aliases.get(rail, rail)

    def set_min_spread(self, fiat: str, spread_pct: Decimal, asset: str = "USDT") -> None:
        key = f"{fiat.upper()}_{asset.upper()}"
        config = self.order_configs.get(key) or self.order_config_for(fiat, asset)
        self.order_configs[key] = config
        config.min_spread_pct = spread_pct

    def set_alert_spread(
        self,
        spread_pct: Decimal,
        fiat: str | None = None,
        asset: str = "USDT",
    ) -> None:
        if fiat is None:
            self.min_spread_alert_pct = spread_pct
            self.default_pair_order_config.alert_min_spread_pct = spread_pct
            for config in self.order_configs.values():
                config.alert_min_spread_pct = spread_pct
            return
        key = f"{fiat.upper()}_{asset.upper()}"
        config = self.order_configs.get(key) or self.order_config_for(fiat, asset)
        self.order_configs[key] = config
        config.alert_min_spread_pct = spread_pct

    def set_alert_profit(
        self,
        profit_usd: Decimal,
        fiat: str | None = None,
        asset: str = "USDT",
    ) -> None:
        if fiat is None:
            self.min_alert_profit_usd = profit_usd
            self.default_pair_order_config.alert_min_profit_usd = profit_usd
            for config in self.order_configs.values():
                config.alert_min_profit_usd = profit_usd
            return
        key = f"{fiat.upper()}_{asset.upper()}"
        config = self.order_configs.get(key) or self.order_config_for(fiat, asset)
        self.order_configs[key] = config
        config.alert_min_profit_usd = profit_usd

    def platform_enabled(self, platform: str) -> bool:
        return platform.lower() in self.enabled_platforms

    def p2p_price_bounds_for_fiat(self, fiat: str) -> tuple[Decimal | None, Decimal | None]:
        fiat_upper = fiat.upper()
        min_price = self.p2p_price_min_by_fiat.get(fiat_upper)
        max_price = self.p2p_price_max_by_fiat.get(fiat_upper)
        if fiat_upper == "USD":
            min_price = min_price if min_price is not None else self.usd_p2p_min_price
            max_price = max_price if max_price is not None else self.usd_p2p_max_price
        return min_price, max_price

    def p2p_price_max_deviation_pct_for(
        self,
        fiat: str,
        *,
        platform: str | None = None,
    ) -> Decimal:
        fiat_upper = fiat.upper()
        platform_lower = str(platform or "").strip().lower()
        if platform_lower == "bybit":
            bybit_override = self.bybit_p2p_price_max_deviation_pct_by_fiat.get(fiat_upper)
            if bybit_override is not None:
                return bybit_override
        global_override = self.p2p_price_max_deviation_pct_by_fiat.get(fiat_upper)
        if global_override is not None:
            return global_override
        if platform_lower == "bybit":
            return self.bybit_p2p_price_max_deviation_pct
        return self.p2p_price_max_deviation_pct

    def serialize_public(self) -> str:
        lines = [
            f"scan_interval_sec={self.scan_interval_sec}",
            f"scanner_fetch_timeout_sec={self.scanner_fetch_timeout_sec}",
            f"price_update_interval_sec={self.price_update_interval_sec}",
            f"order_poll_interval_sec={self.order_poll_interval_sec}",
            f"strategy_interval_sec={self.strategy_interval_sec}",
            f"forex_update_interval_min={self.forex_update_interval_min}",
            f"market_discovery_cache_ttl_min={self.market_discovery_cache_ttl_min}",
            f"raw_orders_retention_hours={self.raw_orders_retention_hours}",
            f"raw_orders_prune_interval_sec={self.raw_orders_prune_interval_sec}",
            f"market_activity_windows_hours={','.join(str(item) for item in self.market_activity_windows_hours)}",
            f"liquidity_history_cache_ttl_sec={self.liquidity_history_cache_ttl_sec}",
            f"enable_internal_graph={self.enable_internal_graph}",
            f"enable_cross_currency={self.enable_cross_currency}",
            f"internal_graph_interval_sec={self.internal_graph_interval_sec}",
            f"min_internal_spread_pct={self.min_internal_spread_pct}",
            f"min_cross_platform_spread_pct={self.min_cross_platform_spread_pct}",
            f"min_spread_alert_pct={self.min_spread_alert_pct}",
            f"min_alert_profit_usd={self.min_alert_profit_usd}",
            f"min_alert_volume_usdt={self.min_alert_volume_usdt}",
            f"max_alerts_per_scan={self.max_alerts_per_scan}",
            f"alert_allow_tradable_liquidity={self.alert_allow_tradable_liquidity}",
            f"alert_route_cooldown_sec={self.alert_route_cooldown_sec}",
            (
                "alert_route_min_profit_improvement_pct="
                f"{self.alert_route_min_profit_improvement_pct}"
            ),
            f"route_min_amount_buffer_pct={self.route_min_amount_buffer_pct}",
            f"route_max_amount_headroom_pct={self.route_max_amount_headroom_pct}",
            f"route_available_headroom_pct={self.route_available_headroom_pct}",
            f"candidate_book_scan_depth={self.candidate_book_scan_depth}",
            f"candidate_book_rank_start={self.candidate_book_rank_start}",
            f"candidate_book_rank_end={self.candidate_book_rank_end}",
            f"candidate_min_rating={self.candidate_min_rating}",
            f"candidate_min_completed_orders={self.candidate_min_completed_orders}",
            f"candidate_max_last_active_minutes={self.candidate_max_last_active_minutes}",
            f"candidate_min_available_base={self.candidate_min_available_base}",
            f"candidate_median_deviation_pct={self.candidate_median_deviation_pct}",
            f"candidate_phantom_deviation_pct={self.candidate_phantom_deviation_pct}",
            f"allow_external_settlement_signals={self.allow_external_settlement_signals}",
            f"enable_order_execution={self.enable_order_execution}",
            f"auto_manage_buy_side={self.auto_manage_buy_side}",
            f"auto_manage_sell_side={self.auto_manage_sell_side}",
            f"enabled_platforms={','.join(self.enabled_platforms)}",
            f"fx_conversion_rail={self.fx_conversion_rail}",
            f"fx_candidate_rails={','.join(self.fx_candidate_rails)}",
            f"revolut_plan={self.revolut_plan}",
            f"revolut_assume_allowance_used={self.revolut_assume_allowance_used}",
            f"revolut_standard_fair_usage_pct={self.revolut_standard_fair_usage_pct}",
            f"revolut_standard_weekend_pct={self.revolut_standard_weekend_pct}",
            f"wise_estimated_fee_safety_pct={self.wise_estimated_fee_safety_pct}",
            f"inter_exchange_transfer_fee_usdt={self.inter_exchange_transfer_fee_usdt}",
            f"inter_exchange_transfer_risk_pct={self.inter_exchange_transfer_risk_pct}",
            f"base_asset={self.base_asset}",
            f"fiats={','.join(self.fiats)}",
            f"p2p_price_max_deviation_pct={self.p2p_price_max_deviation_pct}",
            "p2p_price_max_deviation_pct_by_fiat="
            + ",".join(
                f"{fiat}:{value}"
                for fiat, value in sorted(self.p2p_price_max_deviation_pct_by_fiat.items())
            ),
            f"bybit_p2p_price_max_deviation_pct={self.bybit_p2p_price_max_deviation_pct}",
            "bybit_p2p_price_max_deviation_pct_by_fiat="
            + ",".join(
                f"{fiat}:{value}"
                for fiat, value in sorted(self.bybit_p2p_price_max_deviation_pct_by_fiat.items())
            ),
            f"market_discovery_fiats={','.join(self.market_discovery_fiats)}",
            f"blocked_order_terms_keywords={','.join(self.blocked_order_terms_keywords)}",
            f"blocked_revolut_terms_keywords={','.join(self.blocked_revolut_terms_keywords)}",
            f"blocked_wise_terms_keywords={','.join(self.blocked_wise_terms_keywords)}",
            f"local_bank_fiats={','.join(self.local_bank_fiats)}",
            f"bank_fx_fiats={','.join(self.bank_fx_fiats)}",
            f"revolut_wallet_fiats={','.join(self.revolut_wallet_fiats)}",
            f"revolut_receive_bank_fiats={','.join(self.revolut_receive_bank_fiats)}",
            f"revolut_receive_fiats={','.join(self.revolut_receive_fiats)}",
            f"revolut_bank_transfer_fiats={','.join(self.revolut_bank_transfer_fiats)}",
            "revolut_card_manual_send_pct_by_fiat="
            + ",".join(
                f"{fiat}:{pct}"
                for fiat, pct in sorted(self.revolut_card_manual_send_pct_by_fiat.items())
            ),
            f"wise_wallet_fiats={','.join(self.wise_wallet_fiats)}",
            f"wise_receive_bank_fiats={','.join(self.wise_receive_bank_fiats)}",
            f"wise_bank_transfer_fiats={','.join(self.wise_bank_transfer_fiats)}",
            f"wise_bank_transfer_slow_fiats={','.join(self.wise_bank_transfer_slow_fiats)}",
            "wise_bank_transfer_send_pct_by_fiat="
            + ",".join(
                f"{fiat}:{pct.normalize()}"
                for fiat, pct in sorted(self.wise_bank_transfer_send_pct_by_fiat.items())
            ),
            f"counterparty_min_rating={self.counterparty_filter.min_rating}",
            f"counterparty_min_orders={self.counterparty_filter.min_completed_orders}",
            f"counterparty_min_days={self.counterparty_filter.min_account_days}",
            f"counterparty_require_online={self.counterparty_filter.require_online}",
            f"counterparty_require_kyc={self.counterparty_filter.require_kyc}",
            f"counterparty_max_last_active_minutes={self.counterparty_filter.max_last_active_minutes}",
            f"strict_single_merchant_mode={self.strict_single_merchant_mode}",
            f"exchange_execution_enabled={self.exchange_execution.enabled}",
            f"exchange_execution_dry_run={self.exchange_execution.dry_run}",
            f"exchange_execution_reserve_both_legs={self.exchange_execution.reserve_both_legs}",
            f"exchange_execution_require_manual_confirm={self.exchange_execution.require_manual_confirm}",
            f"exchange_execution_session_ttl_sec={self.exchange_execution.session_ttl_sec}",
            f"execution_max_capital_per_signal_pct={self.exchange_execution.max_capital_per_signal_pct}",
            f"execution_rebalance_tolerance_pct={self.exchange_execution.rebalance_tolerance_pct}",
            f"execution_revalidation_max_price_drift_pct={self.exchange_execution.revalidation_max_price_drift_pct}",
            f"execution_allowed_platforms={','.join(self.exchange_execution.allowed_platforms)}",
            f"execution_allowed_treasury_providers={','.join(self.exchange_execution.allowed_treasury_providers)}",
            f"bingx_has_credentials={self.bingx.has_credentials}",
            f"bingx_supported_payment_methods={','.join(self.bingx.supported_payment_methods)}",
            f"binance_execution_has_credentials={self.binance_execution.has_credentials}",
            f"revolut_business_enabled={self.revolut_business.enabled}",
            f"revolut_business_has_credentials={self.revolut_business.has_credentials}",
        ]
        for rail, profile in sorted(self.rail_fee_profiles.items()):
            lines.append(
                f"rail_fee[{rail}]: "
                f"send={profile.send_pct}%+{profile.send_fixed_usd}USD"
                f"+{profile.send_fixed_amount}{profile.send_fixed_ccy or ''}, "
                f"receive={profile.receive_pct}%+{profile.receive_fixed_usd}USD"
                f"+{profile.receive_fixed_amount}{profile.receive_fixed_ccy or ''}, "
                f"fx={profile.fx_pct}%+{profile.fx_fixed_usd}USD"
                f"+{profile.fx_fixed_amount}{profile.fx_fixed_ccy or ''}"
                f" rate_markup={profile.fx_rate_markup_pct}%"
            )
        for pair, config in self.order_configs.items():
            lines.append(
                f"{pair}: min={config.min_spread_pct}, target={config.target_spread_pct}, "
                f"alert_spread={config.alert_min_spread_pct}, alert_profit_usd={config.alert_min_profit_usd}, "
                f"max_single_usd={config.max_single_order_usd}, min_order_fiat={config.min_order_fiat}, "
                f"payments={','.join(config.preferred_payments)}"
            )
        return "\n".join(lines)
