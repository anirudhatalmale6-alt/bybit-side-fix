from __future__ import annotations

import logging
import re
from pathlib import Path


_SECRET_PATTERNS = (
    re.compile(r"(https://api\.telegram\.org/bot)([^/\s]+)", re.IGNORECASE),
    re.compile(r"(bot)(\d+:[A-Za-z0-9_-]{20,})", re.IGNORECASE),
    re.compile(r"((?:api_)?key\s*[=:]\s*)([A-Za-z0-9_-]{16,})", re.IGNORECASE),
    re.compile(r"((?:api_)?secret\s*[=:]\s*)([A-Za-z0-9_-]{16,})", re.IGNORECASE),
    re.compile(r"((?:bot_)?token\s*[=:]\s*)([A-Za-z0-9:_-]{16,})", re.IGNORECASE),
)


def redact_sensitive_text(value: str) -> str:
    redacted = value
    for pattern in _SECRET_PATTERNS:
        redacted = pattern.sub(r"\1***", redacted)
    return redacted


class SensitiveDataFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = redact_sensitive_text(record.msg)
        if record.args:
            if isinstance(record.args, dict):
                record.args = {
                    key: redact_sensitive_text(value) if isinstance(value, str) else value
                    for key, value in record.args.items()
                }
            else:
                record.args = tuple(
                    redact_sensitive_text(arg) if isinstance(arg, str) else arg
                    for arg in record.args
                )
        return True


def setup_logging(log_path: Path, level: str = "INFO") -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level.upper())

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.addFilter(SensitiveDataFilter())

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.addFilter(SensitiveDataFilter())

    root.addHandler(console_handler)
    root.addHandler(file_handler)
