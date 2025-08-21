from collections.abc import MutableMapping
from logging import Logger
from logging import LoggerAdapter
from typing import Any

LoggerLike = Logger | LoggerAdapter


class PrefixLogger(LoggerAdapter):
    """A logger that adds a prefix to all log messages."""

    def __init__(self, logger: LoggerLike, prefix: Any) -> None:
        super().__init__(logger)
        self.prefix = prefix

    def process(
        self,
        msg: str,
        kwargs: MutableMapping[str, Any],
    ) -> tuple[str, MutableMapping[str, Any]]:
        return f"{self.prefix} {msg}", kwargs
