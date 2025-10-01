"""Logging utilities for the benchmark framework."""

import logging
import sys
from pathlib import Path
from typing import Optional
from rich.logging import RichHandler
from rich.console import Console


def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    component: str = "benchmark",
    enable_rich: bool = True
) -> logging.Logger:
    """Setup logging configuration.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
        component: Component name for logger
        enable_rich: Enable rich console output

    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(f"py-omb.{component}")
    logger.setLevel(getattr(logging, level.upper()))

    # Clear existing handlers
    logger.handlers.clear()

    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler
    if enable_rich:
        console = Console()
        console_handler = RichHandler(
            console=console,
            show_time=True,
            show_path=False,
            markup=True,
            rich_tracebacks=True
        )
        console_handler.setFormatter(logging.Formatter('%(message)s'))
    else:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)

    # File handler
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Prevent propagation to root logger
    logger.propagate = False

    return logger


def get_logger(component: str) -> logging.Logger:
    """Get logger for component."""
    return logging.getLogger(f"py-omb.{component}")


class LoggerMixin:
    """Mixin class that provides logging capabilities."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger = None

    @property
    def logger(self) -> logging.Logger:
        """Get logger for this component."""
        if self._logger is None:
            component_name = self.__class__.__name__.lower()
            self._logger = get_logger(component_name)
        return self._logger