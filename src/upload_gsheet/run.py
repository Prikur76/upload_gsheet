"""Точка входа: один проход выгрузки (водители + автопарк + кураторы)."""

import logging
import sys

from upload_gsheet.jobs.drivers_and_cars import run_drivers_and_cars_safe
from upload_gsheet.jobs.supervisers import run_supervisers_safe
from upload_gsheet.logging_config import setup_logging
from upload_gsheet.sheets.client import SheetsClient

logger = logging.getLogger(__name__)


def main() -> None:
    """Один проход обновления таблиц. При сбое основной выгрузки выходит с кодом 1."""
    setup_logging(logging.INFO)
    logger.info("START")
    sheets = SheetsClient()
    if not run_drivers_and_cars_safe():
        logger.error("FAILED: водители/автопарк")
        sys.exit(1)
    run_supervisers_safe(sheets)
    logger.info("DONE")
    sys.exit(0)


if __name__ == "__main__":
    main()
