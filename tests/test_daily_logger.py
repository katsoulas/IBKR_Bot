from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from bot.daily_logger import DailyMarketLogger, LoggerOptions
from bot.time_utils import NY_TZ

class FakeClock:
    def __init__(self, start: datetime):
        self.t = start
    def now(self) -> datetime:
        return self.t
    def advance(self, seconds: float) -> None:
        self.t = self.t + timedelta(seconds=seconds)

def read_lines(p: Path) -> list[str]:
    return p.read_text(encoding="utf-8").splitlines()

def test_header_includes_pct_columns(tmp_path: Path):
    start = datetime(2026, 1, 6, 9, 30, 0, tzinfo=NY_TZ)
    clock = FakeClock(start)
    logger = DailyMarketLogger(
        base_dir=tmp_path,
        options=LoggerOptions(
            rotate_at_midnight=False,
            rolling_horizons_seconds=(10, 60),
            flush_each_write=True,
            include_percent_columns=True,
        ),
        time_provider=clock.now,
    )
    try:
        logger.log(100.0, 20.0)
    finally:
        logger.close()
    header = read_lines(logger.file_path)[0]
    assert "pctSPX_10s" in header
    assert "pctVIX_10s" in header
    assert "pctSPX_60s" in header
    assert "pctVIX_60s" in header
