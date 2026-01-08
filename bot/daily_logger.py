from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from typing import Callable, Optional, TextIO

from bot.time_utils import NY_TZ, fmt_ts_ms, ny_now

TimeProvider = Callable[[], datetime]

@dataclass(frozen=True)
class LoggerOptions:
    rotate_at_midnight: bool = True
    rolling_horizons_seconds: tuple[int, ...] = (10, 60, 300)
    flush_each_write: bool = True
    include_percent_columns: bool = True

class DailyMarketLogger:
    def __init__(self, base_dir: str | Path, options: LoggerOptions = LoggerOptions(), time_provider: TimeProvider = ny_now):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.options = options
        self._now = time_provider

        self._lock = threading.RLock()
        self._closed = False

        self._prev_spx: float | None = None
        self._prev_vix: float | None = None

        self._horizons = sorted({int(h) for h in options.rolling_horizons_seconds if int(h) > 0})
        self._max_horizon = max(self._horizons) if self._horizons else 0
        self._buf: deque[tuple[datetime, float, float]] = deque()

        self.current_date = self._ny_date_str()
        self.file_path = self._resolve_filename_for_date(self.current_date)
        self.file: TextIO = open(self.file_path, "a", encoding="utf-8", newline="")

        self._header = self._build_header()
        self._ensure_header()

        self._timer: Optional[threading.Timer] = None
        if self.options.rotate_at_midnight:
            self._schedule_next_midnight_rotation()

    def _ny_date_str(self) -> str:
        return self._now().strftime("%Y.%m.%d")

    def _ny_time_str(self) -> str:
        return self._now().strftime("%H.%M.%S")

    def _ts_str(self, dt: datetime) -> str:
        return fmt_ts_ms(dt)

    def _build_header(self) -> str:
        cols = ["datetime_ny", "SPX", "VIX", "dSPX", "dVIX"]
        for h in self._horizons:
            cols += [f"dSPX_{h}s", f"dVIX_{h}s"]
            if self.options.include_percent_columns:
                cols += [f"pctSPX_{h}s", f"pctVIX_{h}s"]
        return ",".join(cols) + "\n"

    def _ensure_header(self) -> None:
        if self.file.tell() == 0:
            self.file.write(self._header)
            if self.options.flush_each_write:
                self.file.flush()

    def _resolve_filename_for_date(self, date_str: str) -> Path:
        base = self.base_dir / f"Daily_Log_{date_str}.csv"
        if not base.exists():
            return base
        return self.base_dir / f"Daily_Log_{date_str}_{self._ny_time_str()}.csv"

    def _next_ny_midnight(self) -> datetime:
        now = self._now()
        tomorrow = (now + timedelta(days=1)).date()
        return datetime.combine(tomorrow, dtime(0, 0, 0), tzinfo=NY_TZ)

    def _schedule_next_midnight_rotation(self) -> None:
        with self._lock:
            if self._closed:
                return
            if self._timer is not None:
                self._timer.cancel()
            next_midnight = self._next_ny_midnight()
            seconds = (next_midnight - self._now()).total_seconds()
            if seconds < 0:
                seconds = 0.0
            self._timer = threading.Timer(seconds, self._midnight_rotate_callback)
            self._timer.daemon = True
            self._timer.start()

    def _ny_midnight_timestamp_str(self) -> str:
        now = self._now()
        midnight = datetime.combine(now.date(), dtime(0, 0, 0), tzinfo=NY_TZ)
        return self._ts_str(midnight)  # ...00:00:00.000

    def _write_marker_row(self, ts_str: str, spx: float | None, vix: float | None) -> None:
        spx_s = "" if spx is None else f"{spx}"
        vix_s = "" if vix is None else f"{vix}"
        row = [ts_str, spx_s, vix_s, "", ""]
        for _ in self._horizons:
            row += ["", ""]
            if self.options.include_percent_columns:
                row += ["", ""]
        self.file.write(",".join(row) + "\n")
        if self.options.flush_each_write:
            self.file.flush()

    def _midnight_rotate_callback(self) -> None:
        try:
            self.rotate_now(write_midnight_marker=True)
        finally:
            if self.options.rotate_at_midnight:
                self._schedule_next_midnight_rotation()

    def rotate_now(self, write_midnight_marker: bool = False) -> None:
        with self._lock:
            marker_ts = self._ny_midnight_timestamp_str() if write_midnight_marker else None
            if marker_ts is not None:
                self._write_marker_row(marker_ts, self._prev_spx, self._prev_vix)

            if not self.file.closed:
                if self.options.flush_each_write:
                    self.file.flush()
                self.file.close()

            self.current_date = self._ny_date_str()
            self.file_path = self._resolve_filename_for_date(self.current_date)
            self.file = open(self.file_path, "a", encoding="utf-8", newline="")
            self._ensure_header()

            if marker_ts is not None:
                self._write_marker_row(marker_ts, self._prev_spx, self._prev_vix)

            self._prev_spx = None
            self._prev_vix = None
            self._buf.clear()

    def _trim_buffer(self, now_ts: datetime) -> None:
        if self._max_horizon <= 0:
            return
        cutoff = now_ts - timedelta(seconds=self._max_horizon + 5)
        while self._buf and self._buf[0][0] < cutoff:
            self._buf.popleft()

    def _value_at_or_before(self, target_ts: datetime) -> tuple[float, float] | None:
        for ts, spx, vix in reversed(self._buf):
            if ts <= target_ts:
                return spx, vix
        return None

    @staticmethod
    def _pct_change(now_val: float, past_val: float) -> float | None:
        if past_val == 0:
            return None
        return (now_val / past_val - 1.0) * 100.0

    def log(self, spx: float, vix: float) -> None:
        with self._lock:
            now_dt = self._now()
            ts_str = self._ts_str(now_dt)

            d_spx = None if self._prev_spx is None else (spx - self._prev_spx)
            d_vix = None if self._prev_vix is None else (vix - self._prev_vix)

            self._trim_buffer(now_dt)
            self._buf.append((now_dt, spx, vix))

            rolling_parts: list[str] = []
            for h in self._horizons:
                past = self._value_at_or_before(now_dt - timedelta(seconds=h))
                if past is None:
                    rolling_parts += ["", ""]
                    if self.options.include_percent_columns:
                        rolling_parts += ["", ""]
                    continue
                past_spx, past_vix = past
                rolling_parts += [f"{spx - past_spx}", f"{vix - past_vix}"]
                if self.options.include_percent_columns:
                    pct_spx = self._pct_change(spx, past_spx)
                    pct_vix = self._pct_change(vix, past_vix)
                    rolling_parts += [
                        "" if pct_spx is None else f"{pct_spx}",
                        "" if pct_vix is None else f"{pct_vix}",
                    ]

            row = [
                ts_str,
                f"{spx}",
                f"{vix}",
                "" if d_spx is None else f"{d_spx}",
                "" if d_vix is None else f"{d_vix}",
                *rolling_parts,
            ]
            self.file.write(",".join(row) + "\n")
            if self.options.flush_each_write:
                self.file.flush()

            self._prev_spx = spx
            self._prev_vix = vix

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._timer is not None:
                self._timer.cancel()
            if not self.file.closed:
                if self.options.flush_each_write:
                    self.file.flush()
                self.file.close()
