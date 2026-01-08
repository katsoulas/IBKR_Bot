from __future__ import annotations

from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

def _load_ny_tz():
    if ZoneInfo is not None:
        try:
            return ZoneInfo("America/New_York")
        except Exception:
            pass
    if ZoneInfo is not None:
        try:
            return ZoneInfo("Eastern Standard Time")
        except Exception:
            pass
    return timezone.utc

NY_TZ = _load_ny_tz()

def ny_now() -> datetime:
    return datetime.now(NY_TZ)

def fmt_ts_ms(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
