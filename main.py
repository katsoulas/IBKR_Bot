from __future__ import annotations

import logging
import signal
from collections import deque
from datetime import timedelta

from bot.config import Config
from bot.daily_logger import DailyMarketLogger, LoggerOptions
from bot.ib_client import connect_ibkr, disconnect_ibkr
from bot.market_data import subscribe_spx_vix, latest_price
from bot.signals import trend_from_abs_deltas, trend_from_pct_changes
from bot.time_utils import ny_now

log = logging.getLogger("main")
_shutdown_requested = False

def _request_shutdown(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    log.warning("Shutdown requested (signal=%s).", signum)

def _value_at_or_before(buf: deque[tuple], target_ts):
    for ts, spx, vix in reversed(buf):
        if ts <= target_ts:
            return spx, vix
    return None

def main() -> None:
    global _shutdown_requested
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    cfg = Config()

    signal.signal(signal.SIGINT, _request_shutdown)
    signal.signal(signal.SIGTERM, _request_shutdown)

    ib = None
    logger_obj = None
    last_spx = None
    last_vix = None
    buf = deque()

    try:
        ib = connect_ibkr(cfg.ib_host, cfg.ib_port, cfg.ib_client_id, cfg.ib_readonly)

        logger_obj = DailyMarketLogger(
            base_dir=cfg.logs_dir,
            options=LoggerOptions(
                rotate_at_midnight=True,
                rolling_horizons_seconds=cfg.rolling_horizons_seconds,
                flush_each_write=True,
                include_percent_columns=True,
            ),
        )
        log.info("Logging to: %s", logger_obj.file_path)

        spx_ticker, vix_ticker = subscribe_spx_vix(ib, exchange=cfg.exchange)
        ib.sleep(1.0)

        while not _shutdown_requested:
            spx_now = latest_price(spx_ticker)
            vix_now = latest_price(vix_ticker)

            if spx_now is not None:
                last_spx = spx_now
            if vix_now is not None:
                last_vix = vix_now

            if last_spx is not None and last_vix is not None:
                logger_obj.log(last_spx, last_vix)

                now_ts = ny_now()
                buf.append((now_ts, last_spx, last_vix))
                cutoff = now_ts - timedelta(seconds=max(600, cfg.signal_horizon_s + 5))
                while buf and buf[0][0] < cutoff:
                    buf.popleft()

                past = _value_at_or_before(buf, now_ts - timedelta(seconds=cfg.signal_horizon_s))
                if past is not None:
                    past_spx, past_vix = past
                    d_spx = last_spx - past_spx
                    d_vix = last_vix - past_vix

                    sig_abs = trend_from_abs_deltas(d_spx, d_vix)

                    pct_spx = (last_spx / past_spx - 1.0) * 100.0 if past_spx != 0 else None
                    pct_vix = (last_vix / past_vix - 1.0) * 100.0 if past_vix != 0 else None
                    sig_pct = trend_from_pct_changes(pct_spx, pct_vix)

                    log.info(
                        "Trend(%ss): dSPX=%.4f dVIX=%.4f | abs=%s/%s | pct=%s/%s -> %s/%s",
                        cfg.signal_horizon_s,
                        d_spx,
                        d_vix,
                        sig_abs.spx,
                        sig_abs.vix,
                        (f"{pct_spx:.4f}%" if pct_spx is not None else "NA"),
                        (f"{pct_vix:.4f}%" if pct_vix is not None else "NA"),
                        sig_pct.spx,
                        sig_pct.vix,
                    )
            else:
                log.warning("Waiting for SPX/VIX market data (permissions/session may be required).")

            ib.sleep(cfg.poll_seconds)

    finally:
        try:
            if logger_obj is not None:
                logger_obj.close()
        finally:
            if ib is not None:
                disconnect_ibkr(ib)

if __name__ == "__main__":
    main()
