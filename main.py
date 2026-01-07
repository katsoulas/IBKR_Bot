from bot.config import Config
from bot.ib_client import connect_ibkr, disconnect_ibkr
from bot.market_data import subscribe_spx_vix, latest_price
from bot.daily_logger import DailyMarketLogger, LoggerOptions
import signal, logging

logging.basicConfig(level=logging.INFO)
_shutdown = False

def _stop(*_):
    global _shutdown
    _shutdown = True

signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)

cfg = Config()
ib = connect_ibkr(cfg.ib_host, cfg.ib_port, cfg.ib_client_id, cfg.ib_readonly)

logger = DailyMarketLogger(cfg.logs_dir, LoggerOptions())
spx_t, vix_t = subscribe_spx_vix(ib, cfg.exchange)
ib.sleep(1)

last_spx = last_vix = None

while not _shutdown:
    s = latest_price(spx_t)
    v = latest_price(vix_t)
    if s is not None:
        last_spx = s
    if v is not None:
        last_vix = v
    if last_spx and last_vix:
        logger.log(last_spx, last_vix)
    ib.sleep(cfg.poll_seconds)

logger.close()
disconnect_ibkr(ib)
