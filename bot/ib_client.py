from __future__ import annotations

import logging
from ib_insync import IB

log = logging.getLogger("bot.ib_client")

def connect_ibkr(host: str, port: int, client_id: int, readonly: bool) -> IB:
    ib = IB()
    ib.connect(host=host, port=port, clientId=client_id, readonly=readonly, timeout=10)
    log.info("IBKR API is now connected")
    return ib

def disconnect_ibkr(ib: IB) -> None:
    try:
        if ib and ib.isConnected():
            ib.disconnect()
    finally:
        log.info("IBKR API is now disconnected")
