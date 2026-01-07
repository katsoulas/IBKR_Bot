from ib_insync import IB
import logging
def connect_ibkr(host, port, cid, readonly):
    ib = IB()
    ib.connect(host, port, clientId=cid, readonly=readonly)
    logging.info("IBKR API is now connected")
    return ib
def disconnect_ibkr(ib):
    if ib and ib.isConnected():
        ib.disconnect()
        logging.info("IBKR API is now disconnected")
