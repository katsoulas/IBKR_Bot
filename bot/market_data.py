from __future__ import annotations

from ib_insync import IB, Index, Ticker

def subscribe_spx_vix(ib: IB, exchange: str = "CBOE") -> tuple[Ticker, Ticker]:
    spx_contract = Index("SPX", exchange)
    vix_contract = Index("VIX", exchange)
    ib.qualifyContracts(spx_contract, vix_contract)
    spx_ticker = ib.reqMktData(spx_contract, genericTickList="", snapshot=False, regulatorySnapshot=False)
    vix_ticker = ib.reqMktData(vix_contract, genericTickList="", snapshot=False, regulatorySnapshot=False)
    return spx_ticker, vix_ticker

def latest_price(t: Ticker) -> float | None:
    if t.last is not None:
        return float(t.last)
    mp = t.marketPrice()
    if mp is not None:
        return float(mp)
    if t.close is not None:
        return float(t.close)
    return None
