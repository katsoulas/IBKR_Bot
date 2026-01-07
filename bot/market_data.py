from ib_insync import Index
def subscribe_spx_vix(ib, exchange):
    spx = Index("SPX", exchange)
    vix = Index("VIX", exchange)
    ib.qualifyContracts(spx, vix)
    return ib.reqMktData(spx), ib.reqMktData(vix)
def latest_price(t):
    return t.last or t.marketPrice() or t.close
