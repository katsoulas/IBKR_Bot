from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class TrendSignal:
    spx: str
    vix: str

def _classify(value: Optional[float], up_thresh: float, down_thresh: float) -> str:
    if value is None:
        return "NA"
    if value >= up_thresh:
        return "UP"
    if value <= -down_thresh:
        return "DOWN"
    return "FLAT"

def trend_from_abs_deltas(d_spx_h: Optional[float], d_vix_h: Optional[float],
                          spx_up: float = 0.25, spx_down: float = 0.25,
                          vix_up: float = 0.03, vix_down: float = 0.03) -> TrendSignal:
    return TrendSignal(_classify(d_spx_h, spx_up, spx_down), _classify(d_vix_h, vix_up, vix_down))

def trend_from_pct_changes(pct_spx_h: Optional[float], pct_vix_h: Optional[float],
                           spx_up_pct: float = 0.02, spx_down_pct: float = 0.02,
                           vix_up_pct: float = 0.10, vix_down_pct: float = 0.10) -> TrendSignal:
    return TrendSignal(_classify(pct_spx_h, spx_up_pct, spx_down_pct), _classify(pct_vix_h, vix_up_pct, vix_down_pct))
