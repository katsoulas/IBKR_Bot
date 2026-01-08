from dataclasses import dataclass

@dataclass(frozen=True)
class Config:
    ib_host: str = "127.0.0.1"
    ib_port: int = 7497
    ib_client_id: int = 7
    ib_readonly: bool = False

    exchange: str = "CBOE"
    poll_seconds: float = 1.0
    rolling_horizons_seconds: tuple[int, ...] = (10, 60, 300)
    logs_dir: str = "logs"
    signal_horizon_s: int = 60
