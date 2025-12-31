from dataclasses import dataclass, field
from typing import Dict, Any

@dataclass
class StreamProcessorConfig:
    """StreamProcessor 설정"""

    # Buffer
    buffer_size: int = 1000
    watermark_delay_ms: int = 50
    snapshot_buffer_size: int = 5

    # Consistency Check 관련
    consistency_check_enabled: bool = True
    consistency_log_interval: int = 10

    # State Transition Thrshold
    trusted_threshold: float = 0.9
    degraded_threshold: float = 0.5

    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        return {
            'buffer_size': self.buffer_size,
            'watermark_delay_ms': self.watermark_delay_ms,
            'snapshot_buffer_size': self.snapshot_buffer_size,
            'consistency_check_enabled': self.consistency_check_enabled,
            'consistency_log_interval': self.consistency_log_interval,
            'trusted_threshold': self.trusted_threshold,
            'degraded_threshold': self.degraded_threshold,
        }
    

@dataclass
class ConsistencyConfig:
    """Consistency Check 설정"""

    price_tolerance_last_vs_mid: float = 0.1    # 10 bps
    price_tolerance_mark_vs_mid: float = 0.2    # 20 bps
    price_tolerance_index_vs_mid: float = 0.5   # 50 bps
    price_tolerance_mark_vs_index: float = 0.3  # 30 bps

    # Spread Threshold
    spread_excellent: float = 1
    spread_normal: float = 5
    spread_wide: float = 10
    spread_very_wide: float = 20

    # Depth 기준 (BTC)
    min_depth_btc: float = 1.0

    # Trade validation
    trade_amount_tolerance: float = 0.01

    # Consistency weight
    weights: Dict[str, float] = field(default_factory=lambda: {
        'price': 0.3,
        'spread': 0.2,
        'imbalance_funding': 0.2,
        'depth': 0.2,
        'system': 0.1
    })


DEFAULT_PROCESSOR_CONFIG = StreamProcessorConfig()
DEFAULT_CONSISTENCY_CONFIG = ConsistencyConfig()