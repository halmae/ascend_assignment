"""
Uncertainty Vector - 불확실성 측정 구조체

각 차원의 불확실성을 측정하여 State Machine에 전달
"""
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class FreshnessUncertainty:
    """
    Freshness (데이터 신선도) 불확실성
    
    측정 항목:
    - avg_lateness_ms: 평균 지연 시간
    - max_lateness_ms: 최대 지연 시간
    - stale_event_ratio: stale 이벤트 비율
    """
    avg_lateness_ms: float = 0.0
    max_lateness_ms: float = 0.0
    stale_event_ratio: float = 0.0


@dataclass
class IntegrityUncertainty:
    """
    Integrity (데이터 무결성) 불확실성
    
    Sanitization Policy:
    - ACCEPT: spread_valid=T, price_in_spread=T
    - REPAIR: spread_valid=F, deviation < threshold
    - QUARANTINE: spread_valid=F, deviation >= threshold
    """
    spread_valid: bool = True
    price_in_spread: bool = True
    funding_imbalance: bool = False
    price_deviation_bps: float = 0.0    # Sanitization 판단용
    
    # 보조 지표
    failure_rate: float = 0.0


@dataclass
class StabilityUncertainty:
    """
    Stability (시장 안정성) 불확실성
    
    측정 항목:
    - spread_volatility: 스프레드 변동성 (CV)
    - time_since_liquidation_ms: 마지막 청산 후 경과 시간
    - post_liquidation_stable: 청산 후 안정화 여부
    """
    spread_volatility: float = 0.0
    time_since_liquidation_ms: Optional[float] = None
    liquidation_size: float = 0.0
    post_liquidation_stable: bool = True


@dataclass
class UncertaintyVector:
    """
    통합 Uncertainty Vector
    
    State Machine의 입력으로 사용
    """
    freshness: FreshnessUncertainty = field(default_factory=FreshnessUncertainty)
    integrity: IntegrityUncertainty = field(default_factory=IntegrityUncertainty)
    stability: StabilityUncertainty = field(default_factory=StabilityUncertainty)
    timestamp: int = 0