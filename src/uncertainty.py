"""
Uncertainty - 불확실성 데이터 구조 (v2)

================================================================================
변경사항:
1. IntegrityUncertainty에 imbalance_funding_mismatch 추가
2. StabilityUncertainty에 spread_volatility_zscore 추가
3. FreshnessUncertainty에 late_event_count 추가
================================================================================
"""
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class FreshnessUncertainty:
    """
    데이터 신선도 불확실성
    
    Time Alignment Policy 관련 지표
    """
    # 평균 지연 (ms)
    avg_lateness_ms: float = 0.0
    
    # 최대 지연 (ms) - allowed_lateness 체크용
    max_lateness_ms: float = 0.0
    
    # Stale 이벤트 비율 (allowed_lateness 초과 비율)
    stale_event_ratio: float = 0.0
    
    # Late 이벤트 수 (buffer_duration 초과)
    late_event_count: int = 0
    
    # Out-of-order 이벤트 수
    out_of_order_count: int = 0


@dataclass
class IntegrityUncertainty:
    """
    데이터 무결성 불확실성
    
    Sanitization Policy 판단용 지표
    """
    # Spread 유효성 (Crossed market 여부)
    spread_valid: bool = True
    
    # Last price가 spread 안에 있는지
    price_in_spread: bool = True
    
    # Price deviation (bps) - mid price 대비
    price_deviation_bps: float = 0.0
    
    # Funding rate 이상 여부
    funding_imbalance: bool = False
    
    # === 새로 추가: Imbalance-Funding 불일치 ===
    # Orderbook imbalance와 funding rate 방향 불일치
    imbalance_funding_mismatch: bool = False
    
    # Orderbook imbalance 값 (-1 ~ +1)
    # (bid_depth - ask_depth) / (bid_depth + ask_depth)
    orderbook_imbalance: float = 0.0
    
    # 현재 funding rate
    funding_rate: Optional[float] = None


@dataclass
class StabilityUncertainty:
    """
    시장 안정성 불확실성
    
    Hypothesis Validity 판단용 지표
    """
    # === Spread Volatility (변동성) ===
    # 최근 spread들의 CV (Coefficient of Variation)
    spread_volatility: float = 0.0
    
    # === 새로 추가: z-score 기반 지표 ===
    # spread_volatility를 정상 분포 대비 z-score로 변환
    spread_volatility_zscore: Optional[float] = None
    
    # === Liquidation 관련 (참조용) ===
    # 직접 판단에는 사용 안 함 (Orderbook Health로 대체 예정)
    time_since_liquidation_ms: Optional[float] = None
    liquidation_size: float = 0.0
    post_liquidation_stable: bool = True


@dataclass
class UncertaintyVector:
    """
    전체 불확실성 벡터
    
    StateEvaluator에서 사용
    """
    freshness: FreshnessUncertainty = field(default_factory=FreshnessUncertainty)
    integrity: IntegrityUncertainty = field(default_factory=IntegrityUncertainty)
    stability: StabilityUncertainty = field(default_factory=StabilityUncertainty)
    
    # 이벤트 timestamp (watermark 계산용)
    timestamp: int = 0