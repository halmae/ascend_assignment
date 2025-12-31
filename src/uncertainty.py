"""
Uncertainty 정의 모듈

Effective Orderbook State = (Orderbook Structure, Uncertainty Vector U_t)

U_t = {
    Freshness: 데이터가 얼마나 신선한가?
    Integrity: 데이터가 서로 일관성이 있는가?
    Stability: Orderbook이 안정적인가?
}
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from enum import Enum


class TradabilityState(Enum):
    """Tradability 상태"""
    TRADABLE = "TRADABLE"           # 모든 uncertainty가 허용 범위 내
    RESTRICTED = "RESTRICTED"       # 일부 uncertainty 초과 - 제한된 판단만 허용
    NOT_TRADABLE = "NOT_TRADABLE"   # uncertainty가 너무 높음 - 판단 중단


@dataclass
class FreshnessUncertainty:
    """
    Freshness Uncertainty: 데이터가 얼마나 신선한가?
    
    - avg_lateness_ms: 최근 윈도우 내 평균 lateness
    - max_lateness_ms: 최근 윈도우 내 최대 lateness
    - stale_event_ratio: lateness threshold 초과 이벤트 비율
    """
    avg_lateness_ms: float = 0.0
    max_lateness_ms: float = 0.0
    stale_event_ratio: float = 0.0  # threshold 초과 비율 (drop하지 않고 측정만)
    
    def is_acceptable(self, 
                      max_avg_lateness: float = 20.0,
                      max_stale_ratio: float = 0.05) -> bool:
        """Freshness가 허용 범위 내인가?"""
        return (self.avg_lateness_ms <= max_avg_lateness and 
                self.stale_event_ratio <= max_stale_ratio)


@dataclass
class IntegrityUncertainty:
    """
    Integrity Uncertainty: 데이터가 서로 일관성이 있는가?
    
    Consistency checks:
    - spread_valid: Orderbook이 crossed market이 아닌가?
    - price_in_spread: Last price가 mid price 근처인가?
    - funding_imbalance_aligned: Funding과 Imbalance 부호가 일치하는가?
    """
    spread_valid: bool = True
    price_in_spread: bool = True
    funding_imbalance_aligned: bool = True
    
    # 최근 윈도우 내 failure rate
    spread_valid_failure_rate: float = 0.0
    price_in_spread_failure_rate: float = 0.0
    funding_aligned_failure_rate: float = 0.0
    
    @property
    def all_consistent(self) -> bool:
        """현재 시점에서 모든 consistency check 통과?"""
        return self.spread_valid and self.price_in_spread and self.funding_imbalance_aligned
    
    @property
    def failure_count(self) -> int:
        """현재 시점 실패 개수"""
        return sum([
            not self.spread_valid,
            not self.price_in_spread,
            not self.funding_imbalance_aligned
        ])
    
    def is_acceptable(self, max_failure_rate: float = 0.05) -> bool:
        """Integrity가 허용 범위 내인가?"""
        return (self.spread_valid and  # 현재 crossed market이 아니어야 함
                self.spread_valid_failure_rate <= max_failure_rate)


@dataclass
class StabilityUncertainty:
    """
    Stability Uncertainty: Orderbook이 안정적인가?
    
    특히 Liquidation 이후 안정성 측정
    - spread_volatility: Spread의 시간적 변동성
    - imbalance_volatility: Imbalance의 시간적 변동성
    - time_since_liquidation_ms: 마지막 대규모 청산 이후 경과 시간
    - post_liquidation_stable: 청산 이후 안정화되었는가?
    """
    spread_volatility: float = 0.0
    imbalance_volatility: float = 0.0
    mid_price_volatility: float = 0.0
    
    # Liquidation 관련
    time_since_liquidation_ms: Optional[float] = None
    liquidation_size: float = 0.0
    post_liquidation_stable: bool = True
    
    def is_acceptable(self,
                      max_spread_volatility: float = 0.1,
                      min_time_after_liquidation_ms: float = 5000.0) -> bool:
        """Stability가 허용 범위 내인가?"""
        # 최근 청산이 있었다면 충분한 시간이 지났는지 확인
        if self.time_since_liquidation_ms is not None:
            if self.time_since_liquidation_ms < min_time_after_liquidation_ms:
                return False
        
        return self.spread_volatility <= max_spread_volatility


@dataclass
class UncertaintyVector:
    """
    Uncertainty Vector U_t
    
    Effective Orderbook State = (Orderbook Structure, U_t)
    """
    freshness: FreshnessUncertainty = field(default_factory=FreshnessUncertainty)
    integrity: IntegrityUncertainty = field(default_factory=IntegrityUncertainty)
    stability: StabilityUncertainty = field(default_factory=StabilityUncertainty)
    
    timestamp: int = 0
    
    def get_tradability_state(self) -> TradabilityState:
        """
        Tradability 판단
        
        U_t ≤ U_max → TRADABLE
        """
        # Integrity가 깨지면 (crossed market) → NOT_TRADABLE
        if not self.integrity.spread_valid:
            return TradabilityState.NOT_TRADABLE
        
        # Stability가 안정적이지 않으면 → NOT_TRADABLE
        if not self.stability.post_liquidation_stable:
            return TradabilityState.NOT_TRADABLE
        
        # 모든 조건 만족 → TRADABLE
        if (self.freshness.is_acceptable() and 
            self.integrity.is_acceptable() and 
            self.stability.is_acceptable()):
            return TradabilityState.TRADABLE
        
        # 일부 조건 미달 → RESTRICTED
        return TradabilityState.RESTRICTED
    
    def to_log_dict(self) -> Dict:
        """로깅용 딕셔너리"""
        return {
            'timestamp': self.timestamp,
            'tradability': self.get_tradability_state().value,
            'freshness': {
                'avg_lateness_ms': round(self.freshness.avg_lateness_ms, 2),
                'max_lateness_ms': round(self.freshness.max_lateness_ms, 2),
                'stale_ratio': round(self.freshness.stale_event_ratio, 4),
            },
            'integrity': {
                'spread_valid': self.integrity.spread_valid,
                'price_in_spread': self.integrity.price_in_spread,
                'funding_aligned': self.integrity.funding_imbalance_aligned,
                'failure_count': self.integrity.failure_count,
            },
            'stability': {
                'spread_volatility': round(self.stability.spread_volatility, 4),
                'time_since_liq_ms': self.stability.time_since_liquidation_ms,
                'post_liq_stable': self.stability.post_liquidation_stable,
            }
        }