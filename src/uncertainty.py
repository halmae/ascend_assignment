"""
Uncertainty - 불확실성 데이터 구조 (v3 - Price Volatility)

================================================================================
v3 변경사항:
- AR(1) Calculator 제거 (spread 변동이 너무 작아 무의미)
- VolatilityCalculator 추가 (rolling std of price returns)
- window=75가 최적 (EDA 결과: Cohen's d = 0.537)

판단 로직:
- volatility <= 0.50 bps → VALID
- volatility <= 0.62 bps → WEAKENING  
- volatility > 0.62 bps → INVALID
================================================================================
"""
from collections import deque
from dataclasses import dataclass, field
from typing import Optional
import math


@dataclass
class FreshnessUncertainty:
    """데이터 신선도 불확실성"""
    avg_lateness_ms: float = 0.0
    max_lateness_ms: float = 0.0
    stale_event_ratio: float = 0.0
    late_event_count: int = 0
    out_of_order_count: int = 0


@dataclass
class IntegrityUncertainty:
    """데이터 무결성 불확실성"""
    spread_valid: bool = True
    price_in_spread: bool = True
    price_deviation_bps: float = 0.0
    funding_imbalance: bool = False
    imbalance_funding_mismatch: bool = False
    orderbook_imbalance: float = 0.0
    funding_rate: Optional[float] = None


@dataclass
class VolatilityDiagnostics:
    """
    Price Volatility 진단 결과
    
    지표: rolling std of mid_price returns (bps)
    """
    # 현재 volatility (bps)
    volatility: Optional[float] = None
    
    # 샘플 수
    n_samples: int = 0
    
    # 추가 정보
    last_return: float = 0.0
    mean_return: float = 0.0
    
    @property
    def is_unknown(self) -> bool:
        """샘플 부족으로 판단 불가"""
        return self.volatility is None


@dataclass
class StabilityUncertainty:
    """시장 안정성 불확실성 (Price Volatility 기반)"""
    
    # Price Volatility 진단
    volatility: VolatilityDiagnostics = field(default_factory=VolatilityDiagnostics)
    
    # Liquidation 관련 (참고용)
    time_since_liquidation_ms: Optional[float] = None
    liquidation_size: float = 0.0


@dataclass
class UncertaintyVector:
    """전체 불확실성 벡터"""
    freshness: FreshnessUncertainty = field(default_factory=FreshnessUncertainty)
    integrity: IntegrityUncertainty = field(default_factory=IntegrityUncertainty)
    stability: StabilityUncertainty = field(default_factory=StabilityUncertainty)
    timestamp: int = 0


# =============================================================================
# Volatility Calculator - O(1) Rolling Statistics
# =============================================================================

class VolatilityCalculator:
    """
    Price Volatility 계산기 - O(1) Rolling Window
    
    지표: rolling std of price returns (bps)
    
    Returns 정의:
        return_t = (price_t - price_{t-1}) / price_{t-1} * 10000  (bps)
    
    Volatility 정의:
        volatility = std(returns) over window
    
    최적화:
    - deque(maxlen) 사용: O(1) append
    - Rolling sum, sum_sq 유지: O(1) 업데이트
    - Lazy evaluation: compute() 호출 시에만 계산
    """
    
    def __init__(self, window_size: int = 75, min_samples: int = 20):
        self.window_size = window_size
        self.min_samples = min_samples
        
        # Price history (for return calculation)
        self._prices: deque = deque(maxlen=window_size + 1)
        
        # Returns history
        self._returns: deque = deque(maxlen=window_size)
        
        # Rolling statistics for returns
        self._sum = 0.0
        self._sum_sq = 0.0
        self._n = 0
        
        # Cached result
        self._volatility: Optional[float] = None
        self._dirty: bool = True
    
    def update(self, mid_price: float):
        """
        새 가격 추가 - O(1)
        """
        # Return 계산 (이전 가격 대비)
        if len(self._prices) >= 1:
            prev_price = self._prices[-1]
            if prev_price > 0:
                ret = (mid_price - prev_price) / prev_price * 10000  # bps
                
                # Window overflow 시 가장 오래된 return 제거
                if len(self._returns) >= self.window_size:
                    old_ret = self._returns[0]
                    self._sum -= old_ret
                    self._sum_sq -= old_ret * old_ret
                    self._n -= 1
                
                # 새 return 추가
                self._returns.append(ret)
                self._sum += ret
                self._sum_sq += ret * ret
                self._n += 1
        
        # 가격 저장
        self._prices.append(mid_price)
        self._dirty = True
    
    def _recompute(self):
        """내부 통계 재계산 - O(1)"""
        if not self._dirty:
            return
        
        # 샘플 부족 → None (unknown)
        if self._n < self.min_samples:
            self._volatility = None
            self._dirty = False
            return
        
        n = self._n
        
        # Variance = E[X²] - E[X]²
        mean = self._sum / n
        variance = self._sum_sq / n - mean * mean
        
        # Numerical stability
        if variance < 0:
            variance = 0
        
        self._volatility = math.sqrt(variance)
        self._dirty = False
    
    def compute(self) -> VolatilityDiagnostics:
        """Volatility 진단 반환 - O(1)"""
        self._recompute()
        
        result = VolatilityDiagnostics(n_samples=self._n)
        result.volatility = self._volatility
        
        # 추가 정보
        if self._n > 0:
            result.mean_return = self._sum / self._n
            if len(self._returns) > 0:
                result.last_return = self._returns[-1]
        
        return result
    
    def reset(self):
        """초기화 (snapshot 등으로 연속성 끊길 때)"""
        self._prices.clear()
        self._returns.clear()
        self._sum = 0.0
        self._sum_sq = 0.0
        self._n = 0
        self._volatility = None
        self._dirty = True