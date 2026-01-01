"""
Uncertainty - 불확실성 데이터 구조 (v4 - O(1) AR(1))

================================================================================
핵심 최적화:
- AR1Calculator: O(1) rolling statistics
- deque 사용 (O(1) append/popleft)
- Lazy evaluation (_dirty flag)

판단 로직:
- fit_quality > 0.5 → VALID (예측 가능)
- fit_quality < 0.3 → INVALID (예측 불가 = 불안정)
================================================================================
"""
from collections import deque
from dataclasses import dataclass, field
from typing import Optional


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
class AR1Diagnostics:
    """
    AR(1) 모델 진단 결과
    
    모델: s_t = φ * s_{t-1} + ε_t
    
    변경:
    - fit_quality: Optional (None = unknown, 샘플 부족)
    - unknown ≠ unstable
    """
    # φ (autocorrelation coefficient)
    phi: float = 0.0
    
    # Residual 표준편차
    residual_std: float = 0.0
    
    # Forecast error (1-step ahead)
    forecast_error: float = 0.0
    
    # Fit quality (R²) - None이면 "unknown" (샘플 부족)
    fit_quality: Optional[float] = None
    
    # 샘플 수
    n_samples: int = 0
    
    @property
    def is_unknown(self) -> bool:
        """샘플 부족으로 판단 불가"""
        return self.fit_quality is None
    
    @property
    def is_exploding(self) -> bool:
        """Residual이 폭발하는가? (4σ 기준)"""
        if self.residual_std > 0 and self.forecast_error > 0:
            return self.forecast_error > self.residual_std * 4.0
        return False
    
    @property
    def is_forecast_stable(self) -> bool:
        """Forecast가 안정적인가? (2.5σ 이내)"""
        if self.residual_std > 0:
            return self.forecast_error <= self.residual_std * 2.5
        return True


@dataclass
class StabilityUncertainty:
    """시장 안정성 불확실성 (AR(1) 기반)"""
    
    # Legacy: Volatility proxy (CV)
    spread_volatility_proxy: float = 0.0
    
    # AR(1) Diagnostics
    ar1: AR1Diagnostics = field(default_factory=AR1Diagnostics)
    
    # Predictability = fit quality (None이면 unknown)
    predictability: Optional[float] = None
    
    # Liquidation 관련
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
# AR(1) Calculator - O(1) Rolling Statistics
# =============================================================================

class AR1Calculator:
    """
    AR(1) 모델 계산기 - O(1) Rolling Window
    
    모델: s_t = φ * s_{t-1} + ε_t
    
    최적화:
    - deque(maxlen) 사용: O(1) append, 자동 overflow 처리
    - Rolling sum 유지: O(1) 업데이트
    - Lazy evaluation: compute() 호출 시에만 계산
    
    수학적 배경:
    - (x, y) 쌍: (s_{t-1}, s_t)
    - φ = Cov(x,y) / Var(x)
    - Cov(x,y) = E[xy] - E[x]E[y] = Σxy/n - (Σx/n)(Σy/n)
    - Var(x) = E[x²] - E[x]² = Σx²/n - (Σx/n)²
    """
    
    def __init__(self, window_size: int = 100, min_samples: int = 20):
        self.window_size = window_size
        self.min_samples = min_samples  # 최소 샘플 수
        
        # deque with maxlen: 자동으로 오래된 값 제거
        self._values: deque = deque(maxlen=window_size)
        
        # Rolling sums for (x, y) pairs
        self._sum_x = 0.0
        self._sum_y = 0.0
        self._sum_xx = 0.0
        self._sum_xy = 0.0
        self._sum_yy = 0.0
        self._n = 0
        
        # Cached results (None = unknown)
        self._phi: float = 0.0
        self._residual_std: float = 0.0
        self._fit_quality: Optional[float] = None  # None = unknown
        self._dirty: bool = True
    
    def update(self, value: float):
        """
        새 값 추가 - O(1)
        """
        # 새 (x, y) 쌍 추가
        if len(self._values) >= 1:
            x = self._values[-1]  # 이전 값
            y = value             # 현재 값
            self._sum_x += x
            self._sum_y += y
            self._sum_xx += x * x
            self._sum_xy += x * y
            self._sum_yy += y * y
            self._n += 1
        
        # Window overflow 시 가장 오래된 쌍 제거
        if len(self._values) >= self.window_size:
            # 제거될 쌍: (values[0], values[1])
            if len(self._values) >= 2:
                old_x = self._values[0]
                old_y = self._values[1]
                self._sum_x -= old_x
                self._sum_y -= old_y
                self._sum_xx -= old_x * old_x
                self._sum_xy -= old_x * old_y
                self._sum_yy -= old_y * old_y
                self._n -= 1
        
        # 값 저장 (deque가 자동으로 overflow 처리)
        self._values.append(value)
        self._dirty = True
    
    def _recompute(self):
        """내부 통계 재계산 - O(1)"""
        if not self._dirty:
            return
        
        # 샘플 부족 → fit_quality = None (unknown)
        if self._n < self.min_samples:
            self._phi = 0.0
            self._residual_std = 0.0
            self._fit_quality = None  # unknown!
            self._dirty = False
            return
        
        n = self._n
        
        # Mean
        mean_x = self._sum_x / n
        mean_y = self._sum_y / n
        
        # Variance and Covariance
        var_x = max(0, self._sum_xx / n - mean_x * mean_x)
        var_y = max(0, self._sum_yy / n - mean_y * mean_y)
        cov_xy = self._sum_xy / n - mean_x * mean_y
        
        # 수치 안정성 임계값 (spread_bps 기준으로 의미있는 변동)
        # spread가 0.01 bps 이상 변동해야 의미있음
        MIN_VARIANCE = 0.0001  # (0.01 bps)^2
        
        # var_y가 너무 작으면 "변동 없음" → unknown
        if var_y < MIN_VARIANCE:
            self._phi = 0.0
            self._residual_std = 0.0
            self._fit_quality = None  # unknown (변동 없음)
            self._dirty = False
            return
        
        # var_x가 너무 작으면 φ 계산 불가
        if var_x < MIN_VARIANCE:
            self._phi = 0.0
            self._residual_std = var_y ** 0.5
            self._fit_quality = 0.0  # 설명력 없음
            self._dirty = False
            return
        
        # φ = Cov(x, y) / Var(x)
        self._phi = cov_xy / var_x
        
        # φ 범위 제한 (-1 ~ 1) - 수치 오차로 벗어날 수 있음
        self._phi = max(-1.0, min(1.0, self._phi))
        
        # Residual variance = Var(y) - φ² * Var(x)
        residual_var = max(0, var_y - self._phi * self._phi * var_x)
        self._residual_std = residual_var ** 0.5
        
        # Fit quality (R²)
        self._fit_quality = max(0.0, min(1.0, 1.0 - residual_var / var_y))
        
        self._dirty = False
    
    def compute(self) -> AR1Diagnostics:
        """AR(1) 진단 반환 - O(1)"""
        self._recompute()
        
        result = AR1Diagnostics(n_samples=len(self._values))
        result.phi = self._phi
        result.residual_std = self._residual_std
        result.fit_quality = self._fit_quality  # None 가능
        
        # Forecast error: |actual - predicted|
        if len(self._values) >= 2:
            predicted = self._phi * self._values[-2]
            actual = self._values[-1]
            result.forecast_error = abs(actual - predicted)
        
        return result
    
    def reset(self):
        """초기화"""
        self._values.clear()
        self._sum_x = 0.0
        self._sum_y = 0.0
        self._sum_xx = 0.0
        self._sum_xy = 0.0
        self._sum_yy = 0.0
        self._n = 0
        self._phi = 0.0
        self._residual_std = 0.0
        self._fit_quality = None  # unknown
        self._dirty = True