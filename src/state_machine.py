"""
State Machine - 시스템 상태 관리 (v3 - Price Volatility)

================================================================================
v3 변경사항:
- AR(1) 기반 Hypothesis 제거
- Price Volatility 기반 Hypothesis로 교체
- EDA 결과: 평상시 p90=0.50, p95=0.62 기준 사용
================================================================================

핵심 구조:
    Data Trust (Freshness + Integrity) → TRUSTED / DEGRADED / UNTRUSTED
    Hypothesis (Volatility) → VALID / WEAKENING / INVALID
    
    Decision = f(Data Trust, Hypothesis)
"""
from dataclasses import dataclass, field
from typing import List, Optional

from src.config import THRESHOLDS
from src.enums import (
    DataTrustState,
    HypothesisState,
    DecisionPermissionState,
    SanitizationState,
)
from src.uncertainty import UncertaintyVector


@dataclass
class SystemState:
    """시스템 상태"""
    data_trust: DataTrustState = DataTrustState.TRUSTED
    hypothesis: HypothesisState = HypothesisState.VALID
    decision: DecisionPermissionState = DecisionPermissionState.ALLOWED
    sanitization: SanitizationState = SanitizationState.ACCEPT
    
    trust_reasons: List[str] = field(default_factory=list)
    hypothesis_reasons: List[str] = field(default_factory=list)


class StateEvaluator:
    """
    State Machine Evaluator (v3 - Price Volatility)
    
    평가 순서:
    1. Data Trust 평가 (Freshness + Integrity)
    2. Hypothesis 평가 (Volatility 기반)
    3. Decision 결정 (Trust × Hypothesis Matrix)
    """
    
    def __init__(self):
        self.th = THRESHOLDS
    
    def evaluate(self, 
                 uncertainty: UncertaintyVector,
                 orderbook_spread_bps: Optional[float] = None) -> SystemState:
        """전체 평가 수행"""
        state = SystemState()
        
        # 1. Data Trust 평가
        self._evaluate_data_trust(uncertainty, state)
        
        # 2. Hypothesis 평가
        self._evaluate_hypothesis(uncertainty, state)
        
        # 3. Decision 결정
        self._determine_decision(state)
        
        return state
    
    def _evaluate_data_trust(self, uncertainty: UncertaintyVector, state: SystemState):
        """
        Data Trust 평가
        
        Freshness → latency, stale ratio
        Integrity → Sanitization Policy
        """
        trust = DataTrustState.TRUSTED
        reasons = []
        
        # Freshness
        freshness = uncertainty.freshness
        
        # Latency 기반
        if freshness.avg_lateness_ms > self.th.freshness_degraded_latency_ms:
            trust = DataTrustState.UNTRUSTED
            reasons.append(f"latency_high:{freshness.avg_lateness_ms:.1f}ms")
        elif freshness.avg_lateness_ms > self.th.freshness_trusted_latency_ms:
            trust = DataTrustState.DEGRADED
            reasons.append(f"latency_elevated:{freshness.avg_lateness_ms:.1f}ms")
        
        # Stale ratio 기반
        if freshness.stale_event_ratio > self.th.freshness_degraded_stale_ratio:
            trust = DataTrustState.UNTRUSTED
            reasons.append(f"stale_high:{freshness.stale_event_ratio:.2%}")
        elif freshness.stale_event_ratio > self.th.freshness_trusted_stale_ratio:
            if trust == DataTrustState.TRUSTED:
                trust = DataTrustState.DEGRADED
            reasons.append(f"stale_elevated:{freshness.stale_event_ratio:.2%}")
        
        # Integrity / Sanitization
        integrity = uncertainty.integrity
        sanitization = self._classify_sanitization(integrity, reasons)
        
        if sanitization == SanitizationState.QUARANTINE:
            trust = DataTrustState.UNTRUSTED
        elif sanitization == SanitizationState.REPAIR:
            if trust == DataTrustState.TRUSTED:
                trust = DataTrustState.DEGRADED
        
        state.data_trust = trust
        state.trust_reasons = reasons
        state.sanitization = sanitization
    
    def _classify_sanitization(self, integrity, reasons: List[str]) -> SanitizationState:
        """
        Sanitization Policy (3단계)
        
        Crossed Market 처리:
          - < 10 bps:  ACCEPT (시장 노이즈)
          - < 30 bps:  REPAIR (주의 필요하나 판단 가능)
          - >= 30 bps: QUARANTINE (신뢰 불가)
        
        Price Outside Spread 처리:
          - < 5 bps:   REPAIR
          - < 10 bps:  REPAIR
          - >= 10 bps: QUARANTINE
        """
        # 정상 케이스: spread valid & price in spread
        if integrity.spread_valid and integrity.price_in_spread:
            if integrity.imbalance_funding_mismatch:
                if self.th.imbalance_funding_strict:
                    reasons.append("imbalance_funding_mismatch")
                    return SanitizationState.QUARANTINE
                else:
                    reasons.append("imbalance_funding_warning")
                    return SanitizationState.REPAIR
            return SanitizationState.ACCEPT
        
        # Crossed Market (bid >= ask)
        if not integrity.spread_valid:
            deviation = integrity.price_deviation_bps
            
            if deviation < self.th.crossed_accept_threshold_bps:
                # 작은 crossed: 시장 노이즈로 허용
                # 로그에는 남기지만 ACCEPT
                return SanitizationState.ACCEPT
            
            elif deviation < self.th.crossed_quarantine_threshold_bps:
                # 중간 crossed: 주의 필요
                reasons.append(f"crossed_repair:{deviation:.1f}bps")
                return SanitizationState.REPAIR
            
            else:
                # 심각한 crossed: 신뢰 불가
                reasons.append(f"crossed_quarantine:{deviation:.1f}bps")
                return SanitizationState.QUARANTINE
        
        # Price outside spread (spread는 정상이지만 price가 밖에 있음)
        deviation = integrity.price_deviation_bps
        
        if deviation >= self.th.price_outside_quarantine_bps:
            reasons.append(f"price_outside:{deviation:.1f}bps")
            return SanitizationState.QUARANTINE
        
        reasons.append(f"price_outside_minor:{deviation:.1f}bps")
        return SanitizationState.REPAIR
    
    def _evaluate_hypothesis(self, 
                             uncertainty: UncertaintyVector,
                             state: SystemState):
        """
        Hypothesis 평가 (Price Volatility 기반)
        
        판단 로직 (EDA 결과 기반):
        - volatility = None (unknown) → VALID (보수적)
        - volatility <= 0.50 (p90) → VALID
        - volatility <= 0.62 (p95) → WEAKENING
        - volatility > 0.62 → INVALID
        """
        hypothesis = HypothesisState.VALID
        reasons = []
        
        stability = uncertainty.stability
        vol_diag = stability.volatility
        
        # === 1. 샘플 부족 (unknown) → VALID (보수적 처리) ===
        if vol_diag.is_unknown:
            state.hypothesis = HypothesisState.VALID
            state.hypothesis_reasons = [f"unknown:n={vol_diag.n_samples}"]
            return
        
        volatility = vol_diag.volatility
        
        # 임계값 (config에서)
        valid_threshold = self.th.volatility_valid_threshold       # 0.50 (p90)
        weakening_threshold = self.th.volatility_weakening_threshold  # 0.62 (p95)
        
        # === 2. 판단 ===
        if volatility <= valid_threshold:
            hypothesis = HypothesisState.VALID
            # 이유 없음 (정상)
        
        elif volatility <= weakening_threshold:
            hypothesis = HypothesisState.WEAKENING
            reasons.append(f"volatility_elevated:{volatility:.3f}bps")
        
        else:
            hypothesis = HypothesisState.INVALID
            reasons.append(f"volatility_high:{volatility:.3f}bps")
        
        # === 추가 정보: Liquidation 근처 (참고용) ===
        if stability.time_since_liquidation_ms is not None:
            time_since = stability.time_since_liquidation_ms
            if time_since < 5000:
                reasons.append(f"recent_liq:{time_since:.0f}ms")
        
        state.hypothesis = hypothesis
        state.hypothesis_reasons = reasons
    
    def _determine_decision(self, state: SystemState):
        """
        Decision 결정
        
        Matrix:
                        │ VALID      │ WEAKENING  │ INVALID
        ────────────────┼────────────┼────────────┼───────────
        TRUSTED         │ ALLOWED    │ RESTRICTED │ HALTED
        DEGRADED        │ RESTRICTED │ RESTRICTED │ HALTED
        UNTRUSTED       │ HALTED     │ HALTED     │ HALTED
        """
        trust = state.data_trust
        hypothesis = state.hypothesis
        
        if trust == DataTrustState.UNTRUSTED:
            state.decision = DecisionPermissionState.HALTED
        elif hypothesis == HypothesisState.INVALID:
            state.decision = DecisionPermissionState.HALTED
        elif trust == DataTrustState.DEGRADED or hypothesis == HypothesisState.WEAKENING:
            state.decision = DecisionPermissionState.RESTRICTED
        else:
            state.decision = DecisionPermissionState.ALLOWED