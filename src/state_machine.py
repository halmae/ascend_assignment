"""
State Machine - Decision Engine 핵심 로직

================================================================================
Single Decision Engine:
- config.py의 THRESHOLDS를 참조
- Historical/Realtime에서 동일한 판단 기준 사용
================================================================================

구조:
    UncertaintyVector → StateEvaluator → SystemState
                              ↑
                         THRESHOLDS (config.py)
"""
from dataclasses import dataclass, field
from typing import List, Optional
from src.config import THRESHOLDS
from src.enums import DataTrustState, HypothesisState, DecisionPermissionState, SanitizationState
from src.uncertainty import UncertaintyVector


@dataclass
class SystemState:
    """
    시스템 상태
    
    Data Trust + Hypothesis → Decision
    """
    data_trust: DataTrustState = DataTrustState.TRUSTED
    hypothesis: HypothesisState = HypothesisState.VALID
    decision: DecisionPermissionState = DecisionPermissionState.ALLOWED
    
    # 디버깅용 상태 전이 이유
    trust_reasons: List[str] = field(default_factory=list)
    hypothesis_reasons: List[str] = field(default_factory=list)


class StateEvaluator:
    """
    State Machine 평가기
    
    config.py의 THRESHOLDS를 사용하여 상태 판단
    """
    
    def __init__(self):
        # config.py의 전역 THRESHOLDS 참조
        self.th = THRESHOLDS
    
    def evaluate(self, 
                 uncertainty: UncertaintyVector,
                 orderbook_spread_bps: Optional[float] = None) -> SystemState:
        """
        Uncertainty Vector를 평가하여 SystemState 반환
        
        Args:
            uncertainty: 측정된 불확실성
            orderbook_spread_bps: 현재 스프레드 (bps)
        
        Returns:
            평가된 시스템 상태
        """
        state = SystemState()
        
        # 1. Data Trust 평가
        self._evaluate_data_trust(uncertainty, state)
        
        # 2. Hypothesis 평가
        self._evaluate_hypothesis(uncertainty, orderbook_spread_bps, state)
        
        # 3. Decision 결정 (Trust + Hypothesis 조합)
        self._determine_decision(state)
        
        return state
    
    def _evaluate_data_trust(self, uncertainty: UncertaintyVector, state: SystemState):
        """
        Data Trust 평가
        
        Freshness + Integrity → Data Trust State
        """
        trust = DataTrustState.TRUSTED
        reasons = []
        
        # === Freshness 평가 ===
        freshness = uncertainty.freshness
        
        # Latency 체크
        if freshness.avg_lateness_ms > self.th.freshness_degraded_latency_ms:
            trust = DataTrustState.UNTRUSTED
            reasons.append(f"latency_high:{freshness.avg_lateness_ms:.1f}ms")
        elif freshness.avg_lateness_ms > self.th.freshness_trusted_latency_ms:
            if trust == DataTrustState.TRUSTED:
                trust = DataTrustState.DEGRADED
            reasons.append(f"latency_elevated:{freshness.avg_lateness_ms:.1f}ms")
        
        # Stale ratio 체크
        if freshness.stale_event_ratio > self.th.freshness_degraded_stale_ratio:
            trust = DataTrustState.UNTRUSTED
            reasons.append(f"stale_ratio_high:{freshness.stale_event_ratio:.2%}")
        elif freshness.stale_event_ratio > self.th.freshness_trusted_stale_ratio:
            if trust == DataTrustState.TRUSTED:
                trust = DataTrustState.DEGRADED
            reasons.append(f"stale_ratio_elevated:{freshness.stale_event_ratio:.2%}")
        
        # === Integrity 평가 (Sanitization Policy) ===
        integrity = uncertainty.integrity
        sanitization = self._classify_sanitization(integrity)
        
        if sanitization == SanitizationState.QUARANTINE:
            # QUARANTINE → UNTRUSTED
            trust = DataTrustState.UNTRUSTED
            reasons.append(f"integrity_quarantine:deviation_{integrity.price_deviation_bps:.1f}bps")
        elif sanitization == SanitizationState.REPAIR:
            # REPAIR → DEGRADED (최대)
            if trust == DataTrustState.TRUSTED:
                trust = DataTrustState.DEGRADED
            reasons.append(f"integrity_repair:deviation_{integrity.price_deviation_bps:.1f}bps")
        
        state.data_trust = trust
        state.trust_reasons = reasons
    
    def _classify_sanitization(self, integrity) -> SanitizationState:
        """
        Sanitization Policy 분류
        
        ACCEPT: 정상
        REPAIR: 수정 가능 (minor crossed market)
        QUARANTINE: 신뢰 불가
        """
        if integrity.spread_valid and integrity.price_in_spread:
            return SanitizationState.ACCEPT
        
        # Crossed market 발생
        if not integrity.spread_valid:
            if integrity.price_deviation_bps < self.th.integrity_repair_threshold_bps:
                return SanitizationState.REPAIR
            else:
                return SanitizationState.QUARANTINE
        
        # spread_valid=T but price_in_spread=F (드문 케이스)
        if integrity.price_deviation_bps < self.th.integrity_repair_threshold_bps * 2:
            return SanitizationState.REPAIR
        return SanitizationState.QUARANTINE
    
    def _evaluate_hypothesis(self, 
                             uncertainty: UncertaintyVector,
                             spread_bps: Optional[float],
                             state: SystemState):
        """
        Hypothesis Validity 평가
        
        Stability + Liquidation + Spread → Hypothesis State
        """
        hypothesis = HypothesisState.VALID
        reasons = []
        
        stability = uncertainty.stability
        
        # === Spread Volatility 평가 ===
        if stability.spread_volatility > self.th.stability_weakening_volatility:
            hypothesis = HypothesisState.INVALID
            reasons.append(f"spread_volatility_high:{stability.spread_volatility:.3f}")
        elif stability.spread_volatility > self.th.stability_valid_volatility:
            if hypothesis == HypothesisState.VALID:
                hypothesis = HypothesisState.WEAKENING
            reasons.append(f"spread_volatility_elevated:{stability.spread_volatility:.3f}")
        
        # === Liquidation Cooldown 평가 ===
        if stability.time_since_liquidation_ms is not None:
            time_since = stability.time_since_liquidation_ms
            
            if time_since < self.th.liquidation_weakening_ms:
                hypothesis = HypothesisState.INVALID
                reasons.append(f"post_liquidation_unstable:{time_since:.0f}ms")
            elif time_since < self.th.liquidation_cooldown_ms:
                if hypothesis == HypothesisState.VALID:
                    hypothesis = HypothesisState.WEAKENING
                reasons.append(f"post_liquidation_cooling:{time_since:.0f}ms")
        
        # === Spread 범위 평가 ===
        if spread_bps is not None:
            if spread_bps > self.th.spread_weakening_bps:
                hypothesis = HypothesisState.INVALID
                reasons.append(f"spread_wide:{spread_bps:.1f}bps")
            elif spread_bps > self.th.spread_valid_bps:
                if hypothesis == HypothesisState.VALID:
                    hypothesis = HypothesisState.WEAKENING
                reasons.append(f"spread_elevated:{spread_bps:.1f}bps")
        
        state.hypothesis = hypothesis
        state.hypothesis_reasons = reasons
    
    def _determine_decision(self, state: SystemState):
        """
        Decision Permission 결정
        
        Decision Matrix:
        
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