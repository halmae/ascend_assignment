"""
State Machine - Decision Engine 핵심 로직 (v4 - AR(1) 기반)

================================================================================
핵심 변경:
- Stability 판단 = predictability (예측 가능성)
- AR(1) fit quality + forecast error 기반 판단
- "volatility ↑ 이면서 fit quality ↓" → INVALID

판단 로직:
- fit_quality > 0.5 AND forecast_error 정상 → VALID
- fit_quality 0.3~0.5 OR forecast_error 약간 높음 → WEAKENING
- fit_quality < 0.3 OR forecast_error 폭발 → INVALID
================================================================================
"""
from dataclasses import dataclass, field
from typing import List, Optional
from src.config import THRESHOLDS
from src.enums import DataTrustState, HypothesisState, DecisionPermissionState, SanitizationState
from src.uncertainty import UncertaintyVector


@dataclass
class SystemState:
    """시스템 상태"""
    data_trust: DataTrustState = DataTrustState.TRUSTED
    hypothesis: HypothesisState = HypothesisState.VALID
    decision: DecisionPermissionState = DecisionPermissionState.ALLOWED
    
    trust_reasons: List[str] = field(default_factory=list)
    hypothesis_reasons: List[str] = field(default_factory=list)
    sanitization: SanitizationState = SanitizationState.ACCEPT


class StateEvaluator:
    """
    State Machine 평가기 (v4 - AR(1) 기반)
    
    Hypothesis 판단:
    - AR(1) fit quality (predictability)
    - Forecast error (예측 실패 정도)
    - 두 지표의 조합으로 "모델 붕괴" 감지
    """
    
    def __init__(self):
        self.th = THRESHOLDS
        self.max_event_time: int = 0
    
    def evaluate(self, 
                 uncertainty: UncertaintyVector,
                 orderbook_spread_bps: Optional[float] = None) -> SystemState:
        """Uncertainty → SystemState"""
        state = SystemState()
        
        if uncertainty.timestamp > self.max_event_time:
            self.max_event_time = uncertainty.timestamp
        
        # 1. Data Trust (Freshness + Integrity)
        self._evaluate_data_trust(uncertainty, state)
        
        # 2. Hypothesis (AR(1) 기반 Predictability)
        self._evaluate_hypothesis(uncertainty, orderbook_spread_bps, state)
        
        # 3. Decision
        self._determine_decision(state)
        
        return state
    
    def _evaluate_data_trust(self, uncertainty: UncertaintyVector, state: SystemState):
        """Data Trust 평가"""
        trust = DataTrustState.TRUSTED
        reasons = []
        sanitization = SanitizationState.ACCEPT
        
        freshness = uncertainty.freshness
        
        # 음수 latency → QUARANTINE
        if freshness.avg_lateness_ms < 0:
            trust = DataTrustState.UNTRUSTED
            sanitization = SanitizationState.QUARANTINE
            reasons.append(f"negative_latency:{freshness.avg_lateness_ms:.1f}ms")
            state.data_trust = trust
            state.trust_reasons = reasons
            state.sanitization = sanitization
            return
        
        # Watermark 체크
        watermark = self.max_event_time - int(self.th.watermark_delay_ms * 1000)
        if uncertainty.timestamp < watermark:
            trust = DataTrustState.UNTRUSTED
            sanitization = SanitizationState.QUARANTINE
            delay_ms = (watermark - uncertainty.timestamp) / 1000
            reasons.append(f"behind_watermark:{delay_ms:.0f}ms")
            state.data_trust = trust
            state.trust_reasons = reasons
            state.sanitization = sanitization
            return
        
        # Latency
        if freshness.avg_lateness_ms > self.th.freshness_degraded_latency_ms:
            trust = DataTrustState.UNTRUSTED
            reasons.append(f"latency_high:{freshness.avg_lateness_ms:.1f}ms")
        elif freshness.avg_lateness_ms > self.th.freshness_trusted_latency_ms:
            if trust == DataTrustState.TRUSTED:
                trust = DataTrustState.DEGRADED
            reasons.append(f"latency_elevated:{freshness.avg_lateness_ms:.1f}ms")
        
        # Late event
        if freshness.max_lateness_ms > self.th.allowed_lateness_ms:
            if trust == DataTrustState.TRUSTED:
                trust = DataTrustState.DEGRADED
            reasons.append(f"late_event:{freshness.max_lateness_ms:.1f}ms")
        
        # Stale ratio
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
        """Sanitization Policy"""
        if integrity.spread_valid and integrity.price_in_spread:
            if integrity.imbalance_funding_mismatch:
                if self.th.imbalance_funding_strict:
                    reasons.append("imbalance_funding_mismatch")
                    return SanitizationState.QUARANTINE
                else:
                    reasons.append("imbalance_funding_warning")
                    return SanitizationState.REPAIR
            return SanitizationState.ACCEPT
        
        if not integrity.spread_valid:
            if integrity.price_deviation_bps >= self.th.integrity_repair_threshold_bps:
                reasons.append(f"crossed_market:{integrity.price_deviation_bps:.1f}bps")
                return SanitizationState.QUARANTINE
            else:
                reasons.append(f"crossed_minor:{integrity.price_deviation_bps:.1f}bps")
                return SanitizationState.REPAIR
        
        if integrity.price_deviation_bps >= self.th.integrity_repair_threshold_bps * 2:
            reasons.append(f"price_outside:{integrity.price_deviation_bps:.1f}bps")
            return SanitizationState.QUARANTINE
        
        reasons.append(f"price_outside_minor:{integrity.price_deviation_bps:.1f}bps")
        return SanitizationState.REPAIR
    
    def _evaluate_hypothesis(self, 
                             uncertainty: UncertaintyVector,
                             spread_bps: Optional[float],
                             state: SystemState):
        """
        Hypothesis 평가 (AR(1) 기반) - v2
        
        핵심 변경:
        1. fit_quality = None (unknown) → VALID (보수적)
        2. φ 단독 조건 제거
        3. forecast_error 기반 조건 강화
        
        판단 로직:
        - VALID: fit_quality >= 0.6 AND forecast_error <= 2.5σ
        - INVALID: fit_quality <= 0.25 OR forecast_error >= 4σ
        - WEAKENING: 그 외 (gray zone)
        """
        hypothesis = HypothesisState.VALID
        reasons = []
        
        stability = uncertainty.stability
        ar1 = stability.ar1
        
        # === 1. 샘플 부족 (unknown) → VALID (보수적 처리) ===
        if ar1.fit_quality is None:
            state.hypothesis = HypothesisState.VALID
            state.hypothesis_reasons = [f"unknown:n={ar1.n_samples}"]
            return
        
        fit_quality = ar1.fit_quality
        
        # 임계값
        fit_valid = self.th.ar1_fit_quality_valid          # 0.6
        fit_invalid = self.th.ar1_fit_quality_invalid      # 0.25
        err_valid_mult = self.th.ar1_forecast_error_valid_mult    # 2.5
        err_invalid_mult = self.th.ar1_forecast_error_invalid_mult  # 4.0
        
        # === 2. Forecast Error 계산 ===
        forecast_error_ratio = 0.0
        if ar1.residual_std > 1e-10:
            forecast_error_ratio = ar1.forecast_error / ar1.residual_std
        
        # === 3. INVALID 조건 (엄격) ===
        # fit_quality <= 0.25 OR forecast_error >= 4σ
        is_fit_invalid = fit_quality <= fit_invalid
        is_forecast_exploding = forecast_error_ratio >= err_invalid_mult
        
        if is_fit_invalid or is_forecast_exploding:
            hypothesis = HypothesisState.INVALID
            if is_fit_invalid:
                reasons.append(f"ar1_fit_invalid:{fit_quality:.3f}")
            if is_forecast_exploding:
                reasons.append(f"forecast_exploding:{forecast_error_ratio:.2f}σ")
        
        # === 4. VALID 조건 (관대) ===
        # fit_quality >= 0.6 AND forecast_error <= 2.5σ
        elif fit_quality >= fit_valid and forecast_error_ratio <= err_valid_mult:
            hypothesis = HypothesisState.VALID
            # 이유 없음 (정상)
        
        # === 5. WEAKENING (gray zone) ===
        else:
            hypothesis = HypothesisState.WEAKENING
            reasons.append(f"ar1_gray:fit={fit_quality:.3f},err={forecast_error_ratio:.2f}σ")
        
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