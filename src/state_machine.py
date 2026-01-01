"""
State Machine - Decision Engine 핵심 로직 (v2)

================================================================================
변경사항:
1. Stability를 z-score 기반으로 변경
2. Liquidation cooldown 제거 (→ 추후 Orderbook Health로 대체)
3. Spread 별도 파라미터 제거 (→ Stability에 통합)
4. Sanitization Policy 강화 (음수 latency, imbalance-funding)
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
    
    # 디버깅용 상태 전이 이유
    trust_reasons: List[str] = field(default_factory=list)
    hypothesis_reasons: List[str] = field(default_factory=list)
    
    # Sanitization 상태 추가
    sanitization: SanitizationState = SanitizationState.ACCEPT


class StateEvaluator:
    """
    State Machine 평가기 (v2)
    
    개선사항:
    - Sanitization Policy 강화 (음수 latency, watermark, imbalance-funding)
    - Stability를 z-score 기반으로 변경
    - Liquidation cooldown 제거
    """
    
    def __init__(self):
        self.th = THRESHOLDS
        # Watermark 추적 (가장 최근 본 event time)
        self.max_event_time: int = 0
    
    def evaluate(self, 
                 uncertainty: UncertaintyVector,
                 orderbook_spread_bps: Optional[float] = None) -> SystemState:
        """
        Uncertainty Vector를 평가하여 SystemState 반환
        """
        state = SystemState()
        
        # Watermark 업데이트
        if uncertainty.timestamp > self.max_event_time:
            self.max_event_time = uncertainty.timestamp
        
        # 1. Data Trust 평가 (Freshness + Integrity/Sanitization)
        self._evaluate_data_trust(uncertainty, state)
        
        # 2. Hypothesis 평가 (Stability - z-score 기반)
        self._evaluate_hypothesis(uncertainty, orderbook_spread_bps, state)
        
        # 3. Decision 결정
        self._determine_decision(state)
        
        return state
    
    def _evaluate_data_trust(self, uncertainty: UncertaintyVector, state: SystemState):
        """
        Data Trust 평가
        
        1. Freshness (Latency, Stale ratio)
        2. Integrity / Sanitization Policy
           - 음수 latency → QUARANTINE
           - Watermark 이전 이벤트 → QUARANTINE
           - Crossed market + high deviation → QUARANTINE
           - Imbalance-Funding 불일치 → REPAIR 또는 QUARANTINE
        """
        trust = DataTrustState.TRUSTED
        reasons = []
        sanitization = SanitizationState.ACCEPT
        
        # === Freshness 평가 ===
        freshness = uncertainty.freshness
        
        # 음수 latency 체크 → 즉시 QUARANTINE
        if freshness.avg_lateness_ms < 0:
            trust = DataTrustState.UNTRUSTED
            sanitization = SanitizationState.QUARANTINE
            reasons.append(f"negative_latency:{freshness.avg_lateness_ms:.1f}ms")
            state.data_trust = trust
            state.trust_reasons = reasons
            state.sanitization = sanitization
            return
        
        # Watermark 체크 (이벤트가 watermark보다 오래됨)
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
        
        # Latency 체크
        if freshness.avg_lateness_ms > self.th.freshness_degraded_latency_ms:
            trust = DataTrustState.UNTRUSTED
            reasons.append(f"latency_high:{freshness.avg_lateness_ms:.1f}ms")
        elif freshness.avg_lateness_ms > self.th.freshness_trusted_latency_ms:
            if trust == DataTrustState.TRUSTED:
                trust = DataTrustState.DEGRADED
            reasons.append(f"latency_elevated:{freshness.avg_lateness_ms:.1f}ms")
        
        # Late event 체크 (allowed lateness 초과)
        if freshness.max_lateness_ms > self.th.allowed_lateness_ms:
            if trust == DataTrustState.TRUSTED:
                trust = DataTrustState.DEGRADED
            reasons.append(f"late_event:{freshness.max_lateness_ms:.1f}ms")
        
        # Stale ratio 체크
        if freshness.stale_event_ratio > self.th.freshness_degraded_stale_ratio:
            trust = DataTrustState.UNTRUSTED
            reasons.append(f"stale_ratio_high:{freshness.stale_event_ratio:.2%}")
        elif freshness.stale_event_ratio > self.th.freshness_trusted_stale_ratio:
            if trust == DataTrustState.TRUSTED:
                trust = DataTrustState.DEGRADED
            reasons.append(f"stale_ratio_elevated:{freshness.stale_event_ratio:.2%}")
        
        # === Integrity / Sanitization Policy 평가 ===
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
        Sanitization Policy 분류 (강화 버전)
        
        ACCEPT: 정상 데이터
        REPAIR: 수정 가능 (minor issue)
        QUARANTINE: 신뢰 불가 → UNTRUSTED
        
        QUARANTINE 조건:
        1. Crossed market + high deviation
        2. Imbalance-Funding 방향 불일치 (strict mode)
        """
        # 정상 케이스
        if integrity.spread_valid and integrity.price_in_spread:
            # Imbalance-Funding 체크 (정상 spread여도)
            if integrity.imbalance_funding_mismatch:
                if self.th.imbalance_funding_strict:
                    reasons.append("imbalance_funding_mismatch")
                    return SanitizationState.QUARANTINE
                else:
                    reasons.append("imbalance_funding_mismatch_warning")
                    return SanitizationState.REPAIR
            return SanitizationState.ACCEPT
        
        # Crossed market
        if not integrity.spread_valid:
            if integrity.price_deviation_bps >= self.th.integrity_repair_threshold_bps:
                reasons.append(f"crossed_market:deviation_{integrity.price_deviation_bps:.1f}bps")
                return SanitizationState.QUARANTINE
            else:
                reasons.append(f"crossed_market_minor:deviation_{integrity.price_deviation_bps:.1f}bps")
                return SanitizationState.REPAIR
        
        # spread_valid=T but price_in_spread=F
        if integrity.price_deviation_bps >= self.th.integrity_repair_threshold_bps * 2:
            reasons.append(f"price_outside_spread:deviation_{integrity.price_deviation_bps:.1f}bps")
            return SanitizationState.QUARANTINE
        
        reasons.append(f"price_outside_spread_minor:deviation_{integrity.price_deviation_bps:.1f}bps")
        return SanitizationState.REPAIR
    
    def _evaluate_hypothesis(self, 
                             uncertainty: UncertaintyVector,
                             spread_bps: Optional[float],
                             state: SystemState):
        """
        Hypothesis Validity 평가 (v2)
        
        핵심 질문: "이 trade가 현재 시장에서 발생 가능한가?"
        
        z-score 기반 Spread Deviation으로 판단
        - z = (current_spread - normal_mean) / normal_std
        - z <= 2.0 → VALID (95% 신뢰구간)
        - z <= 3.0 → WEAKENING (99% 신뢰구간)
        - z > 3.0 → INVALID
        
        ※ Liquidation cooldown 제거됨 (Orderbook Health로 대체 예정)
        """
        hypothesis = HypothesisState.VALID
        reasons = []
        
        stability = uncertainty.stability
        
        # === z-score 기반 Spread Deviation 평가 ===
        if spread_bps is not None and self.th.normal_spread_std_bps > 0:
            z_score = (spread_bps - self.th.normal_spread_mean_bps) / self.th.normal_spread_std_bps
            
            if z_score > self.th.stability_weakening_zscore:
                hypothesis = HypothesisState.INVALID
                reasons.append(f"spread_zscore_high:{z_score:.2f}")
            elif z_score > self.th.stability_valid_zscore:
                hypothesis = HypothesisState.WEAKENING
                reasons.append(f"spread_zscore_elevated:{z_score:.2f}")
        
        # === Spread Volatility (변동성) 평가 ===
        # 최근 spread들의 변동성 (CV)을 z-score로 변환
        if stability.spread_volatility_zscore is not None:
            vol_z = stability.spread_volatility_zscore
            
            if vol_z > self.th.stability_weakening_zscore:
                hypothesis = HypothesisState.INVALID
                reasons.append(f"volatility_zscore_high:{vol_z:.2f}")
            elif vol_z > self.th.stability_valid_zscore:
                if hypothesis == HypothesisState.VALID:
                    hypothesis = HypothesisState.WEAKENING
                reasons.append(f"volatility_zscore_elevated:{vol_z:.2f}")
        
        # === (참고) Liquidation 정보는 로깅만 ===
        # Orderbook Health에서 처리 예정
        if stability.time_since_liquidation_ms is not None:
            time_since = stability.time_since_liquidation_ms
            if time_since < 5000:  # 5초 이내
                reasons.append(f"recent_liquidation:{time_since:.0f}ms_ago")
        
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