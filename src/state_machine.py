"""
State Machine 모듈

과제 요구사항에 따른 3-State 구조:
1. Data Trust State: 데이터 품질/신뢰도
2. Hypothesis Validity State: 리서치 가설의 유효성
3. Decision Permission State: 위 두 상태의 조합 결과

Decision Permission = f(Data Trust, Hypothesis Validity)
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum

from src.enums import DataTrustState, HypothesisValidityState, DecisionPermissionState
from src.uncertainty import UncertaintyVector, FreshnessUncertainty, IntegrityUncertainty, StabilityUncertainty


@dataclass
class StateThresholds:
    """
    State 전이 임계값 설정
    
    이 값들은 Research 데이터 분석을 통해 조정해야 합니다.
    """
    # === Data Trust Thresholds ===
    # Freshness 기준
    freshness_trusted_max_avg_latency_ms: float = 20.0      # 평균 latency 20ms 이하 → TRUSTED
    freshness_degraded_max_avg_latency_ms: float = 50.0     # 50ms 이하 → DEGRADED, 초과 → UNTRUSTED
    freshness_trusted_max_stale_ratio: float = 0.05         # stale 비율 5% 이하 → TRUSTED
    freshness_degraded_max_stale_ratio: float = 0.15        # 15% 이하 → DEGRADED
    
    # Integrity 기준
    integrity_trusted_max_failure_rate: float = 0.02        # 실패율 2% 이하 → TRUSTED
    integrity_degraded_max_failure_rate: float = 0.10       # 10% 이하 → DEGRADED
    
    # === Hypothesis Validity Thresholds ===
    # Stability 기준 (Liquidation 후 안정성)
    stability_valid_max_spread_volatility: float = 0.05     # spread 변동성 5% 이하 → VALID
    stability_weakening_max_spread_volatility: float = 0.15 # 15% 이하 → WEAKENING
    
    # Liquidation cooldown
    liquidation_valid_min_time_ms: float = 5000.0           # 5초 이상 경과 → VALID
    liquidation_weakening_min_time_ms: float = 2000.0       # 2초 이상 → WEAKENING
    
    # Spread 기준 (crossed market 등)
    spread_valid_max_relative_bps: float = 10.0             # 10bp 이하 → VALID
    spread_weakening_max_relative_bps: float = 30.0         # 30bp 이하 → WEAKENING


@dataclass
class SystemState:
    """
    시스템의 현재 상태
    
    세 가지 상태를 모두 포함하며, Decision Permission은 자동 계산됨
    """
    data_trust: DataTrustState = DataTrustState.UNTRUSTED
    hypothesis: HypothesisValidityState = HypothesisValidityState.INVALID
    timestamp: int = 0
    
    # 상태 결정에 사용된 세부 정보 (추적성을 위해)
    trust_reasons: List[str] = field(default_factory=list)
    hypothesis_reasons: List[str] = field(default_factory=list)
    
    @property
    def decision(self) -> DecisionPermissionState:
        """
        Decision Permission State 계산
        
        Decision = f(Data Trust, Hypothesis Validity)
        """
        # UNTRUSTED 또는 INVALID → 무조건 HALTED
        if self.data_trust == DataTrustState.UNTRUSTED:
            return DecisionPermissionState.HALTED
        if self.hypothesis == HypothesisValidityState.INVALID:
            return DecisionPermissionState.HALTED
        
        # TRUSTED + VALID → ALLOWED
        if (self.data_trust == DataTrustState.TRUSTED and 
            self.hypothesis == HypothesisValidityState.VALID):
            return DecisionPermissionState.ALLOWED
        
        # 나머지 조합 → RESTRICTED
        return DecisionPermissionState.RESTRICTED
    
    def to_dict(self) -> Dict:
        """로깅용 딕셔너리 변환"""
        return {
            'timestamp': self.timestamp,
            'data_trust': self.data_trust.value,
            'hypothesis': self.hypothesis.value,
            'decision': self.decision.value,
            'trust_reasons': self.trust_reasons,
            'hypothesis_reasons': self.hypothesis_reasons
        }


class StateEvaluator:
    """
    Uncertainty Vector를 기반으로 System State를 평가하는 클래스
    
    핵심 역할:
    1. UncertaintyVector → DataTrustState 변환
    2. UncertaintyVector → HypothesisValidityState 변환
    3. 상태 전이 감지 및 로깅
    """
    
    def __init__(self, thresholds: Optional[StateThresholds] = None):
        self.thresholds = thresholds or StateThresholds()
        self.current_state = SystemState()
        self.previous_state: Optional[SystemState] = None
        
        # 상태 전이 이력
        self.state_history: List[Dict] = []
    
    def evaluate(self, 
                 uncertainty: UncertaintyVector,
                 orderbook_spread_bps: Optional[float] = None) -> SystemState:
        """
        Uncertainty Vector를 평가하여 System State 반환
        
        Args:
            uncertainty: 현재 Uncertainty Vector
            orderbook_spread_bps: 현재 Orderbook의 spread (basis points)
        
        Returns:
            SystemState: 평가된 시스템 상태
        """
        # 이전 상태 저장
        self.previous_state = SystemState(
            data_trust=self.current_state.data_trust,
            hypothesis=self.current_state.hypothesis,
            timestamp=self.current_state.timestamp,
            trust_reasons=self.current_state.trust_reasons.copy(),
            hypothesis_reasons=self.current_state.hypothesis_reasons.copy()
        )
        
        # 1. Data Trust State 평가
        data_trust, trust_reasons = self._evaluate_data_trust(uncertainty)
        
        # 2. Hypothesis Validity State 평가
        hypothesis, hypo_reasons = self._evaluate_hypothesis(uncertainty, orderbook_spread_bps)
        
        # 3. 새 상태 생성
        new_state = SystemState(
            data_trust=data_trust,
            hypothesis=hypothesis,
            timestamp=uncertainty.timestamp,
            trust_reasons=trust_reasons,
            hypothesis_reasons=hypo_reasons
        )
        
        # 4. 상태 전이 감지
        if self._has_state_changed(new_state):
            self._record_transition(new_state)
        
        self.current_state = new_state
        return new_state
    
    def _evaluate_data_trust(self, 
                             uncertainty: UncertaintyVector) -> Tuple[DataTrustState, List[str]]:
        """
        Data Trust State 평가
        
        Data Trust는 "데이터를 얼마나 신뢰할 수 있는가?"를 나타냄
        - Freshness (데이터 신선도)
        - Integrity (데이터 일관성)
        """
        th = self.thresholds
        reasons = []
        
        # === Freshness 평가 ===
        freshness = uncertainty.freshness
        freshness_score = self._score_freshness(freshness)
        
        if freshness_score == 'UNTRUSTED':
            reasons.append(f"freshness_critical: avg_latency={freshness.avg_lateness_ms:.1f}ms, "
                          f"stale_ratio={freshness.stale_event_ratio:.2%}")
        elif freshness_score == 'DEGRADED':
            reasons.append(f"freshness_degraded: avg_latency={freshness.avg_lateness_ms:.1f}ms")
        
        # === Integrity 평가 ===
        integrity = uncertainty.integrity
        integrity_score = self._score_integrity(integrity)
        
        if integrity_score == 'UNTRUSTED':
            reasons.append(f"integrity_critical: spread_valid={integrity.spread_valid}, "
                          f"failure_rate={integrity.spread_valid_failure_rate:.2%}")
        elif integrity_score == 'DEGRADED':
            reasons.append(f"integrity_degraded: failure_rate={integrity.spread_valid_failure_rate:.2%}")
        
        # === 최종 Data Trust 결정 ===
        # 가장 나쁜 점수 기준 (보수적 접근)
        scores = [freshness_score, integrity_score]
        
        if 'UNTRUSTED' in scores:
            return DataTrustState.UNTRUSTED, reasons
        elif 'DEGRADED' in scores:
            return DataTrustState.DEGRADED, reasons
        else:
            return DataTrustState.TRUSTED, ["all_checks_passed"]
    
    def _score_freshness(self, freshness: FreshnessUncertainty) -> str:
        """Freshness 점수화"""
        th = self.thresholds
        
        # stale ratio 체크 (더 중요)
        if freshness.stale_event_ratio > th.freshness_degraded_max_stale_ratio:
            return 'UNTRUSTED'
        
        # average latency 체크
        if freshness.avg_lateness_ms > th.freshness_degraded_max_avg_latency_ms:
            return 'UNTRUSTED'
        elif freshness.avg_lateness_ms > th.freshness_trusted_max_avg_latency_ms:
            return 'DEGRADED'
        elif freshness.stale_event_ratio > th.freshness_trusted_max_stale_ratio:
            return 'DEGRADED'
        
        return 'TRUSTED'
    
    def _score_integrity(self, integrity: IntegrityUncertainty) -> str:
        """Integrity 점수화"""
        th = self.thresholds
        
        # Crossed market은 즉시 UNTRUSTED
        if not integrity.spread_valid:
            return 'UNTRUSTED'
        
        # Failure rate 체크
        if integrity.spread_valid_failure_rate > th.integrity_degraded_max_failure_rate:
            return 'UNTRUSTED'
        elif integrity.spread_valid_failure_rate > th.integrity_trusted_max_failure_rate:
            return 'DEGRADED'
        
        return 'TRUSTED'
    
    def _evaluate_hypothesis(self,
                             uncertainty: UncertaintyVector,
                             spread_bps: Optional[float] = None) -> Tuple[HypothesisValidityState, List[str]]:
        """
        Hypothesis Validity State 평가
        
        Hypothesis는 "Liquidation 이후 Orderbook이 안정적인가?"라는 
        리서치 가설의 유효성을 나타냄
        - Stability (Orderbook 안정성)
        - Post-Liquidation 상태
        """
        th = self.thresholds
        reasons = []
        
        stability = uncertainty.stability
        
        # === Post-Liquidation 안정성 평가 ===
        liquidation_score = self._score_liquidation_stability(stability)
        
        if liquidation_score == 'INVALID':
            reasons.append(f"post_liquidation_unstable: time_since={stability.time_since_liquidation_ms}ms, "
                          f"required={th.liquidation_weakening_min_time_ms}ms")
        elif liquidation_score == 'WEAKENING':
            reasons.append(f"post_liquidation_recovering: time_since={stability.time_since_liquidation_ms}ms")
        
        # === Spread Volatility 평가 ===
        volatility_score = self._score_spread_volatility(stability)
        
        if volatility_score == 'INVALID':
            reasons.append(f"spread_volatility_high: {stability.spread_volatility:.4f}")
        elif volatility_score == 'WEAKENING':
            reasons.append(f"spread_volatility_elevated: {stability.spread_volatility:.4f}")
        
        # === 최종 Hypothesis 결정 ===
        scores = [liquidation_score, volatility_score]
        
        if 'INVALID' in scores:
            return HypothesisValidityState.INVALID, reasons
        elif 'WEAKENING' in scores:
            return HypothesisValidityState.WEAKENING, reasons
        else:
            return HypothesisValidityState.VALID, ["hypothesis_conditions_met"]
    
    def _score_liquidation_stability(self, stability: StabilityUncertainty) -> str:
        """Liquidation 후 안정성 점수화"""
        th = self.thresholds
        
        # Liquidation이 없었으면 VALID
        if stability.time_since_liquidation_ms is None:
            return 'VALID'
        
        time_since = stability.time_since_liquidation_ms
        
        if time_since < th.liquidation_weakening_min_time_ms:
            return 'INVALID'
        elif time_since < th.liquidation_valid_min_time_ms:
            return 'WEAKENING'
        else:
            return 'VALID'
    
    def _score_spread_volatility(self, stability: StabilityUncertainty) -> str:
        """Spread 변동성 점수화"""
        th = self.thresholds
        
        vol = stability.spread_volatility
        
        if vol > th.stability_weakening_max_spread_volatility:
            return 'INVALID'
        elif vol > th.stability_valid_max_spread_volatility:
            return 'WEAKENING'
        else:
            return 'VALID'
    
    def _has_state_changed(self, new_state: SystemState) -> bool:
        """상태 전이 감지"""
        if self.previous_state is None:
            return True
        
        return (new_state.data_trust != self.previous_state.data_trust or
                new_state.hypothesis != self.previous_state.hypothesis)
    
    def _record_transition(self, new_state: SystemState):
        """상태 전이 기록"""
        transition = {
            'ts': new_state.timestamp,
            'from': {
                'data_trust': self.previous_state.data_trust.value if self.previous_state else None,
                'hypothesis': self.previous_state.hypothesis.value if self.previous_state else None,
                'decision': self.previous_state.decision.value if self.previous_state else None
            },
            'to': {
                'data_trust': new_state.data_trust.value,
                'hypothesis': new_state.hypothesis.value,
                'decision': new_state.decision.value
            },
            'trigger': {
                'trust_reasons': new_state.trust_reasons,
                'hypothesis_reasons': new_state.hypothesis_reasons
            }
        }
        self.state_history.append(transition)
    
    def get_state_summary(self) -> Dict:
        """상태 요약 반환"""
        return {
            'current_state': self.current_state.to_dict(),
            'total_transitions': len(self.state_history),
            'state_history': self.state_history
        }