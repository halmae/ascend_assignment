"""
Stream Processor - Historical 데이터 처리

================================================================================
Single Decision Engine:
- config.py의 THRESHOLDS를 참조
- StateEvaluator와 동일한 파라미터 사용
================================================================================
"""
import time
from collections import deque
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field

from src.config import THRESHOLDS
from src.enums import EventType, DecisionPermissionState
from src.data_types import Event, OrderbookState
from src.uncertainty import (
    UncertaintyVector, 
    FreshnessUncertainty, 
    IntegrityUncertainty, 
    StabilityUncertainty
)
from src.state_machine import StateEvaluator, SystemState
from src.consistency import ConsistencyChecker


@dataclass 
class ProcessingResult:
    """처리 결과"""
    dataset_name: str = ""
    processing_time_sec: float = 0.0
    
    # 통계
    stats: Dict[str, int] = field(default_factory=dict)
    
    # Decision 분포
    decision_counts: Dict[str, int] = field(default_factory=lambda: {
        'ALLOWED': 0, 'RESTRICTED': 0, 'HALTED': 0
    })
    
    # 로그
    state_transitions: List[Dict] = field(default_factory=list)
    decisions_log: List[Dict] = field(default_factory=list)
    
    @property
    def total_decisions(self) -> int:
        return sum(self.decision_counts.values())
    
    @property
    def allowed_rate(self) -> float:
        total = self.total_decisions
        return self.decision_counts['ALLOWED'] / total if total > 0 else 0
    
    @property
    def halted_rate(self) -> float:
        total = self.total_decisions
        return self.decision_counts['HALTED'] / total if total > 0 else 0


class StreamProcessor:
    """
    Historical 스트림 처리기
    
    CSV 파일에서 읽은 이벤트를 처리하여 Decision 생성
    """
    
    def __init__(self, dataset_name: str = ""):
        self.dataset_name = dataset_name
        
        # config.py 참조
        self.th = THRESHOLDS
        
        # Core components
        self.state_evaluator = StateEvaluator()
        self.consistency_checker = ConsistencyChecker()
        
        # Orderbook 상태
        self.orderbook = OrderbookState(
            timestamp=0,
            bid_levels={},
            ask_levels={}
        )
        
        # Latency 추적
        self.latencies: deque = deque(maxlen=self.th.latency_window_size)
        
        # Spread history
        self.spreads: deque = deque(maxlen=self.th.spread_history_size)
        
        # Liquidation 추적
        self.last_liquidation_ts: Optional[int] = None
        self.last_liquidation_size: float = 0.0
        
        # 현재 상태
        self.current_state = SystemState()
        self.current_ticker: Dict = {}
        
        # 통계
        self.stats = {
            'trades': 0,
            'orderbook_updates': 0,
            'tickers': 0,
            'liquidations': 0,
            'snapshots': 0,
        }
        
        self.decision_counts = {
            'ALLOWED': 0,
            'RESTRICTED': 0,
            'HALTED': 0,
        }
        
        # 로그
        self.state_transitions: List[Dict] = []
        self.decisions_log: List[Dict] = []
        
        # 타이밍
        self.start_time: Optional[float] = None
    
    def process_event(self, event: Event):
        """이벤트 처리"""
        if self.start_time is None:
            self.start_time = time.time()
        
        if event.event_type == EventType.TRADE:
            self._process_trade(event)
        elif event.event_type == EventType.ORDERBOOK:
            self._process_orderbook(event)
        elif event.event_type == EventType.TICKER:
            self._process_ticker(event)
        elif event.event_type == EventType.LIQUIDATION:
            self._process_liquidation(event)
        elif event.event_type == EventType.SNAPSHOT:
            self._process_snapshot(event)
    
    def _process_trade(self, event: Event):
        """Trade 처리"""
        self.stats['trades'] += 1
        
        # Latency 계산
        latency_ms = (event.local_timestamp - event.timestamp) / 1000.0
        if latency_ms > 0:
            self.latencies.append(latency_ms)
    
    def _process_orderbook(self, event: Event):
        """Orderbook 업데이트 처리"""
        self.stats['orderbook_updates'] += 1
        
        data = event.data
        
        # Latency
        latency_ms = (event.local_timestamp - event.timestamp) / 1000.0
        if latency_ms > 0:
            self.latencies.append(latency_ms)
        
        # Orderbook 업데이트
        for price, qty in data.get('bids', []):
            if qty == 0:
                self.orderbook.bid_levels.pop(float(price), None)
            else:
                self.orderbook.bid_levels[float(price)] = float(qty)
        
        for price, qty in data.get('asks', []):
            if qty == 0:
                self.orderbook.ask_levels.pop(float(price), None)
            else:
                self.orderbook.ask_levels[float(price)] = float(qty)
        
        self.orderbook.timestamp = event.timestamp
    
    def _process_liquidation(self, event: Event):
        """Liquidation 처리"""
        self.stats['liquidations'] += 1
        
        data = event.data
        self.last_liquidation_ts = event.timestamp
        self.last_liquidation_size = float(data.get('quantity', 0))
    
    def _process_snapshot(self, event: Event):
        """Snapshot 처리 (Orderbook 초기화)"""
        self.stats['snapshots'] += 1
        
        data = event.data
        self.orderbook = OrderbookState(
            timestamp=event.timestamp,
            bid_levels={float(p): float(q) for p, q in data.get('bids', [])},
            ask_levels={float(p): float(q) for p, q in data.get('asks', [])}
        )
    
    def _process_ticker(self, event: Event):
        """
        Ticker 처리 - Checkpoint
        
        이 시점에서 Uncertainty 계산 → State Machine 평가
        """
        self.stats['tickers'] += 1
        self.current_ticker = event.data
        
        # Orderbook이 없으면 스킵
        if not self.orderbook.bid_levels or not self.orderbook.ask_levels:
            return
        
        # 이전 상태 저장
        prev_state = SystemState(
            data_trust=self.current_state.data_trust,
            hypothesis=self.current_state.hypothesis,
        )
        
        # 1. Uncertainty 계산
        freshness = self._calculate_freshness()
        integrity = self._calculate_integrity()
        stability = self._calculate_stability(event.timestamp)
        
        uncertainty = UncertaintyVector(
            freshness=freshness,
            integrity=integrity,
            stability=stability,
            timestamp=event.timestamp
        )
        
        # 2. Spread history 업데이트
        self._update_spread_history()
        
        # 3. State Machine 평가
        spread_bps = self.orderbook.get_spread_bps()
        self.current_state = self.state_evaluator.evaluate(
            uncertainty=uncertainty,
            orderbook_spread_bps=spread_bps
        )
        
        # 4. Decision 카운트
        decision = self.current_state.decision
        self.decision_counts[decision.value] += 1
        
        # 5. State 전이 감지
        if (prev_state.data_trust != self.current_state.data_trust or
            prev_state.hypothesis != self.current_state.hypothesis):
            
            self.state_transitions.append({
                'ts': event.timestamp,
                'data_trust': self.current_state.data_trust.value,
                'hypothesis': self.current_state.hypothesis.value,
                'decision': decision.value,
                'trigger': {
                    'from_trust': prev_state.data_trust.value,
                    'from_hypothesis': prev_state.hypothesis.value,
                    'trust_reasons': self.current_state.trust_reasons,
                    'hypothesis_reasons': self.current_state.hypothesis_reasons,
                }
            })
        
        # 6. HALT/RESTRICT 로깅
        if decision in [DecisionPermissionState.HALTED, DecisionPermissionState.RESTRICTED]:
            self.decisions_log.append({
                'ts': event.timestamp,
                'action': decision.value,
                'reason': {
                    'data_trust': self.current_state.data_trust.value,
                    'hypothesis': self.current_state.hypothesis.value,
                    'trust_reasons': self.current_state.trust_reasons,
                    'hypothesis_reasons': self.current_state.hypothesis_reasons,
                }
            })
    
    def _calculate_freshness(self) -> FreshnessUncertainty:
        """Freshness 계산"""
        if not self.latencies:
            return FreshnessUncertainty()
        
        latencies = list(self.latencies)
        avg = sum(latencies) / len(latencies)
        max_lat = max(latencies)
        
        # config.py의 threshold 사용
        stale_count = sum(1 for l in latencies if l > self.th.stale_threshold_ms)
        stale_ratio = stale_count / len(latencies)
        
        return FreshnessUncertainty(
            avg_lateness_ms=avg,
            max_lateness_ms=max_lat,
            stale_event_ratio=stale_ratio
        )
    
    def _calculate_integrity(self) -> IntegrityUncertainty:
        """Integrity 계산"""
        ticker_dict = {
            'last_price': float(self.current_ticker.get('last_price', 0)),
            'funding_rate': self.current_ticker.get('funding_rate'),
        }
        return self.consistency_checker.check_integrity(
            ticker_data=ticker_dict,
            orderbook=self.orderbook
        )
    
    def _calculate_stability(self, current_ts: int) -> StabilityUncertainty:
        """Stability 계산"""
        stability = StabilityUncertainty()
        
        # Spread volatility (CV)
        if len(self.spreads) >= 2:
            spreads = list(self.spreads)
            avg = sum(spreads) / len(spreads)
            if avg > 0:
                variance = sum((s - avg) ** 2 for s in spreads) / len(spreads)
                stability.spread_volatility = (variance ** 0.5) / avg
        
        # Liquidation cooldown
        if self.last_liquidation_ts:
            time_since = (current_ts - self.last_liquidation_ts) / 1000.0  # ms
            stability.time_since_liquidation_ms = time_since
            stability.liquidation_size = self.last_liquidation_size
            stability.post_liquidation_stable = time_since >= self.th.liquidation_cooldown_ms
        
        return stability
    
    def _update_spread_history(self):
        """Spread history 업데이트"""
        spread = self.orderbook.get_spread()
        if spread is not None and spread > 0:
            self.spreads.append(spread)
    
    def get_result(self) -> ProcessingResult:
        """처리 결과 반환"""
        return ProcessingResult(
            dataset_name=self.dataset_name,
            processing_time_sec=time.time() - self.start_time if self.start_time else 0,
            stats=self.stats.copy(),
            decision_counts=self.decision_counts.copy(),
            state_transitions=self.state_transitions,
            decisions_log=self.decisions_log,
        )
    
    def print_summary(self):
        """결과 요약 출력"""
        total = sum(self.decision_counts.values())
        if total == 0:
            print("No decisions made")
            return
        
        print(f"\n{'='*60}")
        print(f"📊 {self.dataset_name} 결과")
        print(f"{'='*60}")
        
        for decision, count in self.decision_counts.items():
            pct = count / total * 100
            print(f"  {decision}: {count:,} ({pct:.1f}%)")
        
        print(f"\n  State Transitions: {len(self.state_transitions)}")
        print(f"{'='*60}")