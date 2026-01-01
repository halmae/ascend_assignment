"""
Processor - 통합 Decision Engine (v5 - Price Volatility)

================================================================================
v5 변경사항:
- AR(1) 제거 (spread 변동이 너무 작아 무의미)
- Price Volatility (window=75) 기반 Stability
- EDA 결과: Cohen's d = 0.537 (Medium effect)
================================================================================
"""
import json
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, TextIO
from dataclasses import dataclass, field

from src.config import THRESHOLDS, get_thresholds_dict
from src.enums import EventType, DecisionPermissionState, SanitizationState
from src.data_types import Event, OrderbookState
from src.uncertainty import (
    UncertaintyVector,
    FreshnessUncertainty,
    IntegrityUncertainty,
    StabilityUncertainty,
    VolatilityCalculator,
    VolatilityDiagnostics,
)
from src.state_machine import StateEvaluator, SystemState
from src.consistency import ConsistencyChecker


@dataclass
class ProcessingResult:
    """처리 결과"""
    mode: str = ""
    processing_time_sec: float = 0.0
    
    stats: Dict[str, int] = field(default_factory=dict)
    decision_counts: Dict[str, int] = field(default_factory=lambda: {
        'ALLOWED': 0, 'RESTRICTED': 0, 'HALTED': 0
    })
    sanitization_counts: Dict[str, int] = field(default_factory=lambda: {
        'ACCEPT': 0, 'REPAIR': 0, 'QUARANTINE': 0
    })
    
    state_transitions_count: int = 0
    decisions_count: int = 0
    
    @property
    def total_decisions(self) -> int:
        return sum(self.decision_counts.values())
    
    @property
    def allowed_rate(self) -> float:
        total = self.total_decisions
        return self.decision_counts['ALLOWED'] / total if total > 0 else 0


class Processor:
    """
    통합 Decision Engine (v5 - Price Volatility)
    
    핵심 변경:
    - VolatilityCalculator로 price dynamics 모델링
    - Rolling std of returns 기반 stability 판단
    """
    
    def __init__(self, mode: str = "", output_dir: str = None):
        self.mode = mode
        self.th = THRESHOLDS
        
        # Output 디렉토리 설정
        self.output_dir = Path(output_dir) if output_dir else None
        
        # 실시간 로그 파일 핸들
        self._transitions_file: Optional[TextIO] = None
        self._decisions_file: Optional[TextIO] = None
        self._liquidations_file: Optional[TextIO] = None
        
        if self.output_dir:
            self._init_log_files()
        
        # Core components
        self.state_evaluator = StateEvaluator()
        self.consistency_checker = ConsistencyChecker()
        
        # Orderbook
        self.orderbook = OrderbookState(
            timestamp=0,
            bid_levels={},
            ask_levels={}
        )
        
        # === Buffers ===
        self.latencies: deque = deque(maxlen=self.th.latency_window_size)
        
        # === Volatility Calculator for mid price ===
        self.volatility_calculator = VolatilityCalculator(
            window_size=self.th.volatility_window_size,
            min_samples=self.th.volatility_min_samples
        )
        
        # Out-of-order 추적
        self.last_event_time: int = 0
        self.out_of_order_count: int = 0
        self.late_event_count: int = 0
        
        # Liquidation 추적
        self.last_liquidation_ts: Optional[int] = None
        self.last_liquidation_size: float = 0.0
        
        # State
        self.current_state = SystemState()
        self.current_ticker: Dict = {}
        
        # Stats
        self.stats = {
            'trades': 0,
            'orderbook_updates': 0,
            'tickers': 0,
            'liquidations': 0,
            'snapshots': 0,
            'out_of_order': 0,
            'late_events': 0,
        }
        
        self.decision_counts = {
            'ALLOWED': 0,
            'RESTRICTED': 0,
            'HALTED': 0,
        }
        
        self.sanitization_counts = {
            'ACCEPT': 0,
            'REPAIR': 0,
            'QUARANTINE': 0,
        }
        
        # Counters
        self.state_transitions_count = 0
        self.decisions_logged_count = 0
        
        # Timing
        self.start_time: Optional[datetime] = None
    
    def _init_log_files(self):
        """로그 파일 초기화"""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self._transitions_file = open(self.output_dir / "state_transitions.jsonl", 'w')
        self._decisions_file = open(self.output_dir / "decisions.jsonl", 'w')
        self._liquidations_file = open(self.output_dir / "liquidations.jsonl", 'w')
    
    def _log_transition(self, log: Dict):
        """State transition 실시간 기록"""
        self.state_transitions_count += 1
        if self._transitions_file:
            self._transitions_file.write(json.dumps(log) + '\n')
            self._transitions_file.flush()
    
    def _log_decision(self, log: Dict):
        """Decision 실시간 기록"""
        self.decisions_logged_count += 1
        if self._decisions_file:
            self._decisions_file.write(json.dumps(log) + '\n')
            self._decisions_file.flush()
    
    def _log_liquidation(self, log: Dict):
        """Liquidation 실시간 기록"""
        if self._liquidations_file:
            self._liquidations_file.write(json.dumps(log) + '\n')
            self._liquidations_file.flush()
    
    def process_event(self, event: Event) -> Optional[Dict]:
        """단일 이벤트 처리"""
        if self.start_time is None:
            self.start_time = datetime.now()
        
        # Out-of-order 체크
        if event.timestamp < self.last_event_time:
            self.out_of_order_count += 1
            self.stats['out_of_order'] += 1
        self.last_event_time = max(self.last_event_time, event.timestamp)
        
        if event.event_type == EventType.TRADE:
            self._process_trade(event)
            return None
        elif event.event_type == EventType.ORDERBOOK:
            self._process_orderbook(event)
            return None
        elif event.event_type == EventType.SNAPSHOT:
            self._process_snapshot(event)
            return None
        elif event.event_type == EventType.LIQUIDATION:
            return self._process_liquidation(event)
        elif event.event_type == EventType.TICKER:
            return self._process_ticker(event)
        
        return None
    
    def _process_trade(self, event: Event):
        """Trade 처리"""
        self.stats['trades'] += 1
        
        latency_ms = (event.local_timestamp - event.timestamp) / 1000.0
        
        if latency_ms > self.th.allowed_lateness_ms:
            self.late_event_count += 1
            self.stats['late_events'] += 1
        
        if latency_ms > 0:
            self.latencies.append(latency_ms)
    
    def _process_orderbook(self, event: Event):
        """Orderbook Update 처리"""
        self.stats['orderbook_updates'] += 1
        
        latency_ms = (event.local_timestamp - event.timestamp) / 1000.0
        if latency_ms > self.th.allowed_lateness_ms:
            self.late_event_count += 1
            self.stats['late_events'] += 1
        
        if latency_ms > 0:
            self.latencies.append(latency_ms)
        
        data = event.data
        for level in data.get('bids', []):
            price, qty = float(level[0]), float(level[1])
            if qty == 0:
                self.orderbook.bid_levels.pop(price, None)
            else:
                self.orderbook.bid_levels[price] = qty
        
        for level in data.get('asks', []):
            price, qty = float(level[0]), float(level[1])
            if qty == 0:
                self.orderbook.ask_levels.pop(price, None)
            else:
                self.orderbook.ask_levels[price] = qty
        
        self.orderbook.timestamp = event.timestamp
        
        # Mid price 업데이트 → Volatility 계산
        self._update_volatility()
    
    def _process_snapshot(self, event: Event):
        """Snapshot 처리"""
        self.stats['snapshots'] += 1
        
        data = event.data
        self.orderbook = OrderbookState(
            timestamp=event.timestamp,
            bid_levels={float(p): float(q) for p, q in data.get('bids', [])},
            ask_levels={float(p): float(q) for p, q in data.get('asks', [])}
        )
        
        # Volatility 리셋 (새 snapshot이면 연속성 끊김)
        self.volatility_calculator.reset()
    
    def _process_liquidation(self, event: Event) -> Dict:
        """Liquidation 처리"""
        self.stats['liquidations'] += 1
        
        data = event.data
        self.last_liquidation_ts = event.timestamp
        self.last_liquidation_size = float(data.get('quantity', 0))
        
        liq_event = {
            'timestamp': event.timestamp,
            'side': data.get('side'),
            'quantity': self.last_liquidation_size,
            'price': float(data.get('price', 0)),
        }
        
        self._log_liquidation(liq_event)
        
        return {'type': 'LIQUIDATION', **liq_event}
    
    def _process_ticker(self, event: Event) -> Dict:
        """Ticker 처리 - Checkpoint"""
        self.stats['tickers'] += 1
        self.current_ticker = event.data
        
        if not self.orderbook.bid_levels or not self.orderbook.ask_levels:
            return {'type': 'SKIP', 'reason': 'orderbook_not_initialized'}
        
        # 이전 상태 저장
        prev_state = SystemState(
            data_trust=self.current_state.data_trust,
            hypothesis=self.current_state.hypothesis,
        )
        
        # Uncertainty 계산
        freshness = self._calculate_freshness()
        integrity = self._calculate_integrity()
        stability = self._calculate_stability(event.timestamp)
        
        uncertainty = UncertaintyVector(
            freshness=freshness,
            integrity=integrity,
            stability=stability,
            timestamp=event.timestamp
        )
        
        # State Machine 평가
        spread_bps = self.orderbook.get_spread_bps()
        self.current_state = self.state_evaluator.evaluate(
            uncertainty=uncertainty,
            orderbook_spread_bps=spread_bps
        )
        
        decision = self.current_state.decision
        self.decision_counts[decision.value] += 1
        self.sanitization_counts[self.current_state.sanitization.value] += 1
        
        # Uncertainty Snapshot (Volatility 정보 포함)
        vol_diag = stability.volatility
        uncertainty_snapshot = {
            'freshness': {
                'avg_latency_ms': round(freshness.avg_lateness_ms, 2),
                'max_latency_ms': round(freshness.max_lateness_ms, 2),
                'stale_ratio': round(freshness.stale_event_ratio, 4),
            },
            'integrity': {
                'spread_valid': integrity.spread_valid,
                'price_in_spread': integrity.price_in_spread,
                'deviation_bps': round(integrity.price_deviation_bps, 2),
                'sanitization': self.current_state.sanitization.value,
            },
            'stability': {
                # Price Volatility
                'volatility_bps': round(vol_diag.volatility, 4) if vol_diag.volatility is not None else None,
                'volatility_n_samples': vol_diag.n_samples,
                # Liquidation
                'time_since_liq_ms': round(stability.time_since_liquidation_ms, 0) if stability.time_since_liquidation_ms else None,
            },
            'spread_bps': round(spread_bps, 4) if spread_bps else None,
        }
        
        # State 전이 감지 → 파일 기록
        if (prev_state.data_trust != self.current_state.data_trust or
            prev_state.hypothesis != self.current_state.hypothesis):
            
            transition_log = {
                'ts': event.timestamp,
                'from': {
                    'data_trust': prev_state.data_trust.value,
                    'hypothesis': prev_state.hypothesis.value,
                },
                'to': {
                    'data_trust': self.current_state.data_trust.value,
                    'hypothesis': self.current_state.hypothesis.value,
                    'decision': decision.value,
                },
                'trigger': {
                    'trust_reasons': self.current_state.trust_reasons,
                    'hypothesis_reasons': self.current_state.hypothesis_reasons,
                },
                'uncertainty': uncertainty_snapshot,
            }
            self._log_transition(transition_log)
        
        # HALT/RESTRICT → 파일 기록
        if decision in [DecisionPermissionState.HALTED, DecisionPermissionState.RESTRICTED]:
            decision_log = {
                'ts': event.timestamp,
                'action': decision.value,
                'data_trust': self.current_state.data_trust.value,
                'hypothesis': self.current_state.hypothesis.value,
                'sanitization': self.current_state.sanitization.value,
                'reasons': {
                    'trust': self.current_state.trust_reasons,
                    'hypothesis': self.current_state.hypothesis_reasons,
                },
                'uncertainty': uncertainty_snapshot,
            }
            self._log_decision(decision_log)
        
        return {
            'type': 'DECISION',
            'decision': decision.value,
        }
    
    def _calculate_freshness(self) -> FreshnessUncertainty:
        """Freshness 계산"""
        freshness = FreshnessUncertainty()
        
        if not self.latencies:
            return freshness
        
        latencies = list(self.latencies)
        freshness.avg_lateness_ms = sum(latencies) / len(latencies)
        freshness.max_lateness_ms = max(latencies)
        
        stale_count = sum(1 for l in latencies if l > self.th.allowed_lateness_ms)
        freshness.stale_event_ratio = stale_count / len(latencies)
        freshness.late_event_count = self.late_event_count
        freshness.out_of_order_count = self.out_of_order_count
        
        return freshness
    
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
        """
        Stability 계산 (Price Volatility 기반)
        """
        stability = StabilityUncertainty()
        
        # Volatility diagnostics
        stability.volatility = self.volatility_calculator.compute()
        
        # Liquidation 정보
        if self.last_liquidation_ts:
            time_since = (current_ts - self.last_liquidation_ts) / 1000.0
            stability.time_since_liquidation_ms = time_since
            stability.liquidation_size = self.last_liquidation_size
        
        return stability
    
    def _update_volatility(self):
        """Mid price로 Volatility 업데이트"""
        if not self.orderbook.bid_levels or not self.orderbook.ask_levels:
            return
        
        best_bid = self.orderbook.get_best_bid()
        best_ask = self.orderbook.get_best_ask()
        
        if best_bid is None or best_ask is None:
            return
        
        if best_bid >= best_ask:
            return  # Crossed market
        
        mid_price = (best_bid + best_ask) / 2
        self.volatility_calculator.update(mid_price)
    
    def close(self):
        """리소스 정리"""
        if self._transitions_file:
            self._transitions_file.close()
        if self._decisions_file:
            self._decisions_file.close()
        if self._liquidations_file:
            self._liquidations_file.close()
    
    def save_summary(self):
        """Summary 저장"""
        if not self.output_dir:
            return
        
        total = sum(self.decision_counts.values())
        elapsed = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        summary = {
            'mode': self.mode,
            'processing_time_sec': round(elapsed, 2),
            'thresholds': get_thresholds_dict(),
            'stats': self.stats,
            'decision_distribution': {
                'counts': self.decision_counts,
                'rates': {
                    k: round(v / total * 100, 2) if total > 0 else 0
                    for k, v in self.decision_counts.items()
                }
            },
            'sanitization_distribution': self.sanitization_counts,
            'state_transitions_count': self.state_transitions_count,
            'decisions_logged_count': self.decisions_logged_count,
        }
        
        with open(self.output_dir / "summary.json", 'w') as f:
            json.dump(summary, f, indent=2)
    
    def get_result(self) -> ProcessingResult:
        """처리 결과 반환"""
        elapsed = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        return ProcessingResult(
            mode=self.mode,
            processing_time_sec=elapsed,
            stats=self.stats.copy(),
            decision_counts=self.decision_counts.copy(),
            sanitization_counts=self.sanitization_counts.copy(),
            state_transitions_count=self.state_transitions_count,
            decisions_count=self.decisions_logged_count,
        )