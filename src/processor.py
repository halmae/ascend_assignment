"""
Processor - 통합 Decision Engine (v2)

================================================================================
변경사항:
1. z-score 기반 Stability 계산
2. Time Alignment Policy 반영 (allowed_lateness, watermark)
3. Liquidation cooldown 제거
================================================================================
"""
import json
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, field

from src.config import THRESHOLDS, get_thresholds_dict
from src.enums import EventType, DecisionPermissionState, SanitizationState
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
    mode: str = ""
    processing_time_sec: float = 0.0
    
    stats: Dict[str, int] = field(default_factory=dict)
    decision_counts: Dict[str, int] = field(default_factory=lambda: {
        'ALLOWED': 0, 'RESTRICTED': 0, 'HALTED': 0
    })
    
    state_transitions: List[Dict] = field(default_factory=list)
    decisions_log: List[Dict] = field(default_factory=list)
    
    @property
    def total_decisions(self) -> int:
        return sum(self.decision_counts.values())
    
    @property
    def allowed_rate(self) -> float:
        total = self.total_decisions
        return self.decision_counts['ALLOWED'] / total if total > 0 else 0


class Processor:
    """
    통합 Decision Engine (v2)
    """
    
    def __init__(self, mode: str = ""):
        self.mode = mode
        self.th = THRESHOLDS
        
        # Core components
        self.state_evaluator = StateEvaluator()
        self.consistency_checker = ConsistencyChecker()
        
        # Orderbook
        self.orderbook = OrderbookState(
            timestamp=0,
            bid_levels={},
            ask_levels={}
        )
        
        # === Time Alignment Policy 관련 버퍼 ===
        self.latencies: deque = deque(maxlen=self.th.latency_window_size)
        self.spreads: deque = deque(maxlen=self.th.spread_history_size)
        
        # Out-of-order 추적
        self.last_event_time: int = 0
        self.out_of_order_count: int = 0
        self.late_event_count: int = 0
        
        # Liquidation 추적 (로깅용)
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
        
        # Logs
        self.state_transitions: List[Dict] = []
        self.decisions_log: List[Dict] = []
        self.liquidation_events: List[Dict] = []
        
        # Timing
        self.start_time: Optional[datetime] = None
        self.last_log_time: Optional[datetime] = None
    
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
        
        # Late event 체크
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
    
    def _process_snapshot(self, event: Event):
        """Snapshot 처리"""
        self.stats['snapshots'] += 1
        
        data = event.data
        self.orderbook = OrderbookState(
            timestamp=event.timestamp,
            bid_levels={float(p): float(q) for p, q in data.get('bids', [])},
            ask_levels={float(p): float(q) for p, q in data.get('asks', [])}
        )
    
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
        self.liquidation_events.append(liq_event)
        
        return {'type': 'LIQUIDATION', **liq_event}
    
    def _process_ticker(self, event: Event) -> Dict:
        """Ticker 처리 - Checkpoint"""
        self.stats['tickers'] += 1
        self.current_ticker = event.data
        
        if self.stats['tickers'] == 1:
            print("📊 첫 Ticker 수신! Decision Engine 가동 중...")
        
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
        
        self._update_spread_history()
        
        # State Machine 평가
        spread_bps = self.orderbook.get_spread_bps()
        self.current_state = self.state_evaluator.evaluate(
            uncertainty=uncertainty,
            orderbook_spread_bps=spread_bps
        )
        
        decision = self.current_state.decision
        self.decision_counts[decision.value] += 1
        self.sanitization_counts[self.current_state.sanitization.value] += 1
        
        # Uncertainty Snapshot
        uncertainty_snapshot = {
            'freshness': {
                'avg_latency_ms': round(freshness.avg_lateness_ms, 2),
                'max_latency_ms': round(freshness.max_lateness_ms, 2),
                'stale_ratio': round(freshness.stale_event_ratio, 4),
                'late_events': freshness.late_event_count,
                'out_of_order': freshness.out_of_order_count,
            },
            'integrity': {
                'spread_valid': integrity.spread_valid,
                'price_in_spread': integrity.price_in_spread,
                'price_deviation_bps': round(integrity.price_deviation_bps, 2),
                'sanitization': self.current_state.sanitization.value,
                'imbalance': round(integrity.orderbook_imbalance, 3),
                'imbalance_funding_mismatch': integrity.imbalance_funding_mismatch,
            },
            'stability': {
                'spread_volatility': round(stability.spread_volatility, 4),
                'spread_volatility_zscore': round(stability.spread_volatility_zscore, 2) if stability.spread_volatility_zscore else None,
                'time_since_liquidation_ms': round(stability.time_since_liquidation_ms, 0) if stability.time_since_liquidation_ms else None,
            },
            'spread_bps': round(spread_bps, 2) if spread_bps else None,
        }
        
        # State 전이 감지
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
            self.state_transitions.append(transition_log)
            self._print_state_transition(transition_log)
        
        # HALT/RESTRICT 로깅
        if decision in [DecisionPermissionState.HALTED, DecisionPermissionState.RESTRICTED]:
            decision_log = {
                'ts': event.timestamp,
                'action': decision.value,
                'data_trust': self.current_state.data_trust.value,
                'hypothesis': self.current_state.hypothesis.value,
                'sanitization': self.current_state.sanitization.value,
                'reasons': {
                    'trust_reasons': self.current_state.trust_reasons,
                    'hypothesis_reasons': self.current_state.hypothesis_reasons,
                },
                'uncertainty': uncertainty_snapshot,
            }
            self.decisions_log.append(decision_log)
        
        return {
            'type': 'DECISION',
            'decision': decision.value,
            'data_trust': self.current_state.data_trust.value,
            'hypothesis': self.current_state.hypothesis.value,
        }
    
    def _calculate_freshness(self) -> FreshnessUncertainty:
        """Freshness 계산"""
        freshness = FreshnessUncertainty()
        
        if not self.latencies:
            return freshness
        
        latencies = list(self.latencies)
        freshness.avg_lateness_ms = sum(latencies) / len(latencies)
        freshness.max_lateness_ms = max(latencies)
        
        # Stale ratio (allowed_lateness 초과 비율)
        stale_count = sum(1 for l in latencies if l > self.th.allowed_lateness_ms)
        freshness.stale_event_ratio = stale_count / len(latencies)
        
        # Late event / out-of-order count
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
        """Stability 계산 (z-score 기반)"""
        stability = StabilityUncertainty()
        
        # Spread volatility (CV)
        if len(self.spreads) >= 2:
            spreads = list(self.spreads)
            avg = sum(spreads) / len(spreads)
            if avg > 0:
                variance = sum((s - avg) ** 2 for s in spreads) / len(spreads)
                stability.spread_volatility = (variance ** 0.5) / avg
                
                # z-score 계산 (normal distribution 대비)
                if self.th.normal_spread_std_bps > 0:
                    # spread_volatility를 bps 단위로 변환하여 z-score 계산
                    # 여기서는 volatility 자체의 z-score를 계산
                    # TODO: 정상 volatility 분포 calibration 필요
                    stability.spread_volatility_zscore = stability.spread_volatility / 0.05  # 임시
        
        # Liquidation (로깅용)
        if self.last_liquidation_ts:
            time_since = (current_ts - self.last_liquidation_ts) / 1000.0
            stability.time_since_liquidation_ms = time_since
            stability.liquidation_size = self.last_liquidation_size
        
        return stability
    
    def _update_spread_history(self):
        """Spread 히스토리 업데이트"""
        spread = self.orderbook.get_spread()
        if spread is not None and spread > 0:
            self.spreads.append(spread)
    
    def _print_state_transition(self, log: Dict):
        """상태 전이 콘솔 출력"""
        from_state = log['from']
        to_state = log['to']
        trigger = log['trigger']
        
        print(f"\n  🔄 STATE TRANSITION")
        print(f"     Trust:      {from_state['data_trust']} → {to_state['data_trust']}")
        print(f"     Hypothesis: {from_state['hypothesis']} → {to_state['hypothesis']}")
        print(f"     Decision:   {to_state['decision']}")
        
        if trigger['trust_reasons']:
            print(f"     Trust Reasons: {', '.join(trigger['trust_reasons'])}")
        if trigger['hypothesis_reasons']:
            print(f"     Hypothesis Reasons: {', '.join(trigger['hypothesis_reasons'])}")
    
    def print_status(self):
        """현재 상태 출력"""
        total = sum(self.decision_counts.values())
        if total == 0:
            return
        
        allowed_pct = self.decision_counts['ALLOWED'] / total * 100
        halted_pct = self.decision_counts['HALTED'] / total * 100
        
        elapsed = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        print(f"\r[{elapsed:5.1f}s] Tickers: {self.stats['tickers']:>5} | "
              f"ALLOWED: {allowed_pct:5.1f}% | HALTED: {halted_pct:5.1f}% | "
              f"Liq: {self.stats['liquidations']}",
              end='', flush=True)
    
    def should_log_status(self, interval_sec: float = 10.0) -> bool:
        """상태 로그 출력 여부"""
        now = datetime.now()
        if self.last_log_time is None:
            self.last_log_time = now
            return True
        
        if (now - self.last_log_time).total_seconds() >= interval_sec:
            self.last_log_time = now
            return True
        return False
    
    def get_result(self) -> ProcessingResult:
        """처리 결과 반환"""
        elapsed = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        return ProcessingResult(
            mode=self.mode,
            processing_time_sec=elapsed,
            stats=self.stats.copy(),
            decision_counts=self.decision_counts.copy(),
            state_transitions=self.state_transitions,
            decisions_log=self.decisions_log,
        )
    
    def save_outputs(self, output_dir: str):
        """결과 저장"""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        print(f"\n\n📁 결과 저장: {output_path}")
        
        # state_transitions.jsonl
        with open(output_path / "state_transitions.jsonl", 'w') as f:
            for t in self.state_transitions:
                f.write(json.dumps(t) + '\n')
        print(f"  ✅ state_transitions.jsonl ({len(self.state_transitions)} records)")
        
        # decisions.jsonl
        with open(output_path / "decisions.jsonl", 'w') as f:
            for d in self.decisions_log:
                f.write(json.dumps(d) + '\n')
        print(f"  ✅ decisions.jsonl ({len(self.decisions_log)} records)")
        
        # summary.json
        total = sum(self.decision_counts.values())
        summary = {
            'mode': self.mode,
            'processing_time_sec': (datetime.now() - self.start_time).total_seconds() if self.start_time else 0,
            'thresholds_used': get_thresholds_dict(),
            'stats': self.stats,
            'decision_distribution': {
                'counts': self.decision_counts,
                'rates': {
                    k: round(v / total * 100, 2) if total > 0 else 0
                    for k, v in self.decision_counts.items()
                }
            },
            'sanitization_distribution': self.sanitization_counts,
            'state_transitions_count': len(self.state_transitions),
        }
        with open(output_path / "summary.json", 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"  ✅ summary.json")
    
    def print_summary(self):
        """최종 결과 출력"""
        total = sum(self.decision_counts.values())
        
        print("\n" + "=" * 70)
        print(f"📊 {self.mode.upper()} Validation 결과")
        print("=" * 70)
        
        print("\n[Decision Distribution]")
        for decision, count in self.decision_counts.items():
            pct = count / total * 100 if total > 0 else 0
            bar = "█" * int(pct / 2)
            print(f"  {decision:12}: {count:>6} ({pct:5.1f}%) {bar}")
        
        print(f"\n[Sanitization Distribution]")
        for san, count in self.sanitization_counts.items():
            pct = count / total * 100 if total > 0 else 0
            print(f"  {san:12}: {count:>6} ({pct:5.1f}%)")
        
        print(f"\n[Statistics]")
        print(f"  Tickers:           {self.stats['tickers']:>8}")
        print(f"  Liquidations:      {self.stats['liquidations']:>8}")
        print(f"  Out-of-Order:      {self.stats['out_of_order']:>8}")
        print(f"  Late Events:       {self.stats['late_events']:>8}")
        print(f"  State Transitions: {len(self.state_transitions):>8}")
        
        print("=" * 70)