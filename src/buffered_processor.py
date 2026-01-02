"""
Buffered Processor - Time Alignment Policy 적용

================================================================================
과제 6.2 Time Alignment Policy 구현:
- EventBuffer를 통해 out-of-order 이벤트 정렬
- watermark 기반으로 처리 시점 결정
- late event 감지 및 drop

기존 Processor와의 차이:
- 이벤트를 즉시 처리하지 않고 버퍼에 모음
- 버퍼에서 정렬된 이벤트를 순서대로 처리
- buffer/window/watermark 파라미터가 실제로 동작

사용법:
    processor = BufferedProcessor(mode="historical", output_dir="./output")
    
    for event in data_source.get_events():
        results = processor.process_event(event)  # 여러 결과 반환 가능
    
    # 스트림 종료 시 남은 이벤트 처리
    remaining_results = processor.flush()
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
)
from src.state_machine import StateEvaluator, SystemState
from src.consistency import ConsistencyChecker
from src.event_buffer import EventBuffer, BufferConfig, BufferStats


@dataclass
class ProcessingResult:
    """처리 결과"""
    mode: str = ""
    processing_time_sec: float = 0.0
    
    stats: Dict[str, int] = field(default_factory=dict)
    buffer_stats: Dict[str, int] = field(default_factory=dict)
    
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


class BufferedProcessor:
    """
    Time Alignment Policy가 적용된 Processor
    
    EventBuffer를 통해 이벤트를 정렬하고 watermark 기반으로 처리
    """
    
    def __init__(self, mode: str = "", output_dir: str = None):
        self.mode = mode
        self.th = THRESHOLDS
        
        # Output 디렉토리 설정
        self.output_dir = Path(output_dir) if output_dir else None
        
        # 실시간 로그 파일 핸들
        self._transitions_file: Optional[TextIO] = None
        self._decisions_file: Optional[TextIO] = None
        
        if self.output_dir:
            self._init_log_files()
        
        # === Event Buffer (Time Alignment) ===
        buffer_config = BufferConfig(
            buffer_max_size=self.th.buffer_max_size,
            window_size_ms=self.th.window_size_ms,
            allowed_lateness_ms=self.th.allowed_lateness_ms,
        )
        self.event_buffer = EventBuffer(buffer_config)
        
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
        
        # === Volatility Calculator ===
        self.volatility_calculator = VolatilityCalculator(
            window_size=self.th.volatility_window_size,
            min_samples=self.th.volatility_min_samples
        )
        
        # === Duplicate 감지용 ===
        self._recent_events: deque = deque(maxlen=1000)
        
        # Out-of-order 추적 (버퍼 이전)
        self.last_event_time: int = 0
        self.out_of_order_count: int = 0
        
        # Liquidation 추적
        self.last_liquidation_ts: Optional[int] = None
        self.last_liquidation_size: float = 0.0
        
        # State
        self.current_state = SystemState()
        self.current_ticker: Dict = {}
        
        # Duration 계산용 이전 timestamp
        self.last_transition_ts: Optional[int] = None
        self.last_decision_ts: Optional[int] = None
        
        # Stats
        self.stats = {
            'events_received': 0,
            'events_processed': 0,
            'trades': 0,
            'orderbook_updates': 0,
            'tickers': 0,
            'liquidations': 0,
            'snapshots': 0,
            'duplicates_skipped': 0,
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
    
    def _log_transition(self, log: Dict):
        """State transition 기록 (duration_ms 포함)"""
        current_ts = log.get('ts', 0)
        
        # 이전 transition이 있으면 duration_ms 계산
        if self.last_transition_ts is not None:
            duration_ms = (current_ts - self.last_transition_ts) / 1000.0  # us → ms
            log['duration_ms'] = round(duration_ms, 1)
        else:
            log['duration_ms'] = None  # 첫 번째 transition
        
        self.last_transition_ts = current_ts
        self.state_transitions_count += 1
        
        if self._transitions_file:
            self._transitions_file.write(json.dumps(log) + '\n')
            self._transitions_file.flush()
    
    def _log_decision(self, log: Dict):
        """Decision 기록 (duration_ms 포함)"""
        current_ts = log.get('ts', 0)
        
        # 이전 decision이 있으면 duration_ms 계산
        if self.last_decision_ts is not None:
            duration_ms = (current_ts - self.last_decision_ts) / 1000.0  # us → ms
            log['duration_ms'] = round(duration_ms, 1)
        else:
            log['duration_ms'] = None  # 첫 번째 decision
        
        self.last_decision_ts = current_ts
        self.decisions_logged_count += 1
        
        if self._decisions_file:
            self._decisions_file.write(json.dumps(log) + '\n')
            self._decisions_file.flush()
    
    def process_event(self, event: Event) -> List[Optional[Dict]]:
        """
        이벤트 처리 (버퍼 통과)
        
        Args:
            event: 처리할 이벤트
            
        Returns:
            처리된 결과 리스트 (Ticker 이벤트에서만 Decision 반환)
        """
        if self.start_time is None:
            self.start_time = datetime.now()
        
        self.stats['events_received'] += 1
        
        # === Duplicate 체크 ===
        if self._is_duplicate(event):
            self.stats['duplicates_skipped'] += 1
            return []
        
        # === Out-of-order 체크 (버퍼 이전) ===
        if event.timestamp < self.last_event_time:
            self.out_of_order_count += 1
        self.last_event_time = max(self.last_event_time, event.timestamp)
        
        # === EventBuffer에 추가 ===
        ready_events = self.event_buffer.add(event)
        
        # === 정렬된 이벤트들 처리 ===
        results = []
        for ready_event in ready_events:
            result = self._process_single_event(ready_event)
            if result is not None:
                results.append(result)
        
        return results
    
    def flush(self) -> List[Optional[Dict]]:
        """
        버퍼에 남은 이벤트 모두 처리 (스트림 종료 시)
        
        Returns:
            처리된 결과 리스트
        """
        remaining_events = self.event_buffer.flush()
        
        results = []
        for event in remaining_events:
            result = self._process_single_event(event)
            if result is not None:
                results.append(result)
        
        return results
    
    def _is_duplicate(self, event: Event) -> bool:
        """중복 이벤트 감지"""
        if event.event_type == EventType.TRADE:
            key_data = (
                event.data.get('price'),
                event.data.get('quantity'),
                event.data.get('side'),
            )
        elif event.event_type == EventType.LIQUIDATION:
            key_data = (
                event.data.get('price'),
                event.data.get('quantity'),
                event.data.get('side'),
            )
        elif event.event_type == EventType.TICKER:
            key_data = (event.data.get('last_price'),)
        else:
            key_data = ()
        
        event_key = (event.timestamp, event.event_type.value, key_data)
        
        if event_key in self._recent_events:
            return True
        
        self._recent_events.append(event_key)
        return False
    
    def _process_single_event(self, event: Event) -> Optional[Dict]:
        """단일 이벤트 처리 (버퍼에서 나온 정렬된 이벤트)"""
        self.stats['events_processed'] += 1
        
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
        if latency_ms > 0:
            self.latencies.append(latency_ms)
    
    def _process_orderbook(self, event: Event):
        """Orderbook 업데이트 처리"""
        self.stats['orderbook_updates'] += 1
        
        data = event.data
        latency_ms = (event.local_timestamp - event.timestamp) / 1000.0
        if latency_ms > 0:
            self.latencies.append(latency_ms)
        
        # Orderbook 업데이트
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
        
        # Mid price로 volatility 업데이트
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
        
        # Volatility calculator 리셋
        self.volatility_calculator.reset()
    
    def _process_liquidation(self, event: Event) -> Dict:
        """Liquidation 처리"""
        self.stats['liquidations'] += 1
        
        data = event.data
        self.last_liquidation_ts = event.timestamp
        self.last_liquidation_size = float(data.get('quantity', 0))
        
        return {
            'type': 'liquidation',
            'ts': event.timestamp,
            'size': self.last_liquidation_size,
        }
    
    def _process_ticker(self, event: Event) -> Optional[Dict]:
        """Ticker 처리 - Decision 생성"""
        self.stats['tickers'] += 1
        self.current_ticker = event.data
        
        if not self.orderbook.bid_levels or not self.orderbook.ask_levels:
            return None
        
        # 이전 상태 저장
        prev_state = SystemState(
            data_trust=self.current_state.data_trust,
            hypothesis=self.current_state.hypothesis,
        )
        
        # 1. Uncertainty 계산
        freshness = self._calculate_freshness(event)
        integrity = self._calculate_integrity()
        stability = self._calculate_stability(event.timestamp)
        
        uncertainty = UncertaintyVector(
            freshness=freshness,
            integrity=integrity,
            stability=stability,
            timestamp=event.timestamp
        )
        
        # 2. State Machine 평가
        self.current_state = self.state_evaluator.evaluate(uncertainty=uncertainty)
        
        # 3. Decision 카운트
        decision = self.current_state.decision
        self.decision_counts[decision.value] += 1
        
        # 4. Sanitization 카운트
        sanitization = self.current_state.sanitization
        self.sanitization_counts[sanitization.value] += 1
        
        # 5. Uncertainty snapshot
        vol_diag = self.volatility_calculator.compute()
        uncertainty_snapshot = {
            'freshness': {
                'avg_latency_ms': round(freshness.avg_lateness_ms, 2),
                'stale_ratio': round(freshness.stale_event_ratio, 4),
            },
            'integrity': {
                'spread_valid': integrity.spread_valid,
                'price_deviation_bps': round(integrity.price_deviation_bps, 2),
            },
            'stability': {
                'volatility_bps': round(vol_diag.volatility, 4) if vol_diag.volatility is not None else None,
                'n_samples': vol_diag.n_samples,
            },
        }
        
        # 6. State transition 로깅
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
                    'sanitization': sanitization.value,
                },
                'trigger': {
                    'trust_reasons': self.current_state.trust_reasons,
                    'hypothesis_reasons': self.current_state.hypothesis_reasons,
                },
                'uncertainty': uncertainty_snapshot,
            }
            self._log_transition(transition_log)
        
        # 7. Decision 로깅
        decision_log = {
            'ts': event.timestamp,
            'action': decision.value,
            'data_trust': self.current_state.data_trust.value,
            'hypothesis': self.current_state.hypothesis.value,
            'sanitization': sanitization.value,
            'reasons': {
                'trust': self.current_state.trust_reasons,
                'hypothesis': self.current_state.hypothesis_reasons,
            },
            'uncertainty': uncertainty_snapshot,
        }
        self._log_decision(decision_log)
        
        return decision_log
    
    def _calculate_freshness(self, event: Event) -> FreshnessUncertainty:
        """Freshness 계산"""
        if not self.latencies:
            return FreshnessUncertainty()
        
        latencies = list(self.latencies)
        avg = sum(latencies) / len(latencies)
        max_lat = max(latencies)
        
        stale_count = sum(1 for l in latencies if l > self.th.allowed_lateness_ms)
        stale_ratio = stale_count / len(latencies)
        
        # Out-of-order 정보 추가
        buffer_stats = self.event_buffer.get_stats()
        
        return FreshnessUncertainty(
            avg_lateness_ms=avg,
            max_lateness_ms=max_lat,
            stale_event_ratio=stale_ratio,
            out_of_order_count=buffer_stats.out_of_order_received,
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
        """Stability 계산 (Price Volatility 기반)"""
        stability = StabilityUncertainty()
        
        # Volatility - VolatilityDiagnostics 객체로 설정
        vol_diag = self.volatility_calculator.compute()
        stability.volatility = vol_diag
        
        # Liquidation cooldown
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
            return  # Crossed market - skip
        
        mid_price = (best_bid + best_ask) / 2
        self.volatility_calculator.update(mid_price)
    
    def close(self):
        """리소스 정리"""
        if self._transitions_file:
            self._transitions_file.close()
        if self._decisions_file:
            self._decisions_file.close()
    
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
            'buffer_stats': self.event_buffer.get_stats_dict(),
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
            buffer_stats=self.event_buffer.get_stats_dict(),
            decision_counts=self.decision_counts.copy(),
            sanitization_counts=self.sanitization_counts.copy(),
            state_transitions_count=self.state_transitions_count,
            decisions_count=self.decisions_logged_count,
        )
    
    def print_summary(self):
        """결과 요약 출력"""
        total = sum(self.decision_counts.values())
        if total == 0:
            print("No decisions made")
            return
        
        print(f"\n{'='*60}")
        print(f"📊 {self.mode} 결과 (BufferedProcessor)")
        print(f"{'='*60}")
        
        print(f"\n[Decision Distribution]")
        for decision, count in self.decision_counts.items():
            pct = count / total * 100
            print(f"  {decision}: {count:,} ({pct:.1f}%)")
        
        print(f"\n[Sanitization Distribution]")
        for sanit, count in self.sanitization_counts.items():
            print(f"  {sanit}: {count:,}")
        
        print(f"\n[Buffer Statistics]")
        buffer_stats = self.event_buffer.get_stats_dict()
        print(f"  Total received: {buffer_stats['total_received']:,}")
        print(f"  Total emitted:  {buffer_stats['total_emitted']:,}")
        print(f"  Dropped (late): {buffer_stats['dropped_late']:,}")
        print(f"  Out-of-order:   {buffer_stats['out_of_order_received']:,}")
        print(f"  Max buffer size:{buffer_stats['max_buffer_size']:,}")
        
        print(f"\n[State Transitions: {self.state_transitions_count}]")
        print(f"{'='*60}")