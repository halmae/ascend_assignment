"""
Effective Orderbook State Processor (v2 - 3-State Architecture)

핵심 개념:
- Effective Orderbook = (Orderbook Structure, Uncertainty Vector U_t)
- 3-State Architecture:
  1. Data Trust State: 데이터 신뢰도 (TRUSTED/DEGRADED/UNTRUSTED)
  2. Hypothesis Validity State: 가설 유효성 (VALID/WEAKENING/INVALID)
  3. Decision Permission State: 판단 허용 (ALLOWED/RESTRICTED/HALTED)

Uncertainty Vector:
- Freshness: 데이터가 얼마나 신선한가?
- Integrity: 데이터가 서로 일관성이 있는가?
- Stability: Orderbook이 안정적인가? (특히 Liquidation 이후)

중요: Orderbook에는 모든 이벤트 적용 (drop 없음)
      Lateness는 Uncertainty metric으로만 사용
"""
import time
import json
from collections import deque
from typing import List, Optional, Dict, Deque
from dataclasses import dataclass

from src.enums import EventType, DataTrustState, HypothesisValidityState, DecisionPermissionState, RepairAction
from src.data_types import Event, OrderbookState
from src.consistency import ConsistencyChecker
from src.uncertainty import (
    UncertaintyVector, 
    FreshnessUncertainty, 
    IntegrityUncertainty, 
    StabilityUncertainty,
    TradabilityState
)
from src.state_machine import StateEvaluator, SystemState, StateThresholds
from src.results import ProcessingResult


@dataclass
class LatencyWindow:
    """최근 N개 이벤트의 latency 추적"""
    window_size: int = 1000
    latencies: Deque[float] = None
    stale_threshold_ms: float = 50.0  # 이 이상이면 "stale"로 간주 (측정용)
    
    def __post_init__(self):
        if self.latencies is None:
            self.latencies = deque(maxlen=self.window_size)
    
    def add(self, latency_ms: float):
        self.latencies.append(latency_ms)
    
    def get_avg(self) -> float:
        if not self.latencies:
            return 0.0
        return sum(self.latencies) / len(self.latencies)
    
    def get_max(self) -> float:
        if not self.latencies:
            return 0.0
        return max(self.latencies)
    
    def get_stale_ratio(self) -> float:
        if not self.latencies:
            return 0.0
        stale_count = sum(1 for l in self.latencies if l > self.stale_threshold_ms)
        return stale_count / len(self.latencies)


@dataclass
class SpreadHistory:
    """Spread/Imbalance 변동성 추적"""
    window_size: int = 100
    spreads: Deque[float] = None
    imbalances: Deque[float] = None
    mid_prices: Deque[float] = None
    
    def __post_init__(self):
        if self.spreads is None:
            self.spreads = deque(maxlen=self.window_size)
        if self.imbalances is None:
            self.imbalances = deque(maxlen=self.window_size)
        if self.mid_prices is None:
            self.mid_prices = deque(maxlen=self.window_size)
    
    def add(self, spread: float, imbalance: float, mid_price: float):
        self.spreads.append(spread)
        self.imbalances.append(imbalance)
        self.mid_prices.append(mid_price)
    
    def get_spread_volatility(self) -> float:
        """Spread의 변동계수 (CV)"""
        if len(self.spreads) < 2:
            return 0.0
        avg = sum(self.spreads) / len(self.spreads)
        if avg == 0:
            return 0.0
        variance = sum((s - avg) ** 2 for s in self.spreads) / len(self.spreads)
        return (variance ** 0.5) / avg
    
    def get_imbalance_volatility(self) -> float:
        """Imbalance의 표준편차"""
        if len(self.imbalances) < 2:
            return 0.0
        avg = sum(self.imbalances) / len(self.imbalances)
        variance = sum((i - avg) ** 2 for i in self.imbalances) / len(self.imbalances)
        return variance ** 0.5
    
    def get_mid_price_volatility(self) -> float:
        """Mid price의 변동계수"""
        if len(self.mid_prices) < 2:
            return 0.0
        avg = sum(self.mid_prices) / len(self.mid_prices)
        if avg == 0:
            return 0.0
        variance = sum((p - avg) ** 2 for p in self.mid_prices) / len(self.mid_prices)
        return (variance ** 0.5) / avg


class EffectiveOrderbookProcessor:
    """
    Effective Orderbook State Processor (v2)
    
    3-State Architecture를 적용한 실시간 스트림 처리기
    """

    def __init__(self,
                 dataset_name: str = "",
                 buffer_size: int = 1000,
                 watermark_delay_ms: int = 50,
                 snapshot_buffer_size: int = 5,
                 max_orderbook_levels: int = 500,
                 stale_threshold_ms: float = 50.0,
                 liquidation_cooldown_ms: float = 5000.0,
                 state_thresholds: Optional[StateThresholds] = None):
        """
        Args:
            stale_threshold_ms: 이 이상의 latency는 "stale"로 측정 (drop 아님)
            liquidation_cooldown_ms: Liquidation 이후 안정화 대기 시간
            state_thresholds: 상태 전이 임계값 (None이면 기본값 사용)
        """
        self.dataset_name = dataset_name
        self.buffer_size = buffer_size
        self.watermark_delay_ms = watermark_delay_ms
        self.snapshot_buffer_size = snapshot_buffer_size
        self.max_orderbook_levels = max_orderbook_levels
        self.stale_threshold_ms = stale_threshold_ms
        self.liquidation_cooldown_ms = liquidation_cooldown_ms

        # Buffers
        self.main_buffer: List[Event] = []
        self.snapshot_buffer: Deque[Event] = deque(maxlen=snapshot_buffer_size)

        # Orderbook State (모든 이벤트 적용 - drop 없음)
        self.current_orderbook: Optional[OrderbookState] = None
        self.initialized = False

        # Uncertainty 추적
        self.ob_latency_window = LatencyWindow(stale_threshold_ms=stale_threshold_ms)
        self.trade_latency_window = LatencyWindow(stale_threshold_ms=stale_threshold_ms)
        self.spread_history = SpreadHistory()
        
        # Liquidation 추적
        self.last_liquidation_ts: Optional[int] = None
        self.last_liquidation_size: float = 0.0
        
        # Integrity 추적 (최근 윈도우)
        self.integrity_window_size = 100
        self.integrity_results: Deque[IntegrityUncertainty] = deque(maxlen=self.integrity_window_size)

        # Current Uncertainty Vector
        self.current_uncertainty = UncertaintyVector()
        
        # Consistency Checker
        self.consistency_checker = ConsistencyChecker()
        
        # === NEW: 3-State Architecture ===
        self.state_evaluator = StateEvaluator(thresholds=state_thresholds)
        self.current_system_state = SystemState()
        
        # Decision 로그 (decisions.jsonl용)
        self.decisions_log: List[Dict] = []

        # Statistics
        self.stats = {
            'events_processed': 0,
            'orderbook_updates': 0,
            'trades_processed': 0,
            'trades_valid': 0,
            'trades_invalid': 0,
            'ticker_checkpoints': 0,
            'liquidations_processed': 0,
            'snapshots_used': 0,
        }
        
        # State 분포 (3-State)
        self.decision_counts = {
            'ALLOWED': 0,
            'RESTRICTED': 0,
            'HALTED': 0
        }
        
        # 기존 Tradability 분포 (호환성 유지)
        self.tradability_counts = {
            'TRADABLE': 0,
            'RESTRICTED': 0,
            'NOT_TRADABLE': 0
        }
        
        # State 전이 로그 (state_transitions.jsonl용)
        self.state_transitions: List[Dict] = []
        
        # Uncertainty 로그 (샘플링)
        self.uncertainty_log: List[Dict] = []
        self.log_interval = 100  # 100 ticker마다 로깅

        # Watermark
        self.last_watermark = None
        self.last_ticker_ts = None
        
        self.start_time = None


    def add_event(self, event: Event):
        """이벤트를 버퍼에 추가"""
        if self.start_time is None:
            self.start_time = time.time()

        # Snapshot은 별도 버퍼
        if event.event_type == EventType.ORDERBOOK and event.data.get('is_snapshot', False):
            self.snapshot_buffer.append(event)
            return
        
        self.main_buffer.append(event)

        # Ticker가 오면 처리
        if event.event_type == EventType.TICKER:
            self.last_ticker_ts = event.timestamp
            self.process_buffer()

    
    def process_buffer(self):
        """버퍼 처리"""
        if not self.main_buffer:
            return
        
        if self.last_ticker_ts:
            watermark = self.last_ticker_ts
        else:
            last_event_ts = max(e.timestamp for e in self.main_buffer)
            watermark = last_event_ts - (self.watermark_delay_ms * 1000)

        self.last_watermark = watermark

        to_process = [e for e in self.main_buffer if e.timestamp <= watermark]
        self.main_buffer = [e for e in self.main_buffer if e.timestamp > watermark]

        to_process.sort(key=lambda e: e.timestamp)

        for event in to_process:
            self._dispatch_event(event)


    def _dispatch_event(self, event: Event):
        """이벤트 분기"""
        self.stats['events_processed'] += 1
        
        if event.event_type == EventType.ORDERBOOK:
            self._process_orderbook(event)
        elif event.event_type == EventType.TRADE:
            self._process_trade(event)
        elif event.event_type == EventType.TICKER:
            self._process_ticker(event)
        elif event.event_type == EventType.LIQUIDATION:
            self._process_liquidation(event)


    def _calculate_lateness_ms(self, event: Event) -> float:
        """Lateness 계산 (ms)"""
        lateness = (event.local_timestamp - event.timestamp) / 1000.0
        return max(0.0, lateness)


    def _process_orderbook(self, event: Event):
        """
        Orderbook 업데이트 처리
        
        중요: 모든 이벤트 적용 (drop 없음)
              Lateness는 Freshness Uncertainty로만 측정
        """
        # 초기화
        if not self.initialized:
            snapshot = self._get_latest_snapshot(before_timestamp=event.timestamp)
            if snapshot:
                self.current_orderbook = self._rebuild_orderbook_from_snapshot(snapshot)
                self.initialized = True
                self._log_init()
            else:
                return
        
        # Lateness 측정 (Freshness Uncertainty용)
        lateness_ms = self._calculate_lateness_ms(event)
        self.ob_latency_window.add(lateness_ms)
        
        # Orderbook 업데이트 (모든 이벤트 적용)
        data = event.data
        price = float(data['price'])
        amount = float(data['amount'])
        side = data['side']

        if side == 'bid':
            if amount == 0:
                self.current_orderbook.bid_levels.pop(price, None)
            else:
                self.current_orderbook.bid_levels[price] = amount
        else:
            if amount == 0:
                self.current_orderbook.ask_levels.pop(price, None)
            else:
                self.current_orderbook.ask_levels[price] = amount
        
        self.current_orderbook.timestamp = event.timestamp
        self.stats['orderbook_updates'] += 1
        
        # Orderbook trimming
        if self.stats['orderbook_updates'] % 1000 == 0:
            self._trim_orderbook()


    def _trim_orderbook(self):
        """Orderbook 크기 제한"""
        if not self.current_orderbook:
            return
        
        max_levels = self.max_orderbook_levels
        bids = self.current_orderbook.bid_levels
        asks = self.current_orderbook.ask_levels
        
        if len(bids) > max_levels:
            sorted_prices = sorted(bids.keys(), reverse=True)[:max_levels]
            self.current_orderbook.bid_levels = {p: bids[p] for p in sorted_prices}
        
        if len(asks) > max_levels:
            sorted_prices = sorted(asks.keys())[:max_levels]
            self.current_orderbook.ask_levels = {p: asks[p] for p in sorted_prices}


    def _process_trade(self, event: Event):
        """
        Trade 처리
        
        Trade Validity 검증:
        "이 trade는 현재 Orderbook 상태에서 발생 가능한 trade인가?"
        """
        if not self.initialized:
            return
        
        self.stats['trades_processed'] += 1
        
        # Lateness 측정
        lateness_ms = self._calculate_lateness_ms(event)
        self.trade_latency_window.add(lateness_ms)
        
        # Trade Validity 검증
        is_valid = self._validate_trade(event, lateness_ms)
        
        if is_valid:
            self.stats['trades_valid'] += 1
        else:
            self.stats['trades_invalid'] += 1


    def _validate_trade(self, event: Event, lateness_ms: float) -> bool:
        """
        Trade Validity 검증
        
        Latency-aware margin: latency가 클수록 margin 증가
        """
        if not self.current_orderbook:
            return False
        
        price = float(event.data['price'])
        
        best_bid = self.current_orderbook.get_best_bid()
        best_ask = self.current_orderbook.get_best_ask()
        
        if best_bid is None or best_ask is None:
            return False
        
        # Crossed market이면 invalid
        if best_bid >= best_ask:
            return False
        
        spread = best_ask - best_bid
        mid_price = (best_bid + best_ask) / 2
        
        # Base margin
        relative_margin = spread * 0.5
        absolute_margin = mid_price * 0.0001  # 1bp
        base_margin = max(relative_margin, absolute_margin)
        
        # Latency-aware margin: 10ms당 1bp 추가
        latency_margin = mid_price * 0.0001 * (lateness_ms / 10.0)
        
        margin = base_margin + latency_margin
        
        return (best_bid - margin) <= price <= (best_ask + margin)


    def _process_ticker(self, event: Event):
        """
        Ticker 처리 - Uncertainty Vector 및 System State 계산
        
        Ticker는 "checkpoint" 역할
        이 시점에서 Effective Orderbook의 Uncertainty를 평가하고
        3-State를 결정함
        """
        if not self.initialized:
            self.stats['ticker_checkpoints'] += 1
            return
        
        self.stats['ticker_checkpoints'] += 1
        
        # 이전 상태 저장 (전이 감지용)
        prev_system_state = SystemState(
            data_trust=self.current_system_state.data_trust,
            hypothesis=self.current_system_state.hypothesis,
            timestamp=self.current_system_state.timestamp
        )
        
        # 1. Freshness Uncertainty 계산
        freshness = FreshnessUncertainty(
            avg_lateness_ms=self.ob_latency_window.get_avg(),
            max_lateness_ms=self.ob_latency_window.get_max(),
            stale_event_ratio=self.ob_latency_window.get_stale_ratio()
        )
        
        # 2. Integrity Uncertainty 계산
        integrity = self.consistency_checker.check_integrity(
            ticker_data=event.data,
            orderbook=self.current_orderbook
        )
        self.integrity_results.append(integrity)
        
        # Failure rate 계산
        if self.integrity_results:
            integrity.spread_valid_failure_rate = sum(
                1 for i in self.integrity_results if not i.spread_valid
            ) / len(self.integrity_results)
            integrity.price_in_spread_failure_rate = sum(
                1 for i in self.integrity_results if not i.price_in_spread
            ) / len(self.integrity_results)
            integrity.funding_aligned_failure_rate = sum(
                1 for i in self.integrity_results if not i.funding_imbalance_aligned
            ) / len(self.integrity_results)
        
        # 3. Stability Uncertainty 계산
        stability = self._calculate_stability(event.timestamp)
        
        # Spread history 업데이트
        spread_bps = None
        if self.current_orderbook:
            best_bid = self.current_orderbook.get_best_bid()
            best_ask = self.current_orderbook.get_best_ask()
            if best_bid and best_ask and best_bid < best_ask:
                spread = best_ask - best_bid
                mid_price = (best_bid + best_ask) / 2
                spread_bps = (spread / mid_price) * 10000  # basis points
                bid_depth = sum(self.current_orderbook.bid_levels.values())
                ask_depth = sum(self.current_orderbook.ask_levels.values())
                total = bid_depth + ask_depth
                imbalance = (bid_depth - ask_depth) / total if total > 0 else 0
                self.spread_history.add(spread, imbalance, mid_price)
        
        # 4. Uncertainty Vector 업데이트
        self.current_uncertainty = UncertaintyVector(
            freshness=freshness,
            integrity=integrity,
            stability=stability,
            timestamp=event.timestamp
        )
        
        # 5. === NEW: 3-State 평가 ===
        self.current_system_state = self.state_evaluator.evaluate(
            uncertainty=self.current_uncertainty,
            orderbook_spread_bps=spread_bps
        )
        
        # Decision 분포 업데이트
        decision = self.current_system_state.decision
        self.decision_counts[decision.value] += 1
        
        # 기존 Tradability 매핑 (호환성)
        tradability = self._map_decision_to_tradability(decision)
        self.tradability_counts[tradability] += 1
        
        # 6. State 전이 로깅
        if self._has_system_state_changed(prev_system_state, self.current_system_state):
            transition_record = self._create_transition_record(
                prev_system_state, 
                self.current_system_state,
                event.timestamp
            )
            self.state_transitions.append(transition_record)
        
        # 7. Decision 로깅 (판단 중단 시 상세 기록)
        if decision == DecisionPermissionState.HALTED:
            self._log_decision(event.timestamp, "HALT", self.current_system_state)
        elif decision == DecisionPermissionState.RESTRICTED:
            self._log_decision(event.timestamp, "RESTRICT", self.current_system_state)
        
        # 8. 주기적 로깅
        if self.stats['ticker_checkpoints'] % self.log_interval == 0:
            self.uncertainty_log.append(self._create_full_log_entry())
            self._print_status()


    def _map_decision_to_tradability(self, decision: DecisionPermissionState) -> str:
        """Decision Permission을 기존 Tradability로 매핑 (호환성)"""
        mapping = {
            DecisionPermissionState.ALLOWED: 'TRADABLE',
            DecisionPermissionState.RESTRICTED: 'RESTRICTED',
            DecisionPermissionState.HALTED: 'NOT_TRADABLE'
        }
        return mapping[decision]


    def _has_system_state_changed(self, prev: SystemState, curr: SystemState) -> bool:
        """System State 전이 감지"""
        return (prev.data_trust != curr.data_trust or 
                prev.hypothesis != curr.hypothesis)


    def _create_transition_record(self, 
                                   prev: SystemState, 
                                   curr: SystemState,
                                   timestamp: int) -> Dict:
        """state_transitions.jsonl 형식의 전이 기록 생성"""
        return {
            'ts': timestamp,
            'data_trust': curr.data_trust.value,
            'hypothesis': curr.hypothesis.value,
            'decision': curr.decision.value,
            'trigger': {
                'from_trust': prev.data_trust.value,
                'from_hypothesis': prev.hypothesis.value,
                'trust_reasons': curr.trust_reasons,
                'hypothesis_reasons': curr.hypothesis_reasons
            }
        }


    def _log_decision(self, timestamp: int, action: str, state: SystemState):
        """decisions.jsonl 형식의 판단 기록 생성"""
        # duration 계산 (다음 상태 변경까지의 시간은 나중에 계산)
        decision_record = {
            'ts': timestamp,
            'action': action,
            'reason': {
                'data_trust': state.data_trust.value,
                'hypothesis': state.hypothesis.value,
                'trust_reasons': state.trust_reasons,
                'hypothesis_reasons': state.hypothesis_reasons
            },
            'duration_ms': None  # 추후 계산
        }
        self.decisions_log.append(decision_record)


    def _create_full_log_entry(self) -> Dict:
        """전체 상태 로그 엔트리 생성"""
        u = self.current_uncertainty
        s = self.current_system_state
        
        return {
            'timestamp': u.timestamp,
            # 3-State
            'data_trust': s.data_trust.value,
            'hypothesis': s.hypothesis.value,
            'decision': s.decision.value,
            # Uncertainty details
            'freshness': {
                'avg_lateness_ms': round(u.freshness.avg_lateness_ms, 2),
                'max_lateness_ms': round(u.freshness.max_lateness_ms, 2),
                'stale_ratio': round(u.freshness.stale_event_ratio, 4),
            },
            'integrity': {
                'spread_valid': u.integrity.spread_valid,
                'price_in_spread': u.integrity.price_in_spread,
                'funding_aligned': u.integrity.funding_imbalance_aligned,
                'failure_count': u.integrity.failure_count,
            },
            'stability': {
                'spread_volatility': round(u.stability.spread_volatility, 4),
                'time_since_liq_ms': u.stability.time_since_liquidation_ms,
                'post_liq_stable': u.stability.post_liquidation_stable,
            }
        }


    def _calculate_stability(self, current_ts: int) -> StabilityUncertainty:
        """Stability Uncertainty 계산"""
        stability = StabilityUncertainty()
        
        # Spread/Imbalance 변동성
        stability.spread_volatility = self.spread_history.get_spread_volatility()
        stability.imbalance_volatility = self.spread_history.get_imbalance_volatility()
        stability.mid_price_volatility = self.spread_history.get_mid_price_volatility()
        
        # Liquidation 이후 시간
        if self.last_liquidation_ts is not None:
            time_since_liq = (current_ts - self.last_liquidation_ts) / 1000.0  # ms
            stability.time_since_liquidation_ms = time_since_liq
            stability.liquidation_size = self.last_liquidation_size
            
            # 충분한 시간이 지났는지 판단
            stability.post_liquidation_stable = time_since_liq >= self.liquidation_cooldown_ms
        
        return stability


    def _process_liquidation(self, event: Event):
        """
        Liquidation 처리
        
        Liquidation은 Orderbook uncertainty의 spike로 정의
        """
        self.stats['liquidations_processed'] += 1
        
        # Liquidation 정보 기록
        self.last_liquidation_ts = event.timestamp
        self.last_liquidation_size = float(event.data.get('quantity', 0))
        
        # 로깅
        print(f"  [LIQUIDATION] ts={event.timestamp}, "
              f"side={event.data.get('side')}, "
              f"size={self.last_liquidation_size:.4f}")


    def _get_latest_snapshot(self, before_timestamp: int) -> Optional[Event]:
        """Snapshot 가져오기"""
        valid_snapshots = [s for s in self.snapshot_buffer if s.timestamp <= before_timestamp]
        if not valid_snapshots:
            return None
        return max(valid_snapshots, key=lambda s: s.timestamp)
    

    def _rebuild_orderbook_from_snapshot(self, snapshot: Event) -> OrderbookState:
        """Snapshot으로 Orderbook 재구성"""
        self.stats['snapshots_used'] += 1
        
        data = snapshot.data
        
        if 'bids' in data and 'asks' in data:
            bids = {float(price): float(amount) for price, amount in data['bids']}
            asks = {float(price): float(amount) for price, amount in data['asks']}
        else:
            bids = {}
            asks = {}
        
        return OrderbookState(
            timestamp=snapshot.timestamp,
            bid_levels=bids,
            ask_levels=asks
        )


    def _log_init(self):
        """초기화 로깅"""
        print(f"\n  [INIT] Effective Orderbook initialized")
        print(f"         Bids: {len(self.current_orderbook.bid_levels)}")
        print(f"         Asks: {len(self.current_orderbook.ask_levels)}")
        best_bid = self.current_orderbook.get_best_bid()
        best_ask = self.current_orderbook.get_best_ask()
        print(f"         Best Bid: {best_bid}")
        print(f"         Best Ask: {best_ask}")
        if best_bid and best_ask:
            print(f"         Spread: {best_ask - best_bid:.2f}")


    def _print_status(self):
        """상태 출력 (3-State 버전)"""
        u = self.current_uncertainty
        s = self.current_system_state
        
        print(f"\n{'='*60}")
        print(f"Ticker #{self.stats['ticker_checkpoints']}")
        print(f"{'='*60}")
        print(f"  [3-State]")
        print(f"    Data Trust:  {s.data_trust.value}")
        print(f"    Hypothesis:  {s.hypothesis.value}")
        print(f"    Decision:    {s.decision.value}")
        print(f"  [Uncertainty]")
        print(f"    Freshness: avg={u.freshness.avg_lateness_ms:.1f}ms, "
              f"stale_ratio={u.freshness.stale_event_ratio:.2%}")
        print(f"    Integrity: spread_valid={u.integrity.spread_valid}, "
              f"failures={u.integrity.failure_count}")
        print(f"    Stability: spread_vol={u.stability.spread_volatility:.4f}, "
              f"post_liq_stable={u.stability.post_liquidation_stable}")


    def get_result(self) -> 'ProcessingResult':
        """처리 결과 반환"""
        processing_time = time.time() - self.start_time if self.start_time else 0
        
        # Decision duration 계산 (연속된 HALT/RESTRICT의 지속 시간)
        self._calculate_decision_durations()
        
        result = ProcessingResult(
            dataset_name=self.dataset_name,
            processing_time_sec=processing_time,
            stats=self.stats.copy(),
            tradability_counts=self.tradability_counts.copy(),
            state_transitions=self.state_transitions.copy(),
            uncertainty_log=self.uncertainty_log.copy(),
            final_uncertainty=self._create_full_log_entry() if self.current_uncertainty else {}
        )
        
        # 추가 데이터 (3-State 관련)
        result.decision_counts = self.decision_counts.copy()
        result.decisions_log = self.decisions_log.copy()
        result.state_evaluator_summary = self.state_evaluator.get_state_summary()
        
        return result


    def _calculate_decision_durations(self):
        """Decision 로그의 duration 계산"""
        for i, decision in enumerate(self.decisions_log):
            if i + 1 < len(self.decisions_log):
                next_ts = self.decisions_log[i + 1]['ts']
                decision['duration_ms'] = (next_ts - decision['ts']) / 1000.0
            else:
                # 마지막 decision은 현재까지의 duration
                if self.current_uncertainty:
                    decision['duration_ms'] = (self.current_uncertainty.timestamp - decision['ts']) / 1000.0