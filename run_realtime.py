"""
Realtime Processor - WebSocket 기반 실시간 처리 (Robust Version)

================================================================================
요구사항 5.2 충족:
- 네트워크 단절 및 재연결 ✅
- 중복 메시지 ✅
- out-of-order 도착 ✅
- burst ✅
- 특정 스트림 장시간 정지 ✅
================================================================================
"""
import asyncio
import websockets
import json
from datetime import datetime
from collections import deque, OrderedDict
from pathlib import Path
from typing import Dict, Optional, List, Set
from dataclasses import dataclass, field

from src.config import THRESHOLDS, REALTIME_CONFIG, get_thresholds_dict, RealtimeConfig
from src.data_types import OrderbookState
from src.uncertainty import (
    UncertaintyVector,
    FreshnessUncertainty,
    IntegrityUncertainty,
    StabilityUncertainty
)
from src.state_machine import StateEvaluator, SystemState
from src.enums import DecisionPermissionState
from src.consistency import ConsistencyChecker


@dataclass
class StreamHealth:
    """스트림 건강 상태 추적"""
    last_received: Optional[datetime] = None
    message_count: int = 0
    is_stale: bool = False
    stale_threshold_sec: float = 10.0  # 10초 이상 수신 없으면 stale
    
    def update(self):
        self.last_received = datetime.now()
        self.message_count += 1
        self.is_stale = False
    
    def check_stale(self) -> bool:
        if self.last_received is None:
            return False
        elapsed = (datetime.now() - self.last_received).total_seconds()
        self.is_stale = elapsed > self.stale_threshold_sec
        return self.is_stale


class RealtimeProcessor:
    """
    Robust 실시간 Decision Engine
    
    Robustness 기능:
    1. 네트워크 단절 → 자동 재연결
    2. 중복 메시지 → 필터링
    3. out-of-order → 허용 (State Machine은 영향 없음)
    4. burst → 처리 (백프레셔 없음, 모두 처리)
    5. 스트림 정지 → 감지 및 경고 (시스템은 계속 동작)
    """
    
    def __init__(self, config: Optional[RealtimeConfig] = None):
        self.config = config or REALTIME_CONFIG
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
        self.orderbook_initialized = False
        
        # === Robustness: 중복 메시지 필터링 ===
        # 최근 N개의 메시지 ID 추적 (trade_id, update_id 등)
        self.seen_trade_ids: OrderedDict = OrderedDict()
        self.seen_depth_ids: OrderedDict = OrderedDict()
        self.max_seen_ids: int = 10000  # 최대 추적 개수
        
        # === Robustness: 스트림 건강 상태 ===
        self.stream_health: Dict[str, StreamHealth] = {
            'trade': StreamHealth(stale_threshold_sec=5.0),
            'depth': StreamHealth(stale_threshold_sec=5.0),
            'ticker': StreamHealth(stale_threshold_sec=10.0),
            'liquidation': StreamHealth(stale_threshold_sec=60.0),  # 청산은 드물 수 있음
        }
        
        # === Robustness: out-of-order 추적 ===
        self.last_timestamps: Dict[str, int] = {}
        self.out_of_order_count: int = 0
        
        # === Robustness: 재연결 통계 ===
        self.reconnect_count: int = 0
        self.last_disconnect: Optional[datetime] = None
        
        # Latency tracking
        self.latencies: deque = deque(maxlen=self.th.latency_window_size)
        
        # Spread history
        self.spreads: deque = deque(maxlen=self.th.spread_history_size)
        
        # Liquidation tracking
        self.last_liquidation_ts: Optional[int] = None
        self.last_liquidation_size: float = 0.0
        
        # Current state
        self.current_state = SystemState()
        self.current_ticker: Dict = {}
        
        # Statistics
        self.stats = {
            'messages_total': 0,
            'messages_processed': 0,
            'messages_duplicate': 0,
            'messages_out_of_order': 0,
            'trades': 0,
            'depth_updates': 0,
            'tickers': 0,
            'liquidations': 0,
            'reconnects': 0,
            'stream_stale_events': 0,
        }
        
        self.decision_counts = {
            'ALLOWED': 0,
            'RESTRICTED': 0,
            'HALTED': 0,
        }
        
        # Logs
        self.state_transitions: List[Dict] = []
        self.decisions_log: List[Dict] = []
        self.liquidation_events: List[Dict] = []
        self.robustness_events: List[Dict] = []  # 재연결, stale 등 로깅
        
        # Timing
        self.start_time: Optional[datetime] = None
    
    def _is_duplicate(self, stream_type: str, msg_id: any) -> bool:
        """
        중복 메시지 체크
        
        Trade: trade_id
        Depth: first_update_id + final_update_id 조합
        """
        if stream_type == 'trade':
            if msg_id in self.seen_trade_ids:
                return True
            self.seen_trade_ids[msg_id] = True
            # 오래된 ID 정리
            while len(self.seen_trade_ids) > self.max_seen_ids:
                self.seen_trade_ids.popitem(last=False)
            return False
        
        elif stream_type == 'depth':
            if msg_id in self.seen_depth_ids:
                return True
            self.seen_depth_ids[msg_id] = True
            while len(self.seen_depth_ids) > self.max_seen_ids:
                self.seen_depth_ids.popitem(last=False)
            return False
        
        return False
    
    def _check_out_of_order(self, stream_type: str, timestamp: int) -> bool:
        """Out-of-order 체크 (경고만, 처리는 계속)"""
        last_ts = self.last_timestamps.get(stream_type, 0)
        
        if timestamp < last_ts:
            self.out_of_order_count += 1
            self.stats['messages_out_of_order'] += 1
            return True
        
        self.last_timestamps[stream_type] = timestamp
        return False
    
    def _check_stream_health(self):
        """모든 스트림 건강 상태 체크"""
        for stream_name, health in self.stream_health.items():
            was_stale = health.is_stale
            is_now_stale = health.check_stale()
            
            # 새로 stale 됨
            if is_now_stale and not was_stale:
                self.stats['stream_stale_events'] += 1
                self.robustness_events.append({
                    'type': 'STREAM_STALE',
                    'stream': stream_name,
                    'timestamp': datetime.now().isoformat(),
                    'last_received': health.last_received.isoformat() if health.last_received else None,
                })
                print(f"\n  ⚠️ Stream '{stream_name}' is stale (no data for {health.stale_threshold_sec}s)")
    
    def _log_reconnect(self):
        """재연결 로깅"""
        self.reconnect_count += 1
        self.stats['reconnects'] += 1
        self.robustness_events.append({
            'type': 'RECONNECT',
            'count': self.reconnect_count,
            'timestamp': datetime.now().isoformat(),
        })
    
    def process_message(self, message: str) -> Optional[Dict]:
        """
        메시지 처리 (Robust)
        
        1. 파싱 에러 → 무시하고 계속
        2. 중복 → 필터링
        3. out-of-order → 경고하고 처리
        """
        self.stats['messages_total'] += 1
        
        # === 파싱 에러 핸들링 ===
        try:
            data = json.loads(message)
        except json.JSONDecodeError as e:
            # 잘못된 JSON → 무시하고 계속
            return None
        
        payload = data.get('data', {})
        event_type = payload.get('e', 'unknown')
        
        local_ts = int(datetime.now().timestamp() * 1000000)
        
        # === 스트림 건강 상태 업데이트 ===
        stream_map = {
            'trade': 'trade',
            'depthUpdate': 'depth',
            '24hrTicker': 'ticker',
            'forceOrder': 'liquidation',
        }
        stream_name = stream_map.get(event_type)
        if stream_name and stream_name in self.stream_health:
            self.stream_health[stream_name].update()
        
        # === 이벤트 타입별 처리 ===
        try:
            if event_type == 'trade':
                return self._process_trade(payload, local_ts)
            elif event_type == 'depthUpdate':
                return self._process_depth(payload, local_ts)
            elif event_type == 'forceOrder':
                return self._process_liquidation(payload, local_ts)
            elif event_type == '24hrTicker':
                return self._process_ticker(payload, local_ts)
        except Exception as e:
            # 개별 이벤트 처리 에러 → 무시하고 계속
            return None
        
        return None
    
    def _process_trade(self, data: Dict, local_ts: int) -> Optional[Dict]:
        """Trade 처리"""
        # 중복 체크
        trade_id = data.get('t')
        if trade_id and self._is_duplicate('trade', trade_id):
            self.stats['messages_duplicate'] += 1
            return None
        
        # out-of-order 체크
        event_ts = data.get('E', 0) * 1000
        self._check_out_of_order('trade', event_ts)
        
        self.stats['trades'] += 1
        self.stats['messages_processed'] += 1
        
        # Latency 계산
        if event_ts > 0:
            latency_ms = (local_ts - event_ts) / 1000.0
            if latency_ms > 0:
                self.latencies.append(latency_ms)
        
        return None
    
    def _process_depth(self, data: Dict, local_ts: int) -> Optional[Dict]:
        """Depth Update 처리"""
        # 중복 체크 (first_update_id + final_update_id)
        first_id = data.get('U', 0)
        final_id = data.get('u', 0)
        depth_id = f"{first_id}-{final_id}"
        
        if self._is_duplicate('depth', depth_id):
            self.stats['messages_duplicate'] += 1
            return None
        
        # out-of-order 체크
        event_ts = data.get('E', 0) * 1000
        self._check_out_of_order('depth', event_ts)
        
        self.stats['depth_updates'] += 1
        self.stats['messages_processed'] += 1
        
        # Latency
        if event_ts > 0:
            latency_ms = (local_ts - event_ts) / 1000.0
            if latency_ms > 0:
                self.latencies.append(latency_ms)
        
        # Orderbook 업데이트
        for bid in data.get('b', []):
            price, qty = float(bid[0]), float(bid[1])
            if qty == 0:
                self.orderbook.bid_levels.pop(price, None)
            else:
                self.orderbook.bid_levels[price] = qty
        
        for ask in data.get('a', []):
            price, qty = float(ask[0]), float(ask[1])
            if qty == 0:
                self.orderbook.ask_levels.pop(price, None)
            else:
                self.orderbook.ask_levels[price] = qty
        
        self.orderbook.timestamp = event_ts
        
        if not self.orderbook_initialized and self.orderbook.bid_levels and self.orderbook.ask_levels:
            self.orderbook_initialized = True
        
        return None
    
    def _process_liquidation(self, data: Dict, local_ts: int) -> Dict:
        """Liquidation 처리"""
        self.stats['liquidations'] += 1
        self.stats['messages_processed'] += 1
        
        order = data.get('o', {})
        event_ts = data.get('E', 0) * 1000
        
        self.last_liquidation_ts = event_ts
        self.last_liquidation_size = float(order.get('q', 0))
        
        event = {
            'timestamp': event_ts,
            'side': order.get('S'),
            'quantity': self.last_liquidation_size,
            'price': float(order.get('p', 0)),
        }
        self.liquidation_events.append(event)
        
        return {'type': 'LIQUIDATION', **event}
    
    def _process_ticker(self, data: Dict, local_ts: int) -> Dict:
        """Ticker 처리 - Checkpoint"""
        self.stats['tickers'] += 1
        self.stats['messages_processed'] += 1
        self.current_ticker = data
        event_ts = data.get('E', 0) * 1000
        
        # 스트림 건강 상태 체크 (ticker 처리 시)
        self._check_stream_health()
        
        if not self.orderbook_initialized:
            return {'type': 'SKIP', 'reason': 'orderbook_not_initialized'}
        
        # 이전 상태 저장
        prev_state = SystemState(
            data_trust=self.current_state.data_trust,
            hypothesis=self.current_state.hypothesis,
        )
        
        # Uncertainty 계산
        uncertainty = UncertaintyVector(
            freshness=self._calculate_freshness(),
            integrity=self._calculate_integrity(),
            stability=self._calculate_stability(event_ts),
            timestamp=event_ts
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
        
        # State 전이 감지
        if (prev_state.data_trust != self.current_state.data_trust or
            prev_state.hypothesis != self.current_state.hypothesis):
            
            self.state_transitions.append({
                'ts': event_ts,
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
        
        # HALT/RESTRICT 로깅
        if decision in [DecisionPermissionState.HALTED, DecisionPermissionState.RESTRICTED]:
            self.decisions_log.append({
                'ts': event_ts,
                'action': decision.value,
                'reason': {
                    'data_trust': self.current_state.data_trust.value,
                    'hypothesis': self.current_state.hypothesis.value,
                    'trust_reasons': self.current_state.trust_reasons,
                    'hypothesis_reasons': self.current_state.hypothesis_reasons,
                }
            })
        
        return {
            'type': 'DECISION',
            'decision': decision.value,
            'data_trust': self.current_state.data_trust.value,
            'hypothesis': self.current_state.hypothesis.value,
        }
    
    def _calculate_freshness(self) -> FreshnessUncertainty:
        if not self.latencies:
            return FreshnessUncertainty()
        
        latencies = list(self.latencies)
        avg = sum(latencies) / len(latencies)
        max_lat = max(latencies)
        
        stale_count = sum(1 for l in latencies if l > self.th.stale_threshold_ms)
        stale_ratio = stale_count / len(latencies)
        
        return FreshnessUncertainty(
            avg_lateness_ms=avg,
            max_lateness_ms=max_lat,
            stale_event_ratio=stale_ratio
        )
    
    def _calculate_integrity(self) -> IntegrityUncertainty:
        ticker_dict = {
            'last_price': float(self.current_ticker.get('c', 0)),
            'funding_rate': float(self.current_ticker.get('r', 0)) if 'r' in self.current_ticker else None,
        }
        return self.consistency_checker.check_integrity(
            ticker_data=ticker_dict,
            orderbook=self.orderbook
        )
    
    def _calculate_stability(self, current_ts: int) -> StabilityUncertainty:
        stability = StabilityUncertainty()
        
        if len(self.spreads) >= 2:
            spreads = list(self.spreads)
            avg = sum(spreads) / len(spreads)
            if avg > 0:
                variance = sum((s - avg) ** 2 for s in spreads) / len(spreads)
                stability.spread_volatility = (variance ** 0.5) / avg
        
        if self.last_liquidation_ts:
            time_since = (current_ts - self.last_liquidation_ts) / 1000.0
            stability.time_since_liquidation_ms = time_since
            stability.liquidation_size = self.last_liquidation_size
            stability.post_liquidation_stable = time_since >= self.th.liquidation_cooldown_ms
        
        return stability
    
    def _update_spread_history(self):
        spread = self.orderbook.get_spread()
        if spread is not None and spread > 0:
            self.spreads.append(spread)
    
    def print_status(self):
        total = sum(self.decision_counts.values())
        if total == 0:
            return
        
        allowed_pct = self.decision_counts['ALLOWED'] / total * 100
        halted_pct = self.decision_counts['HALTED'] / total * 100
        
        elapsed = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        # Robustness 지표 추가
        dup = self.stats['messages_duplicate']
        ooo = self.stats['messages_out_of_order']
        
        print(f"\r[{elapsed:5.1f}s] Tickers: {self.stats['tickers']:>4} | "
              f"ALLOWED: {allowed_pct:5.1f}% | HALTED: {halted_pct:5.1f}% | "
              f"Dup: {dup} | OoO: {ooo}", 
              end='', flush=True)
    
    def save_outputs(self):
        output_path = Path(self.config.output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        print(f"\n\n📁 결과 저장: {output_path}")
        
        # state_transitions.jsonl
        with open(output_path / "state_transitions.jsonl", 'w') as f:
            for t in self.state_transitions:
                f.write(json.dumps(t) + '\n')
        
        # decisions.jsonl
        with open(output_path / "decisions.jsonl", 'w') as f:
            for d in self.decisions_log:
                f.write(json.dumps(d) + '\n')
        
        # summary.json
        total = sum(self.decision_counts.values())
        summary = {
            'phase': 'realtime',
            'config': {
                'symbol': self.config.symbol,
                'duration_sec': self.config.duration_sec,
            },
            'thresholds_used': get_thresholds_dict(),
            'stats': self.stats,
            'decision_distribution': {
                'counts': self.decision_counts,
                'rates': {
                    k: round(v / total * 100, 2) if total > 0 else 0
                    for k, v in self.decision_counts.items()
                }
            },
            'robustness': {
                'reconnects': self.stats['reconnects'],
                'duplicates_filtered': self.stats['messages_duplicate'],
                'out_of_order_events': self.stats['messages_out_of_order'],
                'stream_stale_events': self.stats['stream_stale_events'],
            },
            'state_transitions_count': len(self.state_transitions),
        }
        with open(output_path / "summary.json", 'w') as f:
            json.dump(summary, f, indent=2)
        
        # robustness_events.jsonl
        if self.robustness_events:
            with open(output_path / "robustness_events.jsonl", 'w') as f:
                for e in self.robustness_events:
                    f.write(json.dumps(e) + '\n')
        
        print(f"  ✅ 저장 완료")
    
    def print_summary(self):
        total = sum(self.decision_counts.values())
        
        print("\n" + "=" * 70)
        print("📊 Realtime Validation 결과")
        print("=" * 70)
        
        print("\n[Decision Distribution]")
        for decision, count in self.decision_counts.items():
            pct = count / total * 100 if total > 0 else 0
            print(f"  {decision}: {count:,} ({pct:.1f}%)")
        
        print(f"\n[Robustness Statistics]")
        print(f"  Messages Total:      {self.stats['messages_total']:,}")
        print(f"  Messages Processed:  {self.stats['messages_processed']:,}")
        print(f"  Duplicates Filtered: {self.stats['messages_duplicate']:,}")
        print(f"  Out-of-Order:        {self.stats['messages_out_of_order']:,}")
        print(f"  Reconnects:          {self.stats['reconnects']:,}")
        print(f"  Stream Stale Events: {self.stats['stream_stale_events']:,}")
        
        print(f"\n[Stream Health]")
        for name, health in self.stream_health.items():
            status = "⚠️ STALE" if health.is_stale else "✅ OK"
            print(f"  {name:12}: {status} (count: {health.message_count:,})")
        
        print("=" * 70)


async def run_realtime(config: Optional[RealtimeConfig] = None):
    """
    Realtime Validation 실행 (Robust)
    
    자동 재연결 포함
    """
    config = config or REALTIME_CONFIG
    processor = RealtimeProcessor(config)
    uri = config.get_stream_uri()
    
    print("=" * 70)
    print("🚀 Phase 2: Realtime Validation (Robust)")
    print("=" * 70)
    print(f"Symbol:   {config.symbol.upper()}")
    print(f"Duration: {config.duration_sec}초")
    print(f"\n[Robustness Features]")
    print(f"  ✅ 자동 재연결")
    print(f"  ✅ 중복 메시지 필터링")
    print(f"  ✅ Out-of-order 허용")
    print(f"  ✅ Burst 처리")
    print(f"  ✅ 스트림 상태 모니터링")
    print("=" * 70)
    
    processor.start_time = datetime.now()
    start_ts = asyncio.get_event_loop().time()
    
    while True:
        elapsed = asyncio.get_event_loop().time() - start_ts
        if elapsed >= config.duration_sec:
            break
        
        try:
            async with websockets.connect(
                uri,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                if processor.reconnect_count > 0:
                    print(f"\n  🔄 재연결 성공 (#{processor.reconnect_count})")
                else:
                    print("✅ WebSocket 연결 성공!\n")
                
                while True:
                    elapsed = asyncio.get_event_loop().time() - start_ts
                    if elapsed >= config.duration_sec:
                        break
                    
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        result = processor.process_message(message)
                        
                        if result and result.get('type') == 'LIQUIDATION':
                            print(f"\n  ⚠️ LIQUIDATION: {result['side']} "
                                  f"{result['quantity']:.4f} @ {result['price']:,.2f}")
                        
                        if processor.stats['tickers'] > 0 and processor.stats['tickers'] % config.log_interval == 0:
                            processor.print_status()
                    
                    except asyncio.TimeoutError:
                        continue
        
        except (websockets.exceptions.ConnectionClosed, 
                websockets.exceptions.ConnectionClosedError,
                ConnectionResetError,
                OSError) as e:
            # 네트워크 에러 → 재연결 시도
            processor._log_reconnect()
            print(f"\n  ⚠️ 연결 끊김: {e}. 3초 후 재연결...")
            await asyncio.sleep(3)
            continue
        
        except Exception as e:
            # 기타 에러 → 로깅하고 계속
            print(f"\n  ❌ 에러: {e}")
            await asyncio.sleep(1)
            continue
    
    processor.print_status()
    processor.save_outputs()
    processor.print_summary()