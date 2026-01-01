"""
Realtime Processor - WebSocket 기반 실시간 처리

================================================================================
Single Decision Engine:
- config.py의 THRESHOLDS를 참조
- Historical StreamProcessor와 동일한 파라미터 사용
================================================================================
"""
import asyncio
import websockets
import json
from datetime import datetime
from collections import deque
from pathlib import Path
from typing import Dict, Optional, List

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


class RealtimeProcessor:
    """
    실시간 Decision Engine
    
    config.py의 THRESHOLDS 사용 (Single Decision Engine)
    """
    
    def __init__(self, config: Optional[RealtimeConfig] = None):
        self.config = config or REALTIME_CONFIG
        
        # config.py 참조
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
            'messages': 0,
            'trades': 0,
            'depth_updates': 0,
            'tickers': 0,
            'liquidations': 0,
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
        
        # Timing
        self.start_time: Optional[datetime] = None
    
    def process_message(self, message: str) -> Optional[Dict]:
        """WebSocket 메시지 처리"""
        self.stats['messages'] += 1
        
        data = json.loads(message)
        payload = data.get('data', {})
        event_type = payload.get('e', 'unknown')
        
        local_ts = int(datetime.now().timestamp() * 1000000)
        
        if event_type == 'trade':
            self._process_trade(payload, local_ts)
        elif event_type == 'depthUpdate':
            self._process_depth(payload, local_ts)
        elif event_type == 'forceOrder':
            return self._process_liquidation(payload, local_ts)
        elif event_type == '24hrTicker':
            return self._process_ticker(payload, local_ts)
        
        return None
    
    def _process_trade(self, data: Dict, local_ts: int):
        """Trade 처리"""
        self.stats['trades'] += 1
        
        event_ts = data.get('E', 0) * 1000
        if event_ts > 0:
            latency_ms = (local_ts - event_ts) / 1000.0
            if latency_ms > 0:
                self.latencies.append(latency_ms)
    
    def _process_depth(self, data: Dict, local_ts: int):
        """Depth Update 처리"""
        self.stats['depth_updates'] += 1
        
        event_ts = data.get('E', 0) * 1000
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
    
    def _process_liquidation(self, data: Dict, local_ts: int) -> Dict:
        """Liquidation 처리"""
        self.stats['liquidations'] += 1
        
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
        self.current_ticker = data
        event_ts = data.get('E', 0) * 1000
        
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
        """Freshness 계산"""
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
        """Integrity 계산"""
        ticker_dict = {
            'last_price': float(self.current_ticker.get('c', 0)),
            'funding_rate': float(self.current_ticker.get('r', 0)) if 'r' in self.current_ticker else None,
        }
        return self.consistency_checker.check_integrity(
            ticker_data=ticker_dict,
            orderbook=self.orderbook
        )
    
    def _calculate_stability(self, current_ts: int) -> StabilityUncertainty:
        """Stability 계산"""
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
        """Spread history 업데이트"""
        spread = self.orderbook.get_spread()
        if spread is not None and spread > 0:
            self.spreads.append(spread)
    
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
              f"Liquidations: {self.stats['liquidations']}", 
              end='', flush=True)
    
    def save_outputs(self):
        """결과 저장"""
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
            'state_transitions_count': len(self.state_transitions),
        }
        with open(output_path / "summary.json", 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"  ✅ 저장 완료")
    
    def print_summary(self):
        """최종 결과 출력"""
        total = sum(self.decision_counts.values())
        
        print("\n" + "=" * 60)
        print("📊 Realtime Validation 결과")
        print("=" * 60)
        
        for decision, count in self.decision_counts.items():
            pct = count / total * 100 if total > 0 else 0
            print(f"  {decision}: {count:,} ({pct:.1f}%)")
        
        print(f"\n  State Transitions: {len(self.state_transitions)}")
        print(f"  Liquidations: {self.stats['liquidations']}")
        print("=" * 60)


async def run_realtime(config: Optional[RealtimeConfig] = None):
    """Realtime Validation 실행"""
    config = config or REALTIME_CONFIG
    processor = RealtimeProcessor(config)
    uri = config.get_stream_uri()
    
    print("=" * 60)
    print("🚀 Phase 2: Realtime Validation")
    print("=" * 60)
    print(f"Symbol:   {config.symbol.upper()}")
    print(f"Duration: {config.duration_sec}초")
    print(f"\n[Thresholds from config.py]")
    print(f"  liquidation_cooldown_ms: {THRESHOLDS.liquidation_cooldown_ms}")
    print(f"  integrity_repair_bps:    {THRESHOLDS.integrity_repair_threshold_bps}")
    print("=" * 60)
    
    processor.start_time = datetime.now()
    start_ts = asyncio.get_event_loop().time()
    
    try:
        async with websockets.connect(uri) as ws:
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
                    
                    if processor.stats['tickers'] % config.log_interval == 0:
                        processor.print_status()
                
                except asyncio.TimeoutError:
                    continue
            
            processor.print_status()
            processor.save_outputs()
            processor.print_summary()
            
    except Exception as e:
        print(f"\n❌ 에러: {e}")
        raise