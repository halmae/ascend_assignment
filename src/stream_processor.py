"""
실시간 스트림 처리 엔진
"""
from collections import deque
from typing import List, Optional, Dict, Deque
from src.enums import EventType, DataTrustState, RepairAction
from src.data_types import Event, OrderbookState
from src.consistency import ConsistencyChecker
from src.orderbook import OrderbookMetrics
from src.config import DEFAULT_PROCESSOR_CONFIG, DEFAULT_CONSISTENCY_CONFIG


class StreamProcessor:
    """실시간 스트림 처리 엔진"""

    def __init__(self,
                 buffer_size: int = 1000,
                 watermark_delay_ms: int = 50,
                 snapshot_buffer_size: int = 5,
                 processor_config = None,
                 consistency_config = None):
        """
        Args:
            buffer_size: 메인 버퍼 크기
            watermark_delay_ms: Watermark 지연 시간 (ms)
            snapshot_buffer_size: Snapshot 버퍼 크기
            processor_config: StreamProcessorConfig 인스턴스
            consistency_config: ConsistencyConfig 인스턴스
        """
        self.buffer_size = buffer_size
        self.watermark_delay_ms = watermark_delay_ms
        self.snapshot_buffer_size = snapshot_buffer_size

        # Config
        self.processor_config = processor_config or DEFAULT_PROCESSOR_CONFIG
        self.consistency_config = consistency_config or DEFAULT_CONSISTENCY_CONFIG

        # Buffers
        self.main_buffer: List[Event] = []
        self.snapshot_buffer: Deque[Event] = deque(maxlen=snapshot_buffer_size)

        # Orderbook state
        self.current_orderbook: Optional[OrderbookState] = None
        self.prev_orderbook: Optional[OrderbookState] = None

        # System state
        self.data_trust_state = DataTrustState.UNTRUSTED
        self.initialized = False   # Snapshot을 받았는지

        # Statistics
        self.stats = {
            'events_received': 0,
            'events_processed': 0,
            'trades_processed': 0,
            'orderbook_updates': 0,
            'ticker_checkpoints': 0,
            'snapshots_used': 0,
            'repairs': 0,
            'quarantines': 0,
            'accepts': 0,
        }

        # Consistency Checker
        self.consistency_checker = ConsistencyChecker(consistency_config)

        # last watermark
        self.last_watermark = None
        self.last_ticker_ts = None


    def add_event(self, event: Event):
        """
        이벤트를 버퍼에 추가

        Args:
            event: Event 인스턴스
        """
        self.stats['events_received'] += 1

        # Snapshot은 별도 버퍼로
        if event.event_type == EventType.ORDERBOOK and event.data.get('is_snapshot', False):
            self.snapshot_buffer.append(event)
            print(f"📸 Snapshot 수신: ts={event.timestamp}, buffer size={len(self.snapshot_buffer)}/{self.snapshot_buffer_size}")
            return
        
        # 나머지는 메인 버퍼로
        self.main_buffer.append(event)

        # Ticker가 오면 즉시 처리
        if event.event_type == EventType.TICKER:
            self.last_ticker_ts = event.timestamp
            self.process_buffer()

    
    def process_buffer(self):
        """버퍼의 이벤트들을 처리 (Watermark 기반)"""
        if not self.main_buffer:
            return
        
        # Watermark 계산
        if self.last_ticker_ts:
            watermark = self.last_ticker_ts
        else:
            last_event_ts = max(e.timestamp for e in self.main_buffer)
            watermark = last_event_ts - (self.watermark_delay_ms * 1000)    # ms -> us

        self.last_watermark = watermark

        # Watermark 이하의 이벤트만 처리
        to_process = [e for e in self.main_buffer if e.timestamp <= watermark]
        self.main_buffer = [e for e in self.main_buffer if e.timestamp > watermark]

        # 시간순 정렬
        to_process.sort(key=lambda e: e.timestamp)

        # 처리
        for event in to_process:
            self._dispatch_event(event)


    def _dispatch_event(self, event: Event):
        """이벤트 타입별로 처리 분기"""
        if event.event_type == EventType.ORDERBOOK:
            self._process_orderbook(event)
        elif event.event_type == EventType.TRADE:
            self._process_trade(event)
        elif event.event_type == EventType.TICKER:
            self._process_ticker(event)
        elif event.event_type == EventType.LIQUIDATION:
            self._process_liquidation(event)

        self.stats['events_processed'] += 1

    def _process_orderbook(self, event: Event):
        """Orderbook 업데이트 처리"""
        
        if not self.initialized:
            snapshot = self._get_latest_snapshot(before_timestamp=event.timestamp)
            if snapshot:
                self.current_orderbook = self._rebuild_orderbook_from_snapshot(snapshot)
                self.initialized = True
            else:
                # Snapshot이 없으면 처리 불가
                return
            
        # Prev 상태 저장
        if self.current_orderbook:
            self.prev_orderbook = self.current_orderbook.clone(event.timestamp)

        # Orderbook Update
        data = event.data
        price = float(data['price'])
        amount = float(data['amount'])
        side = data['side']

        if side == 'bid':
            if amount == 0:
                # 삭제
                self.current_orderbook.bid_levels.pop(price, None)
            else:
                self.current_orderbook.bid_levels[price] = amount
        else:
            if amount == 0:
                # 삭제
                self.current_orderbook.ask_levels.pop(price, None)
            else:
                # 추가/업데이트
                self.current_orderbook.ask_levels[price] = amount
        
        self.current_orderbook.timestamp = event.timestamp
        self.stats['orderbook_updates'] += 1

    
    def _process_trade(self, event: Event):
        """Trade 처리 및 검증"""
        # 초기화되지 않았으면 무시
        if not self.initialized:
            return
        
        self.stats['trades_processed'] += 1
        
        # ⭐ 같은 timestamp의 orderbook update로 인한 trade인지 확인
        if self._is_orderbook_induced_trade(event):
            # Orderbook 업데이트가 유발한 trade (skip validation)
            self.stats['accepts'] += 1
            return
        
        # Trade validation
        action = self._validate_trade(event)
        
        if action == RepairAction.ACCEPT:
            self.stats['accepts'] += 1
        elif action == RepairAction.REPAIR:
            self.stats['repairs'] += 1
        elif action == RepairAction.QUARANTINE:
            self.stats['quarantines'] += 1


    def _is_orderbook_induced_trade(self, event: Event) -> bool:
        """
        Orderbook 업데이트가 직접 유발한 trade인지 확인
        
        조건:
        1. Timestamp가 정확히 같음
        2. Price가 일치
        3. Amount가 유사 (1% tolerance)
        4. Side가 매칭 (trade의 buy -> orderbook의 ask)
        
        Args:
            event: Trade Event
            
        Returns:
            True if orderbook-induced trade
        """
        if not self.current_orderbook:
            return False
        
        # 1. Timestamp 체크
        if self.current_orderbook.timestamp != event.timestamp:
            return False
        
        data = event.data
        trade_price = float(data['price'])
        trade_amount = float(data['amount'])
        trade_side = data['side']  # 'buy' or 'sell'
        
        # 2. Trade가 소진한 orderbook side 결정
        # Buy trade -> Ask orderbook을 소진
        # Sell trade -> Bid orderbook을 소진
        orderbook_side = 'ask' if trade_side == 'buy' else 'bid'
        
        # 3. 해당 price level이 현재 orderbook에 없는지 확인
        # (방금 소진되었으므로 없어야 정상)
        if orderbook_side == 'ask':
            levels = self.current_orderbook.ask_levels
        else:
            levels = self.current_orderbook.bid_levels
        
        # Price level이 존재하면 이상함 (아직 안 소진됨)
        if trade_price in levels:
            return False
        
        # 4. Prev orderbook에는 있었는지 확인
        if not self.prev_orderbook:
            return False
        
        if orderbook_side == 'ask':
            prev_levels = self.prev_orderbook.ask_levels
        else:
            prev_levels = self.prev_orderbook.bid_levels
        
        if trade_price not in prev_levels:
            return False
        
        # 5. Amount 체크 (1% tolerance)
        prev_amount = prev_levels[trade_price]
        tolerance = self.consistency_config.trade_amount_tolerance
        
        if abs(prev_amount - trade_amount) / trade_amount <= tolerance:
            # 완전히 소진됨 (orderbook-induced trade)
            return True
        
        return False


    def _validate_trade(self, event: Event) -> RepairAction:
        """
        Trade 검증

        Returns:
            RepairAction (ACCEPT, REPAIR, QUARANTINE)
        """
        if not self.prev_orderbook or not self.current_orderbook:
            return RepairAction.QUARANTINE
        
        data = event.data
        price = float(data['price'])
        amount = float(data['amount'])
        side = data['side']

        target_side = 'ask' if side == 'buy' else 'bid'

        prev_levels = (self.prev_orderbook.ask_levels if target_side == 'ask'
                       else self.prev_orderbook.bid_levels)
        curr_levels = (self.current_orderbook.ask_levels if target_side == 'ask'
                       else self.current_orderbook.bid_levels)
        
        if price not in prev_levels:
            return RepairAction.QUARANTINE
        
        # Amount 변화 확인
        prev_amount = prev_levels[price]
        curr_amount = curr_levels.get(price, 0)

        delta_amount = prev_amount - curr_amount

        tolerance = self.consistency_config.trade_amount_tolerance

        if abs(delta_amount - amount) / amount <= tolerance:
            return RepairAction.ACCEPT
        elif delta_amount > 0:
            return RepairAction.REPAIR
        else:
            return RepairAction.QUARANTINE
        

    def _process_ticker(self, event: Event):
        """Ticker 이벤트 처리 및 Consistency Check"""
        self.stats['ticker_checkpoints'] += 1
        
        # 변수 초기화
        consistency_score = 0.0
        result = None
        
        # Consistency check
        if not self.current_orderbook:
            # Orderbook이 아직 없으면
            consistency_score = 0.0
            self.data_trust_state = DataTrustState.UNTRUSTED
        else:
            # Orderbook이 있으면 consistency check
            result = self.consistency_checker.check_overall_consistency(
                ticker_data=event.data,
                orderbook=self.current_orderbook,
                total_events=self.stats['events_processed'],
                repairs=self.stats['repairs'],
                quarantines=self.stats['quarantines']
            )
            
            consistency_score = result['overall_score']
            
            # State 전환
            if consistency_score >= self.processor_config.trusted_threshold:
                self.data_trust_state = DataTrustState.TRUSTED
            elif consistency_score >= self.processor_config.degraded_threshold:
                self.data_trust_state = DataTrustState.DEGRADED
            else:
                self.data_trust_state = DataTrustState.UNTRUSTED
        
        # 로그 출력 (일정 간격마다)
        if self.stats['ticker_checkpoints'] % self.processor_config.consistency_log_interval == 0:
            if result is not None:
                # 정상적인 consistency check 결과가 있을 때
                self._print_consistency_check(event, result)
            else:
                # Orderbook이 없을 때 간단한 로그
                print(f"\n{'='*60}")
                print(f"🔔 Ticker Checkpoint #{self.stats['ticker_checkpoints']} at {event.timestamp}")
                print(f"{'='*60}")
                print(f"  Data Trust State: {self.data_trust_state.value}")
                print(f"  Consistency Score: {consistency_score:.2%}")
                print(f"  Events Processed: {self.stats['events_processed']}")
                print(f"  ⚠️ Orderbook not initialized yet")

        
    def _process_liquidation(self, event: Event):
        """Liquidation 이벤트 처리 (미래 구현)"""
        pass


    def _get_latest_snapshot(self, before_timestamp: int) -> Optional[Event]:
        """
        지정된 timestamp 이전의 가장 최근 snapshot 가져오기

        Args:
            before_timestamp: 이 시간 이전의 snapshot

        Returns:
            Event 또는 None
        """
        valid_snapshots = [
            s for s in self.snapshot_buffer if s.timestamp <= before_timestamp
        ]

        if not valid_snapshots:
            return None
        
        return max(valid_snapshots, key=lambda s: s.timestamp)
    

    def _rebuild_orderbook_from_snapshot(self, snapshot: Event) -> OrderbookState:
        """
        Snapshot으로부터 Orderbook 상태 재구성
        
        Args:
            snapshot: Snapshot Event
            
        Returns:
            OrderbookState
        """
        self.stats['snapshots_used'] += 1
        self.initialized = True
        
        print(f"🔄 Snapshot으로 Orderbook 재구성 중...")
        
        data = snapshot.data
        
        # Snapshot 데이터 구조 확인
        if 'bids' in data and 'asks' in data:
            # Grouped snapshot 형식
            bids = {float(price): float(amount) for price, amount in data['bids']}
            asks = {float(price): float(amount) for price, amount in data['asks']}
        else:
            # Single row 형식 (이전 방식, 혹시 모를 대비)
            bids = {}
            asks = {}
            if data.get('side') == 'bid':
                bids[float(data['price'])] = float(data['amount'])
            else:
                asks[float(data['price'])] = float(data['amount'])
        
        orderbook = OrderbookState(
            timestamp=snapshot.timestamp,
            bid_levels=bids,
            ask_levels=asks
        )
        
        print(f"📊 Orderbook 재구성: {len(bids)} bids, {len(asks)} asks")
        
        return orderbook
    

    def _print_consistency_check(self, event: Event, result: Dict):
        """Consistency check 결과 출력"""
        consistency_score = result['overall_score']
        
        print(f"\n{'='*60}")
        print(f"🔔 Ticker Checkpoint at {event.timestamp}")
        print(f"{'='*60}")
        print(f"  Data Trust State: {self.data_trust_state.value}")
        print(f"  Consistency Score: {consistency_score:.2%}")
        print(f"  Events Processed: {self.stats['events_processed']}")
        print(f"  Repairs: {self.stats['repairs']}")
        print(f"  Quarantines: {self.stats['quarantines']}")
        
        if self.current_orderbook:
            depth = OrderbookMetrics.calculate_depth(self.current_orderbook)
            print(f"  OB Depth (bid/ask): {depth['bid_depth']:.4f} / {depth['ask_depth']:.4f}")
        
        # Detailed breakdown
        print(f"\n{'='*60}")
        print(f"🔍 Consistency Check Details")
        print(f"{'='*60}")
        
        for key in ['price', 'spread', 'imbalance_funding', 'depth', 'system']:
            if key in result:
                score = result[key]['score']
                emoji = "✅" if score >= 0.8 else "⚠️"
                print(f"  {emoji} {key:20s}: {score:.2%}")
                
                # Price 세부사항
                if key == 'price' and 'details' in result[key]:
                    for detail_key, detail_score in result[key]['details'].items():
                        print(f"      - {detail_key}: {detail_score:.2%}")
        
        print(f"\n  Overall Score: {consistency_score:.2%}")
    
    def print_status(self):
        """현재 상태 출력"""
        print(f"\n{'='*60}")
        print(f"📊 StreamProcessor Status")
        print(f"{'='*60}")
        print(f"  Data Trust State: {self.data_trust_state.value}")
        print(f"  Initialized: {self.initialized}")
        print(f"\n  Buffer Status:")
        print(f"    Main Buffer: {len(self.main_buffer)} / {self.buffer_size}")
        print(f"    Snapshot Buffer: {len(self.snapshot_buffer)} / {self.snapshot_buffer_size}")
        print(f"\n  Statistics:")
        for key, value in self.stats.items():
            print(f"    {key}: {value:,}")
        
        if self.current_orderbook:
            print(f"\n  Current Orderbook:")
            print(f"    Timestamp: {self.current_orderbook.timestamp}")
            print(f"    Bid Levels: {len(self.current_orderbook.bid_levels)}")
            print(f"    Ask Levels: {len(self.current_orderbook.ask_levels)}")
            
            depth = OrderbookMetrics.calculate_depth(self.current_orderbook)
            print(f"    Bid Depth: {depth['bid_depth']:.4f} BTC")
            print(f"    Ask Depth: {depth['ask_depth']:.4f} BTC")