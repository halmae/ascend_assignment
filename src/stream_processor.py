"""
실시간 스트림 처리 엔진 (Event Lateness 기반 필터링)

핵심 개념:
- lateness = local_timestamp - timestamp (이벤트 수신 지연)
- lateness가 큰 이벤트는 "stale"하므로 버림
- Effective Orderbook = stale 이벤트 제외된 Orderbook
- 이 Effective OB의 consistency를 평가
"""
import time
from collections import deque
from typing import List, Optional, Dict, Deque

from src.enums import EventType, DataTrustState, RepairAction
from src.data_types import Event, OrderbookState
from src.consistency import ConsistencyChecker, CheckResult, ConsistencyReport
from src.orderbook import OrderbookMetrics
from src.results import ProcessingResult


class StreamProcessor:
    """실시간 스트림 처리 엔진"""

    def __init__(self,
                 buffer_size: int = 1000,
                 watermark_delay_ms: int = 50,
                 snapshot_buffer_size: int = 5,
                 dataset_name: str = "",
                 max_orderbook_levels: int = 500,
                 max_event_lateness_ms: float = 100.0):
        """
        Args:
            buffer_size: 메인 버퍼 크기
            watermark_delay_ms: Watermark 지연 시간 (ms)
            snapshot_buffer_size: Snapshot 버퍼 크기
            dataset_name: 데이터셋 이름 (결과 식별용)
            max_orderbook_levels: Orderbook 최대 레벨 수
            max_event_lateness_ms: 이벤트 최대 허용 lateness (ms)
                                   초과시 이벤트를 버림
        """
        self.buffer_size = buffer_size
        self.watermark_delay_ms = watermark_delay_ms
        self.snapshot_buffer_size = snapshot_buffer_size
        self.dataset_name = dataset_name
        self.max_orderbook_levels = max_orderbook_levels
        self.max_event_lateness_ms = max_event_lateness_ms

        # Buffers
        self.main_buffer: List[Event] = []
        self.snapshot_buffer: Deque[Event] = deque(maxlen=snapshot_buffer_size)

        # Orderbook state (Effective Orderbook)
        self.current_orderbook: Optional[OrderbookState] = None
        self.prev_orderbook: Optional[OrderbookState] = None

        # System state
        self.data_trust_state = DataTrustState.UNTRUSTED
        self.initialized = False

        # Statistics
        self.stats = {
            'events_received': 0,
            'events_processed': 0,
            'events_dropped': 0,  # lateness 초과로 버린 이벤트
            'trades_processed': 0,
            'trades_dropped': 0,
            'orderbook_updates': 0,
            'orderbook_dropped': 0,
            'ticker_checkpoints': 0,
            'snapshots_received': 0,
            'snapshots_used': 0,
            'trade_accepts': 0,
            'trade_quarantines': 0,
        }
        
        # Event Lateness 통계 (이벤트 타입별)
        self.lateness_stats = {
            'orderbook': {
                'count': 0,
                'sum_ms': 0.0,
                'max_ms': 0.0,
                'dropped': 0,
            },
            'trade': {
                'count': 0,
                'sum_ms': 0.0,
                'max_ms': 0.0,
                'dropped': 0,
            },
            'ticker': {
                'count': 0,
                'sum_ms': 0.0,
                'max_ms': 0.0,
                'dropped': 0,
            },
        }
        
        # Consistency Check 통계 (3가지)
        self.check_stats = {
            'passes': {
                'spread_valid': 0,
                'price_in_spread': 0,
                'funding_imbalance_aligned': 0
            },
            'failures': {
                'spread_valid': 0,
                'price_in_spread': 0,
                'funding_imbalance_aligned': 0
            }
        }
        
        # State 분포
        self.state_counts = {
            'TRUSTED': 0,
            'DEGRADED': 0,
            'UNTRUSTED': 0
        }
        
        # State 전이 기록
        self.state_transitions: List[Dict] = []

        # Consistency Checker
        self.consistency_checker = ConsistencyChecker()

        # Watermark
        self.last_watermark = None
        self.last_ticker_ts = None
        
        # 처리 시작 시간
        self.start_time = None


    def _calculate_lateness_ms(self, event: Event) -> float:
        """
        이벤트의 lateness 계산 (ms)
        
        lateness = local_timestamp - timestamp
                 = 수신 시점 - 발생 시점
                 = 네트워크/처리 지연
        """
        return (event.local_timestamp - event.timestamp) / 1000.0
    
    
    def _update_lateness_stats(self, event_type: str, lateness_ms: float, dropped: bool):
        """이벤트 타입별 lateness 통계 업데이트"""
        if event_type not in self.lateness_stats:
            return
        
        stats = self.lateness_stats[event_type]
        stats['count'] += 1
        stats['sum_ms'] += lateness_ms
        if lateness_ms > stats['max_ms']:
            stats['max_ms'] = lateness_ms
        if dropped:
            stats['dropped'] += 1
    
    
    def _is_stale_event(self, event: Event, event_type: str) -> bool:
        """
        이벤트가 stale한지 판단
        
        Returns:
            True: lateness > threshold → 버려야 함
            False: 정상 → 적용해야 함
        """
        lateness_ms = self._calculate_lateness_ms(event)
        is_stale = lateness_ms > self.max_event_lateness_ms
        
        # 통계 업데이트
        self._update_lateness_stats(event_type, lateness_ms, is_stale)
        
        return is_stale


    def add_event(self, event: Event):
        """이벤트를 버퍼에 추가"""
        if self.start_time is None:
            self.start_time = time.time()
            
        self.stats['events_received'] += 1

        # Snapshot은 별도 버퍼로 (lateness 체크 안 함 - snapshot은 항상 필요)
        if event.event_type == EventType.ORDERBOOK and event.data.get('is_snapshot', False):
            self.snapshot_buffer.append(event)
            self.stats['snapshots_received'] += 1
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
            watermark = last_event_ts - (self.watermark_delay_ms * 1000)

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
        """
        Orderbook 업데이트 처리
        
        Stale 이벤트는 버리고, 신선한 이벤트만 Effective Orderbook에 반영
        """
        # 초기화 체크
        if not self.initialized:
            snapshot = self._get_latest_snapshot(before_timestamp=event.timestamp)
            if snapshot:
                self.current_orderbook = self._rebuild_orderbook_from_snapshot(snapshot)
                self.initialized = True
            else:
                return
        
        # Lateness 체크 - stale 이벤트는 버림
        if self._is_stale_event(event, 'orderbook'):
            self.stats['events_dropped'] += 1
            self.stats['orderbook_dropped'] += 1
            return  # Effective Orderbook에 반영하지 않음
            
        # Prev 상태 저장
        if self.current_orderbook:
            self.prev_orderbook = self.current_orderbook.clone(event.timestamp)

        # Effective Orderbook에 업데이트 적용
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
        
        # 주기적으로 Orderbook 크기 제한 (1000번마다)
        if self.stats['orderbook_updates'] % 1000 == 0:
            self._trim_orderbook()


    def _trim_orderbook(self):
        """Orderbook 크기 제한 - best price 기준 상위 N개만 유지"""
        if not self.current_orderbook:
            return
        
        max_levels = self.max_orderbook_levels
        bids = self.current_orderbook.bid_levels
        asks = self.current_orderbook.ask_levels
        
        # Bid: 높은 가격 순으로 상위 N개만 유지
        if len(bids) > max_levels:
            sorted_prices = sorted(bids.keys(), reverse=True)[:max_levels]
            self.current_orderbook.bid_levels = {p: bids[p] for p in sorted_prices}
        
        # Ask: 낮은 가격 순으로 상위 N개만 유지
        if len(asks) > max_levels:
            sorted_prices = sorted(asks.keys())[:max_levels]
            self.current_orderbook.ask_levels = {p: asks[p] for p in sorted_prices}

    
    def _process_trade(self, event: Event):
        """
        Trade 처리 및 검증
        
        Stale trade는 버림
        """
        if not self.initialized:
            return
        
        # Lateness 체크 - stale trade는 버림
        if self._is_stale_event(event, 'trade'):
            self.stats['events_dropped'] += 1
            self.stats['trades_dropped'] += 1
            return
        
        self.stats['trades_processed'] += 1
        
        # Trade validation (Effective Orderbook 기준)
        action = self._validate_trade(event)
        
        if action == RepairAction.ACCEPT:
            self.stats['trade_accepts'] += 1
        elif action == RepairAction.QUARANTINE:
            self.stats['trade_quarantines'] += 1


    def _validate_trade(self, event: Event) -> RepairAction:
        """
        Trade 검증 (Effective Orderbook 기준)
        """
        if not self.current_orderbook:
            return RepairAction.QUARANTINE
        
        price = float(event.data['price'])
        
        best_bid = self.current_orderbook.get_best_bid()
        best_ask = self.current_orderbook.get_best_ask()
        
        if best_bid is None or best_ask is None:
            return RepairAction.QUARANTINE
        
        spread = best_ask - best_bid
        mid_price = (best_bid + best_ask) / 2
        
        # 상대값과 절대값 중 큰 것 사용
        relative_margin = spread * 0.5
        absolute_margin = mid_price * 0.0001  # 1bp
        margin = max(relative_margin, absolute_margin)
        
        # 범위 체크
        if price < best_bid - margin:
            return RepairAction.QUARANTINE
        if price > best_ask + margin:
            return RepairAction.QUARANTINE
        
        return RepairAction.ACCEPT


    def _process_ticker(self, event: Event):
        """
        Ticker 이벤트 처리
        
        Ticker는 버리지 않고, lateness만 기록
        (Ticker가 consistency check 트리거이므로)
        """
        # Lateness 통계만 기록 (버리지 않음)
        lateness_ms = self._calculate_lateness_ms(event)
        self._update_lateness_stats('ticker', lateness_ms, dropped=False)
        
        self.stats['ticker_checkpoints'] += 1
        
        # 이전 상태 저장
        prev_state = self.data_trust_state
        
        # Effective Orderbook에 대해 Consistency check
        report = self.consistency_checker.check_all(
            ticker_data=event.data,
            orderbook=self.current_orderbook
        )
        
        # Check 통계 업데이트
        for check_name, result in report.checks.items():
            if check_name in self.check_stats['passes']:
                if result == CheckResult.PASS:
                    self.check_stats['passes'][check_name] += 1
                elif result == CheckResult.FAIL:
                    self.check_stats['failures'][check_name] += 1
        
        # Drop rate 기반 추가 판단
        ob_drop_rate = self._get_recent_drop_rate()
        
        # State 결정 로직
        # 1. Drop rate가 높으면 데이터 품질 문제 → 강등
        # 2. Consistency check 결과로 최종 판단
        
        if ob_drop_rate > 0.1:  # 10% 이상 drop
            # 데이터 품질 심각 → UNTRUSTED
            self.data_trust_state = DataTrustState.UNTRUSTED
        elif ob_drop_rate > 0.01:  # 1% 이상 drop
            # 데이터 품질 저하 → 최대 DEGRADED
            if report.all_passed:
                self.data_trust_state = DataTrustState.DEGRADED
            else:
                self.data_trust_state = DataTrustState.UNTRUSTED
        else:
            # Drop rate 정상 → consistency check 결과로 판단
            if report.all_passed:
                self.data_trust_state = DataTrustState.TRUSTED
            elif report.fail_count <= 1:
                self.data_trust_state = DataTrustState.DEGRADED
            else:
                self.data_trust_state = DataTrustState.UNTRUSTED
        
        # State 분포 업데이트
        self.state_counts[self.data_trust_state.value] += 1
        
        # State 전이 기록 (변경되었을 때만)
        if prev_state != self.data_trust_state:
            failed_checks = [k for k, v in report.checks.items() if v == CheckResult.FAIL]
            self.state_transitions.append({
                'timestamp': event.timestamp,
                'from_state': prev_state.value,
                'to_state': self.data_trust_state.value,
                'failed_checks': failed_checks,
                'fail_count': report.fail_count,
                'ob_drop_rate': ob_drop_rate
            })
        
        # 로그 (100개마다)
        if self.stats['ticker_checkpoints'] % 100 == 0:
            failed = [k for k, v in report.checks.items() if v == CheckResult.FAIL]
            print(f"Ticker #{self.stats['ticker_checkpoints']}: "
                  f"{self.data_trust_state.value}, failed={failed}, "
                  f"drop_rate={ob_drop_rate:.2%}")

    
    def _get_recent_drop_rate(self) -> float:
        """
        Orderbook 이벤트 drop rate 계산
        
        Returns:
            dropped / (processed + dropped)
        """
        processed = self.stats['orderbook_updates']
        dropped = self.stats['orderbook_dropped']
        total = processed + dropped
        
        if total == 0:
            return 0.0
        
        return dropped / total

        
    def _process_liquidation(self, event: Event):
        """Liquidation 이벤트 처리 (미래 구현)"""
        pass


    def _get_latest_snapshot(self, before_timestamp: int) -> Optional[Event]:
        """지정된 timestamp 이전의 가장 최근 snapshot"""
        valid_snapshots = [
            s for s in self.snapshot_buffer if s.timestamp <= before_timestamp
        ]
        if not valid_snapshots:
            return None
        return max(valid_snapshots, key=lambda s: s.timestamp)
    

    def _rebuild_orderbook_from_snapshot(self, snapshot: Event) -> OrderbookState:
        """Snapshot으로부터 Orderbook 상태 재구성"""
        self.stats['snapshots_used'] += 1
        self.initialized = True
        
        data = snapshot.data
        
        if 'bids' in data and 'asks' in data:
            bids = {float(price): float(amount) for price, amount in data['bids']}
            asks = {float(price): float(amount) for price, amount in data['asks']}
        else:
            bids = {}
            asks = {}
            if data.get('side') == 'bid':
                bids[float(data['price'])] = float(data['amount'])
            else:
                asks[float(data['price'])] = float(data['amount'])
        
        return OrderbookState(
            timestamp=snapshot.timestamp,
            bid_levels=bids,
            ask_levels=asks
        )
    

    def get_result(self) -> ProcessingResult:
        """처리 결과 반환"""
        processing_time = time.time() - self.start_time if self.start_time else 0
        
        # 평균 lateness 계산 (이벤트 타입별)
        avg_lateness = {}
        for event_type, stats in self.lateness_stats.items():
            if stats['count'] > 0:
                avg_lateness[event_type] = stats['sum_ms'] / stats['count']
            else:
                avg_lateness[event_type] = 0.0
        
        result = ProcessingResult(
            dataset_name=self.dataset_name,
            processing_time_sec=processing_time,
            total_events=self.stats['events_processed'],
            total_trades=self.stats['trades_processed'],
            total_tickers=self.stats['ticker_checkpoints'],
            total_orderbook_updates=self.stats['orderbook_updates'],
            total_snapshots=self.stats['snapshots_used'],
            trade_accepts=self.stats['trade_accepts'],
            trade_quarantines=self.stats['trade_quarantines'],
            events_dropped=self.stats['events_dropped'],
            orderbook_dropped=self.stats['orderbook_dropped'],
            trades_dropped=self.stats['trades_dropped'],
            check_failures=self.check_stats['failures'].copy(),
            check_passes=self.check_stats['passes'].copy(),
            state_counts=self.state_counts.copy(),
            state_transitions=self.state_transitions.copy(),
            lateness_stats=self.lateness_stats.copy(),
            avg_lateness_by_type=avg_lateness
        )
        
        return result
    

    def print_status(self):
        """현재 상태 출력"""
        print(f"\n{'='*60}")
        print(f"📊 StreamProcessor Status")
        print(f"{'='*60}")
        print(f"  Dataset: {self.dataset_name}")
        print(f"  Data Trust State: {self.data_trust_state.value}")
        print(f"  Initialized: {self.initialized}")
        print(f"\n  Lateness Config:")
        print(f"    Max Event Lateness: {self.max_event_lateness_ms}ms")
        print(f"\n  Drop Statistics:")
        print(f"    Orderbook dropped: {self.stats['orderbook_dropped']:,}")
        print(f"    Trades dropped: {self.stats['trades_dropped']:,}")
        print(f"    Drop rate: {self._get_recent_drop_rate():.2%}")
        print(f"\n  Statistics:")
        for key, value in self.stats.items():
            print(f"    {key}: {value:,}")