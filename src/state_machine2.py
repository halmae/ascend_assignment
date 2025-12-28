from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, Any, List, Optional
from collections import deque


class DataTrustState(Enum):
    TRUSTED = "TRUSTED"
    DEGRADED = "DEGRADED"
    UNTRUSTED = "UNTRUSTED"

class HypothesisState(Enum):
    VALID = "VALID"
    WEAKENING = "WEAKENING"
    INVALID = "INVALID"

class DecisionPermission(Enum):
    ALLOWED = "ALLOWED"
    RESTRICTED = "RESTRICTED"
    HALTED = "HALTED"

@dataclass
class SystemState:
    """
    Docstring for SystemState
    """
    timestamp: datetime
    data_trust: DataTrustState
    hypothesis: HypothesisState
    decision: DecisionPermission
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp' : self.timestamp.isoformat(),
            'data_trust' : self.data_trust.value,
            'hypothesis' : self.hypothesis.value,
            'decision' : self.decision.value,
            'reason' : self.reason
        }
    
@dataclass
class OrderbookSnapshot:
    """
    Orderbook snapshot at a specific timestamp
    """
    timestamp: int   # microseconds
    bids: List[tuple] # [(price, amount), ...]
    asks: List[tuple] # [(price, amount), ...]

    def best_bid(self) -> Optional[float]:
        return self.bids[0][0] if self.bids else None
    
    def best_ask(self) -> Optional[float]:
        return self.asks[0][0] if self.asks else None
    
    def spread(self) -> Optional[float]:
        if self.best_bid() and self.best_ask():
            return self.best_ask() - self.best_bid()
        return None
    
    def mid_price(self) -> Optional[float]:
        if self.best_bid() and self.best_ask():
            return (self.best_bid() + self.best_ask()) / 2
        return None
    
    def depth(self) -> Dict[str, int]:
        return {
            'bid_levels': len(self.bids),
            'ask_levels': len(self.asks),
            'total_levels': len(self.bids) + len(self.asks)
        }
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp': self.timestamp,
            'bids': self.bids,
            'asks': self.asks,
            'best_bid': self.best_bid(),
            'best_ask': self.best_ask(),
            'spread': self.spread(),
            'mid_price': self.mid_price(),
            'depth': self.depth()
        }


class StateMachine:

    def __init__(
            self,
            window: int = 1_000_000,    # 1 sec window
            lateness: int = 4_000,      # 4 ms for latency
            buffer: int = 100,
    ):
        # === Configuration ===
        self.window = window
        self.lateness = lateness
        self.buffer = buffer

        # === Watermark ===
        self.current_watermark = None
        self.window_end = None

        # === Event Buffers ===
        self.event_buffer = deque(maxlen=buffer)

        # === Snapshot Store ===
        self.current_snapshot = None
        self.previous_snapshot = None

        # === System State ===
        self.data_trust = DataTrustState.TRUSTED
        self.hypothesis = HypothesisState.VALID
        self.decision = DecisionPermission.ALLOWED

        # === Statistics ===
        self.stats = {
            'total_events': 0,
            'accepted': 0,
            'repaired': 0,
            'quarantined': 0,
            'out_of_order': 0,
            'late_arrivals': 0
        }

        # === History & Logs ===
        self.state_history = []

    
    def receive_snapshot(self, snapshot_data) -> bool:
        """
        스냅샷 수신 - 여러 형식 지원

        Args:
            snapshot_data:
                - List[Dict] : CSV 형식
                - Dict: Websocket 형식 (single message)
        """
        # === 1. 입력 형식 감지 ===
        if isinstance(snapshot_data, dict):
            # WebSocket 형식
            return self._receive_snapshot_websocket(snapshot_data)
        elif isinstance(snapshot_data, list):
            # CSV 형식
            return self._receive_snapshot_csv(snapshot_data)
        else:
            print(f"Unknown snapshot format: {type(snapshot_data)}")
            return False


    def _receive_snapshot_websocket(self, snapshot: Dict) -> bool:
        """
        WebSocket 형식 스냅샷 처리

        Expected format:
        {
            'timestamp': int (microseconds),
            'bids': [['price', 'amount'], ...],
            'asks': [['price', 'amount'], ...]
        }
        """
        # === 필수 필드 검증 ===
        required_fields = ['timestamp', 'bids', 'asks']
        if not all(field in snapshot for field in required_fields):
            print(f"Missing required fields. Expected: {required_fields}")
            return False
        
        snapshot_ts = snapshot['timestamp']

        # === Watermark 역행 체크 ===
        if self.current_watermark and snapshot_ts < self.current_watermark:
            time_gap = self.current_watermark - snapshot_ts
            print(f"Out-of-order snapshot! Gap: {time_gap / 1_000:.2f}ms")
            self.stats['out_of_order'] += 1

        # === 이전 스냅샷 백업 ===
        self.previous_snapshot = self.current_snapshot

        # === 데이터 파싱 및 정렬 ===
        bids = sorted(
            [(float(price), float(amount)) for price, amount in snapshot['bids']],
            key=lambda x: x[0],
            reverse=True    # 높은 가격부터
        )

        asks = sorted(
            [(float(price), float(amount)) for price, amount in snapshot['asks']],
            key=lambda x: x[0]    # 낮은 가격부터
        )

        # === Crossed Market 검증 ===
        if bids and asks:
            best_bid = bids[0][0]
            best_ask = asks[0][0]

            if best_bid >= best_ask:
                print(f"CROSSED MARKET!! Bid: {best_bid}, Ask: {best_ask}")
                self.data_trust = DataTrustState.DEGRADED
                # 엄격한 정책이라면 거부
                # return False

        # === 스냅샷 저장 ===
        self.current_snapshot = {
            'timestamp': snapshot_ts,
            'bids': bids,
            'asks': asks
        }

        # === Watermark 업데이트 ===
        self.current_watermark = snapshot_ts
        self.window_end = snapshot_ts + self.window

        # === Statistics ===
        total_levels = len(bids) + len(asks)
        self.stats['total_events'] += total_levels
        self.stats['accepted'] += total_levels

        # === Log ===
        print(f"WebSocket Snapshot received")
        print(f"    Timestamp: {snapshot_ts}")
        print(f"    Levels: {len(bids)} bids, {len(asks)} asks")
        if bids and asks:
            spread = asks[0][0] - bids[0][0]
            print(f"    Spread: {spread:.2f}")

        return True

    def _receive_snapshot_csv(self, snapshot_events: List[Dict]) -> bool:
        """
        CSV 형식 스냅샷 처리

        Expected format:
        [
            {'timestamp': int, 'side': 'bid', 'price': float, 'amount': float}, ...
        ]
        """
        # === 입력 검증 ===
        if not snapshot_events:
            print("Empty snapshot")
            return False

        # === Timestamp 일관성 검증 ===
        timestamps = set(event['timestamp'] for event in snapshot_events)
        if len(timestamps) != 1:
            print(f"Inconsistent timestamps: {len(timestamps)} different values")
            return False
        
        snapshot_ts = timestamps.pop()

        # === WebSocket 형식으로 변환 ===
        websocket_format = {
            'timestamp': snapshot_ts,
            'bids': [[str(e['price']), str(e['amount'])] for e in snapshot_events if e['side'] == 'bid'],
            'asks': [[str(e['price']), str(e['amount'])] for e in snapshot_events if e['side'] == 'ask']
        }

        # === WebSocket Handler로 위임 ===
        return self._receive_snapshot_websocket(websocket_format)

    def process_event(self, event: Dict) -> str:
        """
        개별 이벤트 처리

        Args:
            event: {
                'stream' : 'orderbook' | 'trades' | 'liquidations' | 'ticker',
                'timestamp': int,
                'local_timestamp': int,
                'data': {...} # 실제 이벤트 데이터
            }

        Returns:
            'ACCEPT' | 'REPAIR' | 'QUARANTINE'
        """

        self.stats['total_events'] += 1

        # === Snapshot vs Incremental 구분 ===
        if event['stream'] == 'orderbook' and event['data'].get('is_snapshot', False):
            return self._process_shapshot_event(event)
        
        # === 데이터 품질 검사 ===
        sanitization_result = self._sanitize_event(event)

        if sanitization_result == 'QUARANTINE':
            return self._quarantine_event(event)
        
        if sanitization_result == 'REPAIR':
            event = self._repair_event(event)

        # === 스트림별 처리 ===
        if event['stream'] == 'orderbook':
            self._update_orderbook(event)
        elif event['stream'] == 'trades':
            self._process_trade(event)
        elif event['stream'] == 'liquidations':
            self._process_liquidation(event)
        elif event['stream'] == 'ticker':
            self._process_ticker(event)

        # === Update Statistics ===
        if sanitization_result == 'ACCEPT':
            self.stats['accepted'] += 1
        elif sanitization_result == 'REPAIR':
            self.stats['repaired'] += 1

        # === State transition check ===
        self._check_state_transition()

        return sanitization_result


    def _sanitize_event(self, event: Dict) -> str:
        """
        이벤트 데이터 품질 검사

        Returns:
            'ACCEPT' | 'REPAIR' | 'QUARANTINE'
        """
        data = event['data']
        timestamp = event['timestamp']

        # === Check Out-of-order timestamp ===
        if self.current_watermark and timestamp < self.current_watermark - self.lateness:
            self.stats['late_arrivals'] += 1
            return 'QUARANTINE'
        
        # === Check Future timestamp ===
        if self.window_end and timestamp > self.window_end + self.window:
            return 'QUARANTINE'
        
        # === Check Orderbook specific checks ===
        if event['stream'] == 'orderbook':
            return self._sanitize_orderbook_event(event)
        
        # === Check Trades specific checks ===
        if event['stream'] == 'trades':
            return self._sanitize_trade_event(event)
        
        # === Liquidation specific checks ===
        if event['stream'] == 'liquidations':
            return self._sanitize_liquidation_event(event)
        
        return 'ACCEPT'


    def _sanitize_orderbook_event(self, event: Dict) -> str:
        """
        Orderbook 이벤트 검증
        """
        data = event['data']

        # 필수 필드 확인
        required_fields = ['price', 'amount', 'side']
        if not all(field in data for field in required_fields):
            return 'QUARANTINE'
        
        price = data['price']
        amount = data['amount']
        side = data['side']

        # === Invalid price / amount ===
        if price <= 0 or amount < 0:
            return 'QUARANTINE'
        
        # === Fat-finger price check ===
        if self.current_snapshot:
            mid_price = self.get_mid_price()

            if mid_price:
                price_deviation = abs(price - mid_price) / mid_price

                # TODO How to define this threshold?
                if price_deviation > 0.10:
                    print(f"Fat-finger detected: price={price}, mid={mid_price}, deviation={price_deviation*100:.2f}%")
                    return 'QUARANTINE'
                
                # TODO 경고하지만 수정 가능? <- 판단을 허용할만큼 안정한 정도?
                if price_deviation > 0.05:
                    return 'REPAIR'
                
        # === Crossed market check ===
        # TODO 나중에 _update_orderbook에서 체크
        
        return 'ACCEPT'


    def _sanitize_trade_event(self, event: Dict) -> str:
        """
        Trade 이벤트 검증
        """
        data = event['data']

        if 'price' not in data or 'amount' not in data:
            return 'QUARANTINE'
        
        if data['price'] <= 0 or data['amount'] <= 0:
            return 'QUARANTINE'
        
        # Fat-finger check
        if self.current_snapshot:
            mid_price = self.get_mid_price()
            if mid_price:
                deviation = abs(data['price'] - mid_price) / mid_price
                if deviation > 0.10:
                    return 'QUARANTINE'
                
        return 'ACCEPT'


    def _sanitize_liquidation_event(self, event: Dict) -> str:
        """
        Liquidation 이벤트 검증

        청산 이벤트는 시장 위기 신호 -> 특별 처리
        """
        data = event['data']

        if 'price' not in data or 'amount' not in data:
            return 'QUARANTINE'
        
        print(f"Liquidation detected: {data}")
        self.hypothesis = HypothesisState.WEAKENING

        return 'ACCEPT'


    def _repair_event(self, event: Dict) -> Dict:
        """
        수정 가능한 데이터 복구
        """
        # TODO
        return 'QUARANTINE'


    def _update_orderbook(self, event: Dict):
        """
        Orderbook 증분 업데이트
        """

        if not self.current_snapshot:
            print("No snapshot available for update")
            return
        
        data = event['data']
        price = data['price']
        amount = data['amount']
        side = data['side']

        # === 업데이트 적용 ===
        if side == 'bid':
            levels = self.current_snapshot['bids']
        else:
            levels = self.current_snapshot['asks']

        # 기존 price level 찾기
        updated = False
        for i, (p, a) in enumerate(levels):
            if abs(p - price) < 0.01:
                if amount == 0:
                    levels.pop(i)
                else:
                    levels[i] = (price, amount)
                updated = True
                break

        if not updated and amount > 0:
            levels.append((price, amount))

            levels.sort(key=lambda x: x[0], reverse=(side == 'bid'))

        if self.current_snapshot['bids'] and self.current_snapshot['asks']:
            best_bid = self.current_snapshot['bids'][0][0]
            best_ask = self.current_snapshot['asks'][0][0]

            if best_bid >= best_ask:
                print(f"Crossed market after update! Bid: {best_bid}, Ask: {best_ask}")
                self.data_trust = DataTrustState.DEGRADED

    
    def _process_trade(self, event: Dict):
        """
        Trade 이벤트 처리 (현재는 로깅만)
        """
        # 거래 발생 확인용
        pass


    def _process_liquidation(self, event: Dict):
        """
        Liquidation 이벤트 처리
        """
        # 이미 _sanitize_liquidation_event에서 처리
        pass


    def _process_ticker(self, event: Dict):
        """
        Ticker 이벤트 처리
        """
        # 시장 가격 정보 업데이트
        pass


    def update_watermark(self, timestamp: int):
        """
        Watermark 업데이트
        """
        pass


    def check_data_quality(self) -> DataTrustState:
        """
        현재 데이터 품질 검사
        """
        pass


    def transition_state(self, reason: str = ""):
        """
        상태 전이 및 로깅
        """
        pass