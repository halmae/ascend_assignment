"""
Data Source - Historical/Realtime 통합 데이터 소스 (최적화 버전)

================================================================================
v2 최적화:
- Orderbook을 row 단위가 아닌 timestamp 단위로 그룹화하여 처리
- iloc 슬라이싱 대신 인덱스 기반 접근
- 메모리 효율성 개선
================================================================================
"""
import asyncio
import json
from abc import ABC, abstractmethod
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional, Dict, AsyncIterator, List, Tuple
from dataclasses import dataclass
import heapq

import pandas as pd
import numpy as np

from src.enums import EventType
from src.data_types import Event


@dataclass
class SourceStats:
    """데이터 소스 통계"""
    events_total: int = 0
    events_processed: int = 0
    duplicates_filtered: int = 0
    out_of_order: int = 0
    reconnects: int = 0


class DataSource(ABC):
    """데이터 소스 추상 클래스"""
    
    def __init__(self):
        self.stats = SourceStats()
        self._is_running = False
    
    @abstractmethod
    def get_events(self) -> Iterator[Event]:
        pass
    
    @abstractmethod
    async def get_events_async(self) -> AsyncIterator[Event]:
        pass
    
    @abstractmethod
    def is_async(self) -> bool:
        pass
    
    def stop(self):
        self._is_running = False


class HistoricalDataSource(DataSource):
    """
    Historical 데이터 소스 (최적화 버전)
    
    최적화:
    1. Orderbook을 timestamp별로 미리 그룹화
    2. 청크 단위로 처리하여 메모리 효율성 확보
    3. heapq를 사용한 효율적인 병합 정렬
    """
    
    def __init__(self, data_dir: str):
        super().__init__()
        self.data_dir = Path(data_dir)
        self.files = self._find_files()
    
    def is_async(self) -> bool:
        return False
    
    def _find_files(self) -> Dict[str, Path]:
        """데이터 파일 찾기"""
        files = {}
        for name in ['orderbook', 'trades', 'ticker', 'liquidations']:
            for ext in ['.csv', '.csv.gz']:
                path = self.data_dir / f"{name}{ext}"
                if path.exists():
                    files[name] = path
                    break
        return files
    
    def get_events(self) -> Iterator[Event]:
        """
        CSV에서 이벤트 읽기 (최적화된 버전)
        
        전략: 각 파일을 독립적으로 처리하고, timestamp 기준으로 병합
        """
        self._is_running = True
        
        print(f"📂 데이터 로딩 시작...")
        print(f"   Files: {list(self.files.keys())}")
        
        # 각 스트림의 이벤트 제너레이터
        generators = {}
        
        if 'orderbook' in self.files:
            generators['orderbook'] = self._stream_orderbook_events()
        if 'trades' in self.files:
            generators['trades'] = self._stream_csv_events('trades', EventType.TRADE)
        if 'ticker' in self.files:
            generators['ticker'] = self._stream_csv_events('ticker', EventType.TICKER)
        if 'liquidations' in self.files:
            generators['liquidations'] = self._stream_csv_events('liquidations', EventType.LIQUIDATION)
        
        # heapq를 사용한 병합 정렬
        # (timestamp, stream_name, event)
        heap = []
        
        # 각 스트림에서 첫 이벤트 가져오기
        for name, gen in generators.items():
            try:
                event = next(gen)
                heapq.heappush(heap, (event.local_timestamp, name, event, gen))
            except StopIteration:
                pass
        
        print(f"   ✅ 스트림 초기화 완료. 병합 시작...")
        
        # 병합 정렬로 이벤트 순서대로 반환
        while heap and self._is_running:
            ts, name, event, gen = heapq.heappop(heap)
            
            self.stats.events_total += 1
            self.stats.events_processed += 1
            
            yield event
            
            # 해당 스트림에서 다음 이벤트 가져오기
            try:
                next_event = next(gen)
                heapq.heappush(heap, (next_event.local_timestamp, name, next_event, gen))
            except StopIteration:
                pass
    
    def _stream_orderbook_events(self) -> Iterator[Event]:
        """
        Orderbook CSV 스트리밍 (timestamp별 그룹화)
        
        CSV 형식: 각 row가 하나의 price level
        → 같은 timestamp의 row들을 모아서 하나의 이벤트로
        """
        path = self.files['orderbook']
        chunk_size = 500_000  # 50만 rows씩
        
        pending_ts = None
        pending_local_ts = None
        pending_is_snapshot = False
        pending_bids = []
        pending_asks = []
        
        for chunk in pd.read_csv(path, chunksize=chunk_size):
            # timestamp로 정렬
            chunk = chunk.sort_values('local_timestamp')
            
            for _, row in chunk.iterrows():
                ts = int(row.get('timestamp', 0))
                local_ts = int(row.get('local_timestamp', ts))
                is_snapshot = row.get('is_snapshot', False)
                if isinstance(is_snapshot, str):
                    is_snapshot = is_snapshot.lower() == 'true'
                
                side = str(row.get('side', '')).lower()
                price = float(row.get('price', 0))
                amount = float(row.get('amount', 0))
                
                # 새로운 timestamp면 이전 것을 yield
                if pending_ts is not None and pending_ts != ts:
                    if pending_bids or pending_asks:
                        event_type = EventType.SNAPSHOT if pending_is_snapshot else EventType.ORDERBOOK
                        yield Event(
                            event_type=event_type,
                            timestamp=pending_ts,
                            local_timestamp=pending_local_ts,
                            data={'bids': pending_bids, 'asks': pending_asks}
                        )
                    
                    pending_bids = []
                    pending_asks = []
                
                # 현재 row 추가
                pending_ts = ts
                pending_local_ts = local_ts
                pending_is_snapshot = is_snapshot
                
                if side == 'bid':
                    pending_bids.append([price, amount])
                elif side == 'ask':
                    pending_asks.append([price, amount])
        
        # 마지막 pending flush
        if pending_ts is not None and (pending_bids or pending_asks):
            event_type = EventType.SNAPSHOT if pending_is_snapshot else EventType.ORDERBOOK
            yield Event(
                event_type=event_type,
                timestamp=pending_ts,
                local_timestamp=pending_local_ts,
                data={'bids': pending_bids, 'asks': pending_asks}
            )
    
    def _stream_csv_events(self, name: str, event_type: EventType) -> Iterator[Event]:
        """일반 CSV 스트리밍 (trades, ticker, liquidations)"""
        path = self.files[name]
        
        chunk_sizes = {
            'trades': 100_000,
            'ticker': 50_000,
            'liquidations': 10_000,
        }
        chunk_size = chunk_sizes.get(name, 50_000)
        
        for chunk in pd.read_csv(path, chunksize=chunk_size):
            chunk = chunk.sort_values('local_timestamp')
            
            for _, row in chunk.iterrows():
                ts = int(row.get('timestamp', 0))
                local_ts = int(row.get('local_timestamp', ts))
                
                data = self._extract_data(name, row)
                
                yield Event(
                    event_type=event_type,
                    timestamp=ts,
                    local_timestamp=local_ts,
                    data=data
                )
    
    def _extract_data(self, name: str, row: pd.Series) -> Dict:
        """이벤트 데이터 추출"""
        if name == 'trades':
            return {
                'price': float(row.get('price', 0)),
                'quantity': float(row.get('amount', 0)),
                'side': row.get('side', 'unknown'),
            }
        elif name == 'ticker':
            return {
                'last_price': float(row.get('last_price', 0)),
                'funding_rate': row.get('funding_rate'),
            }
        elif name == 'liquidations':
            return {
                'side': row.get('side', 'unknown'),
                'quantity': float(row.get('amount', 0)),
                'price': float(row.get('price', 0)),
            }
        return {}
    
    async def get_events_async(self) -> AsyncIterator[Event]:
        """Historical은 동기지만 async 인터페이스도 제공"""
        for event in self.get_events():
            yield event


class RealtimeDataSource(DataSource):
    """
    Realtime 데이터 소스 (WebSocket)
    """
    
    def __init__(self, symbol: str = "btcusdt", duration_sec: int = 60):
        super().__init__()
        self.symbol = symbol
        self.duration_sec = duration_sec
        self.websocket_url = "wss://fstream.binance.com"
        
        # Robustness
        self.seen_trade_ids: OrderedDict = OrderedDict()
        self.seen_depth_ids: OrderedDict = OrderedDict()
        self.max_seen_ids = 10000
        self.last_timestamps: Dict[str, int] = {}
        
        self.start_time: Optional[datetime] = None
    
    def is_async(self) -> bool:
        return True
    
    def get_stream_uri(self) -> str:
        streams = [
            f"{self.symbol}@trade",
            f"{self.symbol}@depth@100ms",
            f"{self.symbol}@forceOrder",
            f"{self.symbol}@ticker",
        ]
        return f"{self.websocket_url}/stream?streams={'/'.join(streams)}"
    
    def get_events(self) -> Iterator[Event]:
        raise NotImplementedError("Realtime source requires async")
    
    async def get_events_async(self) -> AsyncIterator[Event]:
        """WebSocket에서 이벤트 읽기"""
        import websockets
        
        self._is_running = True
        self.start_time = datetime.now()
        uri = self.get_stream_uri()
        
        while self._is_running:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            if elapsed >= self.duration_sec:
                break
            
            try:
                async with websockets.connect(
                    uri,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    if self.stats.reconnects > 0:
                        print(f"\n  🔄 재연결 성공 (#{self.stats.reconnects})")
                    else:
                        print("✅ WebSocket 연결 성공!")
                        print("📡 데이터 수신 대기 중...\n")
                    
                    while self._is_running:
                        elapsed = (datetime.now() - self.start_time).total_seconds()
                        if elapsed >= self.duration_sec:
                            break
                        
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            event = self._parse_message(message)
                            if event:
                                yield event
                        except asyncio.TimeoutError:
                            continue
            
            except Exception as e:
                self.stats.reconnects += 1
                print(f"\n  ⚠️ 연결 끊김: {e}. 3초 후 재연결...")
                await asyncio.sleep(3)
                continue
    
    def _parse_message(self, message: str) -> Optional[Event]:
        """WebSocket 메시지 파싱"""
        self.stats.events_total += 1
        
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return None
        
        payload = data.get('data', {})
        event_type_str = payload.get('e', 'unknown')
        
        local_ts = int(datetime.now().timestamp() * 1000000)
        
        type_map = {
            'trade': EventType.TRADE,
            'depthUpdate': EventType.ORDERBOOK,
            'forceOrder': EventType.LIQUIDATION,
            '24hrTicker': EventType.TICKER,
        }
        
        event_type = type_map.get(event_type_str)
        if event_type is None:
            return None
        
        # 중복 체크
        if event_type == EventType.TRADE:
            trade_id = payload.get('t')
            if trade_id and self._is_duplicate('trade', trade_id):
                self.stats.duplicates_filtered += 1
                return None
        elif event_type == EventType.ORDERBOOK:
            depth_id = f"{payload.get('U', 0)}-{payload.get('u', 0)}"
            if self._is_duplicate('depth', depth_id):
                self.stats.duplicates_filtered += 1
                return None
        
        event_ts = payload.get('E', 0) * 1000
        self._check_out_of_order(event_type_str, event_ts)
        
        event_data = self._extract_data(event_type_str, payload)
        
        self.stats.events_processed += 1
        
        return Event(
            event_type=event_type,
            timestamp=event_ts,
            local_timestamp=local_ts,
            data=event_data
        )
    
    def _extract_data(self, event_type: str, payload: Dict) -> Dict:
        if event_type == 'trade':
            return {
                'price': float(payload.get('p', 0)),
                'quantity': float(payload.get('q', 0)),
                'side': 'sell' if payload.get('m', False) else 'buy',
            }
        elif event_type == 'depthUpdate':
            return {
                'bids': [[float(b[0]), float(b[1])] for b in payload.get('b', [])],
                'asks': [[float(a[0]), float(a[1])] for a in payload.get('a', [])],
            }
        elif event_type == 'forceOrder':
            order = payload.get('o', {})
            return {
                'side': order.get('S', 'unknown'),
                'quantity': float(order.get('q', 0)),
                'price': float(order.get('p', 0)),
            }
        elif event_type == '24hrTicker':
            return {
                'last_price': float(payload.get('c', 0)),
                'funding_rate': float(payload.get('r', 0)) if 'r' in payload else None,
            }
        return {}
    
    def _is_duplicate(self, stream_type: str, msg_id) -> bool:
        seen = self.seen_trade_ids if stream_type == 'trade' else self.seen_depth_ids
        
        if msg_id in seen:
            return True
        
        seen[msg_id] = True
        while len(seen) > self.max_seen_ids:
            seen.popitem(last=False)
        
        return False
    
    def _check_out_of_order(self, stream_type: str, timestamp: int):
        last_ts = self.last_timestamps.get(stream_type, 0)
        if timestamp < last_ts:
            self.stats.out_of_order += 1
        self.last_timestamps[stream_type] = timestamp


def create_data_source(mode: str, **kwargs) -> DataSource:
    """데이터 소스 팩토리"""
    if mode == 'historical':
        return HistoricalDataSource(
            data_dir=kwargs.get('data_dir', './data')
        )
    elif mode == 'realtime':
        return RealtimeDataSource(
            symbol=kwargs.get('symbol', 'btcusdt'),
            duration_sec=kwargs.get('duration_sec', 60)
        )
    else:
        raise ValueError(f"Unknown mode: {mode}")