"""
Data Source - Historical/Realtime 통합 데이터 소스 (최적화 v3)

================================================================================
최적화:
- iterrows() 제거 → 벡터화 처리
- groupby로 timestamp별 배치 처리
- 메모리 효율적 청크 처리
================================================================================
"""
import asyncio
import json
from abc import ABC, abstractmethod
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional, Dict, AsyncIterator, List
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
    Historical 데이터 소스 (최적화 v3)
    
    핵심 최적화:
    1. Orderbook: groupby + 벡터화 (iterrows 제거)
    2. 청크 단위 처리
    3. heapq 병합 정렬
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
        """이벤트 스트리밍 (병합 정렬)"""
        self._is_running = True
        
        # 각 스트림의 이벤트 제너레이터
        generators = {}
        
        if 'orderbook' in self.files:
            generators['orderbook'] = self._stream_orderbook_fast()
        if 'trades' in self.files:
            generators['trades'] = self._stream_trades_fast()
        if 'ticker' in self.files:
            generators['ticker'] = self._stream_ticker_fast()
        if 'liquidations' in self.files:
            generators['liquidations'] = self._stream_liquidations_fast()
        
        # heapq 병합 정렬
        heap = []
        
        for name, gen in generators.items():
            try:
                event = next(gen)
                heapq.heappush(heap, (event.local_timestamp, name, event, gen))
            except StopIteration:
                pass
        
        while heap and self._is_running:
            ts, name, event, gen = heapq.heappop(heap)
            
            self.stats.events_total += 1
            self.stats.events_processed += 1
            
            yield event
            
            try:
                next_event = next(gen)
                heapq.heappush(heap, (next_event.local_timestamp, name, next_event, gen))
            except StopIteration:
                pass
    
    def _stream_orderbook_fast(self) -> Iterator[Event]:
        """
        Orderbook 빠른 스트리밍 (벡터화)
        
        전략: groupby(timestamp)로 배치 처리
        """
        path = self.files['orderbook']
        chunk_size = 1_000_000  # 100만 rows
        
        # 청크 간 pending 데이터
        pending_ts = None
        pending_local_ts = None
        pending_is_snapshot = False
        pending_bids = []
        pending_asks = []
        
        for chunk in pd.read_csv(path, chunksize=chunk_size):
            # 필요한 컬럼만 사용
            chunk = chunk[['timestamp', 'local_timestamp', 'side', 'price', 'amount', 'is_snapshot']].copy()
            
            # timestamp별 그룹화
            grouped = chunk.groupby('timestamp', sort=True)
            
            for ts, group in grouped:
                ts = int(ts)
                local_ts = int(group['local_timestamp'].iloc[0])
                is_snapshot = group['is_snapshot'].iloc[0]
                if isinstance(is_snapshot, str):
                    is_snapshot = is_snapshot.lower() == 'true'
                
                # 이전 pending flush
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
                
                # 벡터화된 bid/ask 분리
                bids_mask = group['side'] == 'bid'
                asks_mask = group['side'] == 'ask'
                
                bids_data = group.loc[bids_mask, ['price', 'amount']].values.tolist()
                asks_data = group.loc[asks_mask, ['price', 'amount']].values.tolist()
                
                pending_ts = ts
                pending_local_ts = local_ts
                pending_is_snapshot = is_snapshot
                pending_bids = bids_data
                pending_asks = asks_data
        
        # 마지막 pending flush
        if pending_ts is not None and (pending_bids or pending_asks):
            event_type = EventType.SNAPSHOT if pending_is_snapshot else EventType.ORDERBOOK
            yield Event(
                event_type=event_type,
                timestamp=pending_ts,
                local_timestamp=pending_local_ts,
                data={'bids': pending_bids, 'asks': pending_asks}
            )
    
    def _stream_trades_fast(self) -> Iterator[Event]:
        """Trades 빠른 스트리밍"""
        path = self.files['trades']
        chunk_size = 500_000
        
        for chunk in pd.read_csv(path, chunksize=chunk_size):
            # local_timestamp로 정렬
            chunk = chunk.sort_values('local_timestamp')
            
            # 벡터화된 데이터 추출
            timestamps = chunk['timestamp'].values
            local_timestamps = chunk['local_timestamp'].values
            prices = chunk['price'].values
            amounts = chunk['amount'].values
            sides = chunk['side'].values if 'side' in chunk.columns else ['unknown'] * len(chunk)
            
            for i in range(len(chunk)):
                yield Event(
                    event_type=EventType.TRADE,
                    timestamp=int(timestamps[i]),
                    local_timestamp=int(local_timestamps[i]),
                    data={
                        'price': float(prices[i]),
                        'quantity': float(amounts[i]),
                        'side': sides[i] if isinstance(sides, np.ndarray) else sides,
                    }
                )
    
    def _stream_ticker_fast(self) -> Iterator[Event]:
        """Ticker 빠른 스트리밍"""
        path = self.files['ticker']
        chunk_size = 100_000
        
        for chunk in pd.read_csv(path, chunksize=chunk_size):
            chunk = chunk.sort_values('local_timestamp')
            
            timestamps = chunk['timestamp'].values
            local_timestamps = chunk['local_timestamp'].values
            last_prices = chunk['last_price'].values
            funding_rates = chunk['funding_rate'].values if 'funding_rate' in chunk.columns else [None] * len(chunk)
            
            for i in range(len(chunk)):
                yield Event(
                    event_type=EventType.TICKER,
                    timestamp=int(timestamps[i]),
                    local_timestamp=int(local_timestamps[i]),
                    data={
                        'last_price': float(last_prices[i]),
                        'funding_rate': funding_rates[i] if not pd.isna(funding_rates[i]) else None,
                    }
                )
    
    def _stream_liquidations_fast(self) -> Iterator[Event]:
        """Liquidations 빠른 스트리밍"""
        path = self.files['liquidations']
        chunk_size = 50_000
        
        for chunk in pd.read_csv(path, chunksize=chunk_size):
            chunk = chunk.sort_values('local_timestamp')
            
            timestamps = chunk['timestamp'].values
            local_timestamps = chunk['local_timestamp'].values
            sides = chunk['side'].values if 'side' in chunk.columns else ['unknown'] * len(chunk)
            amounts = chunk['amount'].values
            prices = chunk['price'].values
            
            for i in range(len(chunk)):
                yield Event(
                    event_type=EventType.LIQUIDATION,
                    timestamp=int(timestamps[i]),
                    local_timestamp=int(local_timestamps[i]),
                    data={
                        'side': sides[i],
                        'quantity': float(amounts[i]),
                        'price': float(prices[i]),
                    }
                )
    
    async def get_events_async(self) -> AsyncIterator[Event]:
        """Historical은 동기지만 async 인터페이스도 제공"""
        for event in self.get_events():
            yield event


class RealtimeDataSource(DataSource):
    """
    Realtime 데이터 소스 (WebSocket)
    
    duration_sec=0 이면 무한 실행 (Ctrl+C로만 종료)
    """
    
    def __init__(self, symbol: str = "btcusdt", duration_sec: int = 0):
        super().__init__()
        self.symbol = symbol
        self.duration_sec = duration_sec  # 0 = 무한
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
        raise NotImplementedError("Realtime requires async")
    
    def _should_continue(self) -> bool:
        """계속 실행해야 하는지 체크"""
        if not self._is_running:
            return False
        
        # duration_sec = 0 이면 무한 실행
        if self.duration_sec == 0:
            return True
        
        # 시간 제한 체크
        elapsed = (datetime.now() - self.start_time).total_seconds()
        return elapsed < self.duration_sec
    
    async def get_events_async(self) -> AsyncIterator[Event]:
        """
        WebSocket에서 이벤트 읽기
        
        duration_sec=0: 무한 실행 (Ctrl+C로 종료)
        duration_sec>0: 해당 시간 후 자동 종료
        """
        import websockets
        
        self._is_running = True
        self.start_time = datetime.now()
        uri = self.get_stream_uri()
        
        while self._should_continue():
            try:
                async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                    while self._should_continue():
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            event = self._parse_message(message)
                            if event:
                                yield event
                        except asyncio.TimeoutError:
                            continue
            
            except asyncio.CancelledError:
                # Ctrl+C 등으로 취소됨
                self._is_running = False
                break
            
            except Exception as e:
                self.stats.reconnects += 1
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
        return HistoricalDataSource(data_dir=kwargs.get('data_dir', './data'))
    elif mode == 'realtime':
        return RealtimeDataSource(
            symbol=kwargs.get('symbol', 'btcusdt'),
            duration_sec=kwargs.get('duration_sec', 60)
        )
    else:
        raise ValueError(f"Unknown mode: {mode}")