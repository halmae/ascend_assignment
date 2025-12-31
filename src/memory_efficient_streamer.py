"""
메모리 효율적인 스트림 처리기

대용량 데이터셋(1억+ rows)을 처리하기 위한 청크 기반 스트리밍
전체 데이터를 메모리에 로드하지 않고 청크 단위로 처리
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict, List, Iterator, Generator
from pathlib import Path
from dataclasses import dataclass
import heapq
import gc

from src.data_types import Event
from src.enums import EventType


@dataclass
class ChunkConfig:
    """청크 처리 설정"""
    orderbook_chunk_size: int = 5_000_000   # 500만 rows씩
    trades_chunk_size: int = 1_000_000      # 100만 rows씩
    ticker_chunk_size: int = 50_000         # 5만 rows씩
    liquidation_chunk_size: int = 10_000    # 1만 rows씩


class ChunkedStreamReader:
    """
    청크 단위로 CSV를 읽는 Reader
    
    전체 파일을 메모리에 올리지 않고, 필요한 만큼만 읽어서 처리
    """
    
    def __init__(self, 
                 filepath: Path, 
                 chunk_size: int,
                 sort_column: str = 'local_timestamp'):
        self.filepath = filepath
        self.chunk_size = chunk_size
        self.sort_column = sort_column
        
        # 청크 iterator
        self._chunk_iter: Optional[Iterator] = None
        self._current_chunk: Optional[pd.DataFrame] = None
        self._chunk_idx: int = 0
        self._row_idx: int = 0
        self._exhausted: bool = False
        
        # 현재 청크의 numpy 배열들 (메모리 효율)
        self._timestamps: Optional[np.ndarray] = None
        self._data_arrays: Dict[str, np.ndarray] = {}
        
    def initialize(self):
        """Reader 초기화 - 첫 청크 로드"""
        if not self.filepath.exists():
            raise FileNotFoundError(f"파일을 찾을 수 없습니다: {self.filepath}")
        
        self._chunk_iter = pd.read_csv(
            self.filepath,
            chunksize=self.chunk_size,
            dtype={
                'timestamp': 'int64',
                'local_timestamp': 'int64',
                'price': 'float64',
                'amount': 'float64',
            }
        )
        self._load_next_chunk()
        
    def _load_next_chunk(self) -> bool:
        """다음 청크 로드"""
        if self._chunk_iter is None:
            return False
            
        try:
            # 이전 청크 메모리 해제
            self._clear_current_chunk()
            
            chunk = next(self._chunk_iter)
            chunk = chunk.sort_values(self.sort_column).reset_index(drop=True)
            
            self._current_chunk = chunk
            self._chunk_idx += 1
            self._row_idx = 0
            
            # numpy 배열로 변환 (빠른 접근)
            self._timestamps = chunk[self.sort_column].values
            
            return True
            
        except StopIteration:
            self._exhausted = True
            self._clear_current_chunk()
            return False
    
    def _clear_current_chunk(self):
        """현재 청크 메모리 해제"""
        self._current_chunk = None
        self._timestamps = None
        self._data_arrays.clear()
        gc.collect()
    
    def peek_timestamp(self) -> Optional[int]:
        """현재 위치의 timestamp 반환 (이동 없음)"""
        if self._exhausted:
            return None
            
        if self._current_chunk is None or self._row_idx >= len(self._current_chunk):
            if not self._load_next_chunk():
                return None
        
        return int(self._timestamps[self._row_idx])
    
    def get_next_row(self) -> Optional[pd.Series]:
        """다음 행 반환"""
        if self._exhausted:
            return None
            
        if self._current_chunk is None or self._row_idx >= len(self._current_chunk):
            if not self._load_next_chunk():
                return None
        
        row = self._current_chunk.iloc[self._row_idx]
        self._row_idx += 1
        return row
    
    def has_more(self) -> bool:
        """더 읽을 데이터가 있는지"""
        if self._exhausted:
            return False
        if self._current_chunk is not None and self._row_idx < len(self._current_chunk):
            return True
        # 다음 청크 확인
        return self.peek_timestamp() is not None
    
    def close(self):
        """리소스 정리"""
        self._clear_current_chunk()
        self._chunk_iter = None
        self._exhausted = True


class MemoryEfficientStreamer:
    """
    메모리 효율적인 멀티 스트림 병합기
    
    Heap을 사용해 여러 스트림을 timestamp 순으로 병합
    각 스트림은 청크 단위로 읽어서 메모리 사용 최소화
    """
    
    def __init__(self, 
                 data_dir: str,
                 config: Optional[ChunkConfig] = None):
        self.data_dir = Path(data_dir)
        self.config = config or ChunkConfig()
        
        # Stream readers
        self.readers: Dict[str, ChunkedStreamReader] = {}
        
        # Snapshot 처리 (별도 로드 - 보통 작음)
        self.snapshots: List[Dict] = []
        self.snap_idx: int = 0
        
        # 통계
        self.stats = {
            'orderbook_events': 0,
            'trade_events': 0,
            'ticker_events': 0,
            'liquidation_events': 0,
            'snapshot_events': 0,
        }
        
        self._initialized = False
    
    def initialize(self):
        """스트리머 초기화"""
        print("⚡ 메모리 효율적 Streamer 초기화 중...")
        
        # Orderbook reader
        ob_path = self._find_file('orderbook')
        if ob_path:
            self.readers['orderbook'] = ChunkedStreamReader(
                ob_path, 
                self.config.orderbook_chunk_size
            )
            self.readers['orderbook'].initialize()
            print(f"  ✅ Orderbook reader 준비 (chunk: {self.config.orderbook_chunk_size:,})")
        
        # Trades reader
        tr_path = self._find_file('trades')
        if tr_path:
            self.readers['trades'] = ChunkedStreamReader(
                tr_path,
                self.config.trades_chunk_size
            )
            self.readers['trades'].initialize()
            print(f"  ✅ Trades reader 준비 (chunk: {self.config.trades_chunk_size:,})")
        
        # Ticker reader
        tk_path = self._find_file('ticker')
        if tk_path:
            self.readers['ticker'] = ChunkedStreamReader(
                tk_path,
                self.config.ticker_chunk_size
            )
            self.readers['ticker'].initialize()
            print(f"  ✅ Ticker reader 준비 (chunk: {self.config.ticker_chunk_size:,})")
        
        # Liquidation reader (optional)
        liq_path = self._find_file('liquidations')
        if liq_path:
            self.readers['liquidations'] = ChunkedStreamReader(
                liq_path,
                self.config.liquidation_chunk_size
            )
            self.readers['liquidations'].initialize()
            print(f"  ✅ Liquidations reader 준비")
        
        # Snapshot은 별도 로드 (보통 작음)
        self._load_snapshots()
        
        self._initialized = True
        print("✅ 메모리 효율적 Streamer 초기화 완료")
    
    def _find_file(self, stream_name: str) -> Optional[Path]:
        """파일 경로 찾기 (.csv.gz 또는 .csv)"""
        gz_path = self.data_dir / f"{stream_name}.csv.gz"
        if gz_path.exists():
            return gz_path
        
        csv_path = self.data_dir / f"{stream_name}.csv"
        if csv_path.exists():
            return csv_path
        
        return None
    
    def _load_snapshots(self):
        """Snapshot 로드 (orderbook에서 분리)"""
        ob_path = self._find_file('orderbook')
        if not ob_path:
            return
        
        # Snapshot만 필터링해서 로드 (첫 청크만 확인하거나 별도 파일)
        # 실제로는 is_snapshot=True인 행만 읽어야 함
        # 여기서는 간단히 첫 몇 개 청크에서 snapshot 추출
        
        print("  📸 Snapshot 로드 중...")
        snapshot_chunks = pd.read_csv(ob_path, chunksize=1_000_000)
        
        for chunk in snapshot_chunks:
            if 'is_snapshot' not in chunk.columns:
                break
                
            snapshots = chunk[chunk['is_snapshot'] == True]
            if snapshots.empty:
                continue
            
            for ts in snapshots['timestamp'].unique():
                group = snapshots[snapshots['timestamp'] == ts]
                bids = []
                asks = []
                
                for _, row in group.iterrows():
                    price = float(row['price'])
                    amount = float(row['amount'])
                    if row['side'] == 'bid':
                        bids.append([price, amount])
                    else:
                        asks.append([price, amount])
                
                self.snapshots.append({
                    'timestamp': int(ts),
                    'local_timestamp': int(group['local_timestamp'].iloc[0]),
                    'bids': bids,
                    'asks': asks
                })
            
            # Snapshot은 보통 초반에만 있으므로 일정량 찾으면 중단
            if len(self.snapshots) >= 10:
                break
        
        self.snapshots.sort(key=lambda x: x['local_timestamp'])
        print(f"  ✅ Snapshot {len(self.snapshots)}개 로드")
        
        # 청크 iterator 정리
        del snapshot_chunks
        gc.collect()
    
    def get_next_event(self) -> Optional[Event]:
        """다음 이벤트 반환 (timestamp 순)"""
        if not self._initialized:
            raise RuntimeError("initialize()를 먼저 호출하세요")
        
        # 최대 시도 횟수 (snapshot skip으로 인한 반복 방지)
        max_attempts = 1000
        
        for _ in range(max_attempts):
            # 각 스트림의 다음 timestamp 수집
            candidates = []
            
            # Snapshot 확인
            if self.snap_idx < len(self.snapshots):
                snap_ts = self.snapshots[self.snap_idx]['local_timestamp']
                candidates.append((snap_ts, 'snapshot', self.snap_idx))
            
            # 각 reader 확인
            for name, reader in self.readers.items():
                ts = reader.peek_timestamp()
                if ts is not None:
                    candidates.append((ts, name, None))
            
            if not candidates:
                return None
            
            # 가장 빠른 timestamp 선택
            candidates.sort(key=lambda x: x[0])
            ts, stream_type, idx = candidates[0]
            
            # 해당 스트림에서 이벤트 생성
            if stream_type == 'snapshot':
                event = self._create_snapshot_event(self.snapshots[self.snap_idx])
                self.snap_idx += 1
                self.stats['snapshot_events'] += 1
                return event
            
            row = self.readers[stream_type].get_next_row()
            if row is None:
                continue  # 다음 반복에서 다른 스트림 시도
            
            event = self._create_event(stream_type, row)
            if event is not None:
                return event
            # event가 None이면 (snapshot skip) 다음 반복
        
        return None
    
    def _create_event(self, stream_type: str, row: pd.Series) -> Optional[Event]:
        """스트림 타입에 따른 Event 생성"""
        
        if stream_type == 'orderbook':
            # Snapshot이 아닌 일반 업데이트만 처리
            is_snapshot = row.get('is_snapshot', False)
            if pd.isna(is_snapshot):
                is_snapshot = False
            
            if is_snapshot == True or is_snapshot == 'True' or is_snapshot == 'true':
                # Snapshot은 별도 로드되므로 여기서는 None 반환 (skip)
                return None
            
            self.stats['orderbook_events'] += 1
            return Event(
                event_type=EventType.ORDERBOOK,
                timestamp=int(row['timestamp']),
                local_timestamp=int(row['local_timestamp']),
                data={
                    'is_snapshot': False,
                    'price': float(row['price']),
                    'amount': float(row['amount']),
                    'side': row['side']
                }
            )
        
        elif stream_type == 'trades':
            self.stats['trade_events'] += 1
            return Event(
                event_type=EventType.TRADE,
                timestamp=int(row['timestamp']),
                local_timestamp=int(row['local_timestamp']),
                data={
                    'price': float(row['price']),
                    'amount': float(row['amount']),
                    'side': row['side']
                }
            )
        
        elif stream_type == 'ticker':
            self.stats['ticker_events'] += 1
            return Event(
                event_type=EventType.TICKER,
                timestamp=int(row['timestamp']),
                local_timestamp=int(row['local_timestamp']),
                data=row.to_dict()
            )
        
        elif stream_type == 'liquidations':
            self.stats['liquidation_events'] += 1
            return Event(
                event_type=EventType.LIQUIDATION,
                timestamp=int(row['timestamp']),
                local_timestamp=int(row['local_timestamp']),
                data=row.to_dict()
            )
        
        return None
    
    def _create_snapshot_event(self, snapshot: Dict) -> Event:
        """Snapshot Event 생성"""
        return Event(
            event_type=EventType.ORDERBOOK,
            timestamp=snapshot['timestamp'],
            local_timestamp=snapshot['local_timestamp'],
            data={
                'is_snapshot': True,
                'bids': snapshot['bids'],
                'asks': snapshot['asks']
            }
        )
    
    def has_more_events(self) -> bool:
        """더 처리할 이벤트가 있는지"""
        if self.snap_idx < len(self.snapshots):
            return True
        
        for reader in self.readers.values():
            if reader.has_more():
                return True
        
        return False
    
    def get_progress(self) -> Dict[str, int]:
        """진행 상황 반환"""
        return self.stats.copy()
    
    def close(self):
        """모든 리소스 정리"""
        for reader in self.readers.values():
            reader.close()
        self.readers.clear()
        self.snapshots.clear()
        gc.collect()


def create_streamer(data_dir: str, 
                    memory_efficient: bool = True,
                    chunk_config: Optional[ChunkConfig] = None) -> MemoryEfficientStreamer:
    """
    스트리머 팩토리 함수
    
    Args:
        data_dir: 데이터 디렉토리
        memory_efficient: 메모리 효율 모드 사용 여부
        chunk_config: 청크 설정
    
    Returns:
        초기화된 스트리머
    """
    streamer = MemoryEfficientStreamer(data_dir, chunk_config)
    streamer.initialize()
    return streamer