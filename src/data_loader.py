"""
Data Loader - CSV 파일에서 이벤트 읽기
"""
import os
import pandas as pd
from pathlib import Path
from typing import Iterator, Optional, Dict, List

from src.enums import EventType
from src.data_types import Event


class DataLoader:
    """
    Historical 데이터 로더
    
    CSV 파일들을 읽어서 timestamp 순으로 Event 생성
    """
    
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.files = self._find_files()
    
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
    
    def iterate_events(self) -> Iterator[Event]:
        """
        모든 이벤트를 timestamp 순으로 반환
        
        메모리 효율을 위해 청크 단위로 처리
        """
        # 각 파일의 청크 이터레이터 생성
        iterators = {}
        current_rows = {}
        
        chunk_sizes = {
            'orderbook': 100_000,
            'trades': 50_000,
            'ticker': 10_000,
            'liquidations': 1_000,
        }
        
        for name, path in self.files.items():
            iterators[name] = pd.read_csv(
                path, 
                chunksize=chunk_sizes.get(name, 10_000)
            )
            current_rows[name] = self._get_next_rows(iterators[name])
        
        # 병합 정렬
        while True:
            # 가장 빠른 timestamp 찾기
            min_ts = None
            min_name = None
            
            for name, rows in current_rows.items():
                if rows is not None and len(rows) > 0:
                    ts = rows.iloc[0].get('local_timestamp', 0)
                    if min_ts is None or ts < min_ts:
                        min_ts = ts
                        min_name = name
            
            if min_name is None:
                break
            
            # 이벤트 생성
            row = current_rows[min_name].iloc[0]
            current_rows[min_name] = current_rows[min_name].iloc[1:]
            
            # 청크 비었으면 다음 청크 로드
            if len(current_rows[min_name]) == 0:
                current_rows[min_name] = self._get_next_rows(iterators[min_name])
            
            event = self._create_event(min_name, row)
            if event:
                yield event
    
    def _get_next_rows(self, iterator) -> Optional[pd.DataFrame]:
        """다음 청크 로드"""
        try:
            chunk = next(iterator)
            if 'local_timestamp' in chunk.columns:
                return chunk.sort_values('local_timestamp').reset_index(drop=True)
            return chunk.reset_index(drop=True)
        except StopIteration:
            return None
    
    def _create_event(self, name: str, row: pd.Series) -> Optional[Event]:
        """이벤트 생성"""
        event_type_map = {
            'orderbook': EventType.ORDERBOOK,
            'trades': EventType.TRADE,
            'ticker': EventType.TICKER,
            'liquidations': EventType.LIQUIDATION,
        }
        
        event_type = event_type_map.get(name)
        if event_type is None:
            return None
        
        timestamp = int(row.get('timestamp', 0))
        local_timestamp = int(row.get('local_timestamp', timestamp))
        
        # 데이터 추출
        data = {}
        
        if name == 'orderbook':
            # Snapshot 체크
            is_snapshot = row.get('is_snapshot', False)
            if is_snapshot == True or is_snapshot == 'True':
                event_type = EventType.SNAPSHOT
            
            data = {
                'bids': self._parse_levels(row.get('bids', '[]')),
                'asks': self._parse_levels(row.get('asks', '[]')),
            }
        
        elif name == 'trades':
            data = {
                'price': float(row.get('price', 0)),
                'quantity': float(row.get('amount', row.get('quantity', 0))),
                'side': row.get('side', 'unknown'),
            }
        
        elif name == 'ticker':
            data = {
                'last_price': float(row.get('last_price', 0)),
                'funding_rate': row.get('funding_rate'),
            }
        
        elif name == 'liquidations':
            data = {
                'side': row.get('side', 'unknown'),
                'quantity': float(row.get('original_quantity', row.get('quantity', 0))),
                'price': float(row.get('price', 0)),
            }
        
        return Event(
            event_type=event_type,
            timestamp=timestamp,
            local_timestamp=local_timestamp,
            data=data
        )
    
    def _parse_levels(self, levels_str) -> List:
        """호가 레벨 파싱"""
        if pd.isna(levels_str) or levels_str == '[]':
            return []
        
        import ast
        try:
            if isinstance(levels_str, str):
                return ast.literal_eval(levels_str)
            return levels_str
        except:
            return []