"""
DataFrame 기반 스트림 시뮬레이터
"""
import pandas as pd
from typing import Optional, Dict, List
from collections import deque
from src.data_types import Event
from src.enums import EventType
from src.data_loader import DataLoader


class DataFrameStreamer:
    """DataFrame을 실시간 스트림처럼 처리하는 시뮬레이터"""
    
    @classmethod
    def from_loader(cls, loader: DataLoader) -> 'DataFrameStreamer':
        """
        DataLoader로부터 Streamer 생성
        
        Args:
            loader: 데이터가 로드된 DataLoader 인스턴스
            
        Returns:
            DataFrameStreamer 인스턴스
        """
        orderbook = loader.get_stream('orderbook')
        trades = loader.get_stream('trades')
        ticker = loader.get_stream('ticker')
        
        if orderbook is None:
            raise ValueError("orderbook 스트림이 필요합니다.")
        if trades is None:
            raise ValueError("trades 스트림이 필요합니다.")
        if ticker is None:
            raise ValueError("ticker 스트림이 필요합니다.")
        
        return cls(orderbook, trades, ticker)
    
    def __init__(self, 
                 orderbook_df: pd.DataFrame,
                 trades_df: pd.DataFrame,
                 ticker_df: pd.DataFrame):
        """
        Args:
            orderbook_df: Orderbook DataFrame
            trades_df: Trades DataFrame
            ticker_df: Ticker DataFrame
        """
        
        # 데이터 복사 및 정렬
        self.orderbook = orderbook_df.sort_values('local_timestamp').reset_index(drop=True)
        self.trades = trades_df.sort_values('local_timestamp').reset_index(drop=True)
        
        # Ticker 중복 제거 및 정렬
        self.ticker = ticker_df.drop_duplicates(
            subset=['timestamp'], 
            keep='first'
        ).sort_values('local_timestamp').reset_index(drop=True)
        
        # Snapshot 그룹화
        self.snapshot_groups = self._group_snapshots()
        
        # 현재 읽기 위치
        self.orderbook_idx = 0
        self.trades_idx = 0
        self.ticker_idx = 0
        self.snapshot_group_idx = 0
        
        print(f"✅ DataFrameStreamer 초기화 완료")
        print(f"  Orderbook: {len(self.orderbook):,} rows")
        print(f"  Trades: {len(self.trades):,} rows")
        print(f"  Ticker: {len(self.ticker):,} rows (중복 제거 후)")
        print(f"  Snapshot groups: {len(self.snapshot_groups)} groups")
    
    def _group_snapshots(self) -> List[Dict]:
        """
        Orderbook snapshot을 timestamp별로 그룹화
        
        Returns:
            List of snapshot groups
            [
                {
                    'timestamp': int,
                    'local_timestamp': int,
                    'bids': [[price, amount], ...],
                    'asks': [[price, amount], ...]
                },
                ...
            ]
        """
        # ⭐ 컬럼 존재 확인
        if 'is_snapshot' not in self.orderbook.columns:
            print("⚠️ Warning: 'is_snapshot' 컬럼이 없습니다.")
            return []
        
        # ⭐ 올바른 필터링 (DataFrame 인덱싱)
        snapshots = self.orderbook[self.orderbook['is_snapshot'] == True].copy()
        
        if snapshots.empty:
            print("⚠️ Snapshot이 없습니다.")
            return []
        
        print(f"✅ Snapshot 발견: {len(snapshots):,} rows")
        
        # Timestamp별로 그룹화
        snapshot_groups = []
        
        unique_timestamps = snapshots['timestamp'].unique()
        print(f"✅ Unique snapshot timestamps: {len(unique_timestamps)}")
        
        for ts in unique_timestamps:
            group = snapshots[snapshots['timestamp'] == ts]
            
            bids = []
            asks = []
            
            for _, row in group.iterrows():
                price = float(row['price'])
                amount = float(row['amount'])
                side = row['side']
                
                if side == 'bid':
                    bids.append([price, amount])
                else:
                    asks.append([price, amount])
            
            snapshot_groups.append({
                'timestamp': int(ts),
                'local_timestamp': int(group['local_timestamp'].iloc[0]),
                'bids': bids,
                'asks': asks
            })
            
            print(f"  Snapshot @ {ts}: {len(bids)} bids, {len(asks)} asks")
        
        # local_timestamp 순으로 정렬
        snapshot_groups.sort(key=lambda x: x['local_timestamp'])
        
        print(f"✅ Snapshot 그룹 생성 완료: {len(snapshot_groups)} groups")
        
        return snapshot_groups
    

    def _create_orderbook_event(self, row: pd.Series) -> Event:
        """Orderbook Update Event 생성"""
        return Event(
            event_type=EventType.ORDERBOOK,
            timestamp=int(row['timestamp']),
            local_timestamp=int(row['local_timestamp']),
            data={
                'is_snapshot': bool(row['is_snapshot']),  # ⭐ 명시적 bool 변환
                'price': float(row['price']),
                'amount': float(row['amount']),
                'side': row['side']
            }
        )


    def get_next_event(self) -> Optional[Event]:
        """
        다음 이벤트 가져오기 (시간순)
        
        Returns:
            Event 또는 None (더 이상 없으면)
        """
        # 각 스트림의 다음 이벤트 timestamp 확인
        candidates = []
        
        # 1. Snapshot group
        if self.snapshot_group_idx < len(self.snapshot_groups):
            snapshot_group = self.snapshot_groups[self.snapshot_group_idx]
            candidates.append({
                'type': 'snapshot',
                'timestamp': snapshot_group['local_timestamp'],
                'data': snapshot_group
            })
        
        # 2. Orderbook update (snapshot 제외)
        if self.orderbook_idx < len(self.orderbook):
            row = self.orderbook.iloc[self.orderbook_idx]
            
            # ⭐ Snapshot이 아닌 것만 (올바른 체크)
            if row['is_snapshot'] != True:  # 또는 row['is_snapshot'] == False
                candidates.append({
                    'type': 'orderbook',
                    'timestamp': row['local_timestamp'],
                    'data': row
                })
            else:
                # Snapshot 행이면 건너뛰기
                self.orderbook_idx += 1
                # 재귀 호출하여 다음 이벤트 가져오기
                return self.get_next_event()
        
        # 3. Trade
        if self.trades_idx < len(self.trades):
            row = self.trades.iloc[self.trades_idx]
            candidates.append({
                'type': 'trade',
                'timestamp': row['local_timestamp'],
                'data': row
            })
        
        # 4. Ticker
        if self.ticker_idx < len(self.ticker):
            row = self.ticker.iloc[self.ticker_idx]
            candidates.append({
                'type': 'ticker',
                'timestamp': row['local_timestamp'],
                'data': row
            })
        
        # 후보가 없으면 종료
        if not candidates:
            return None
        
        # 가장 빠른 timestamp 선택
        earliest = min(candidates, key=lambda x: x['timestamp'])
        
        # Event 생성 및 인덱스 증가
        if earliest['type'] == 'snapshot':
            event = self._create_snapshot_event(earliest['data'])
            self.snapshot_group_idx += 1
        elif earliest['type'] == 'orderbook':
            event = self._create_orderbook_event(earliest['data'])
            self.orderbook_idx += 1
        elif earliest['type'] == 'trade':
            event = self._create_trade_event(earliest['data'])
            self.trades_idx += 1
        elif earliest['type'] == 'ticker':
            event = self._create_ticker_event(earliest['data'])
            self.ticker_idx += 1
        
        return event
    
    def _create_snapshot_event(self, snapshot_group: Dict) -> Event:
        """Snapshot Event 생성"""
        return Event(
            event_type=EventType.ORDERBOOK,
            timestamp=snapshot_group['timestamp'],
            local_timestamp=snapshot_group['local_timestamp'],
            data={
                'is_snapshot': True,
                'bids': snapshot_group['bids'],
                'asks': snapshot_group['asks']
            }
        )
    
    def _create_orderbook_event(self, row: pd.Series) -> Event:
        """Orderbook Update Event 생성"""
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
    
    def _create_trade_event(self, row: pd.Series) -> Event:
        """Trade Event 생성"""
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
    
    def _create_ticker_event(self, row: pd.Series) -> Event:
        """Ticker Event 생성"""
        # Ticker 데이터의 모든 컬럼을 포함
        data = row.to_dict()
        
        # timestamp는 int로 변환
        if 'timestamp' in data:
            data['timestamp'] = int(data['timestamp'])
        if 'local_timestamp' in data:
            data['local_timestamp'] = int(data['local_timestamp'])
        
        return Event(
            event_type=EventType.TICKER,
            timestamp=int(row['timestamp']),
            local_timestamp=int(row['local_timestamp']),
            data=data
        )
    
    def has_more_events(self) -> bool:
        """아직 처리할 이벤트가 있는지"""
        return (
            self.snapshot_group_idx < len(self.snapshot_groups) or
            self.orderbook_idx < len(self.orderbook) or
            self.trades_idx < len(self.trades) or
            self.ticker_idx < len(self.ticker)
        )
    
    def get_progress(self) -> Dict[str, str]:
        """
        현재 진행 상황
        
        Returns:
            {
                'orderbook': '1000 / 10000',
                'trades': '500 / 5000',
                'ticker': '100 / 1000',
                'snapshot_groups': '1 / 5'
            }
        """
        return {
            'orderbook': f"{self.orderbook_idx:,} / {len(self.orderbook):,}",
            'trades': f"{self.trades_idx:,} / {len(self.trades):,}",
            'ticker': f"{self.ticker_idx:,} / {len(self.ticker):,}",
            'snapshot_groups': f"{self.snapshot_group_idx} / {len(self.snapshot_groups)}"
        }
    
    def reset(self):
        """스트림을 처음부터 다시 시작"""
        self.orderbook_idx = 0
        self.trades_idx = 0
        self.ticker_idx = 0
        self.snapshot_group_idx = 0
        print("🔄 Streamer reset complete")