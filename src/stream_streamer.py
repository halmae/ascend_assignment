"""
DataFrame 기반 스트림 시뮬레이터 (최적화 버전)
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict, List
from src.data_types import Event
from src.enums import EventType
from src.data_loader import DataLoader


class DataFrameStreamer:
    """DataFrame을 실시간 스트림처럼 처리하는 시뮬레이터 (최적화)"""
    
    @classmethod
    def from_loader(cls, loader: DataLoader) -> 'DataFrameStreamer':
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
        
        print("⚡ 최적화된 Streamer 초기화 중...")
        
        # 1. Snapshot 분리 및 그룹화
        self.snapshot_groups = self._group_snapshots(orderbook_df)
        
        # 2. Snapshot 제외한 orderbook만 필터링
        if 'is_snapshot' in orderbook_df.columns:
            orderbook_updates = orderbook_df[orderbook_df['is_snapshot'] != True].copy()
        else:
            orderbook_updates = orderbook_df.copy()
        
        # 3. NumPy 배열로 변환 (핵심 최적화)
        # Orderbook
        self.ob_timestamps = orderbook_updates['local_timestamp'].values
        self.ob_event_timestamps = orderbook_updates['timestamp'].values
        self.ob_prices = orderbook_updates['price'].values.astype(np.float64)
        self.ob_amounts = orderbook_updates['amount'].values.astype(np.float64)
        self.ob_sides = orderbook_updates['side'].values
        self.ob_len = len(orderbook_updates)
        self.ob_idx = 0
        
        # Trades
        trades_sorted = trades_df.sort_values('local_timestamp').reset_index(drop=True)
        self.tr_timestamps = trades_sorted['local_timestamp'].values
        self.tr_event_timestamps = trades_sorted['timestamp'].values
        self.tr_prices = trades_sorted['price'].values.astype(np.float64)
        self.tr_amounts = trades_sorted['amount'].values.astype(np.float64)
        self.tr_sides = trades_sorted['side'].values
        self.tr_len = len(trades_sorted)
        self.tr_idx = 0
        
        # Ticker - DataFrame 유지 (컬럼이 많아서)
        self.ticker = ticker_df.drop_duplicates(
            subset=['timestamp'], keep='first'
        ).sort_values('local_timestamp').reset_index(drop=True)
        self.tk_timestamps = self.ticker['local_timestamp'].values
        self.tk_len = len(self.ticker)
        self.tk_idx = 0
        
        # Snapshot
        self.snapshot_timestamps = np.array([s['local_timestamp'] for s in self.snapshot_groups])
        self.snap_len = len(self.snapshot_groups)
        self.snap_idx = 0
        
        print(f"✅ 최적화된 Streamer 초기화 완료")
        print(f"  Orderbook updates: {self.ob_len:,}")
        print(f"  Trades: {self.tr_len:,}")
        print(f"  Ticker: {self.tk_len:,}")
        print(f"  Snapshots: {self.snap_len}")
    
    def _group_snapshots(self, orderbook_df: pd.DataFrame) -> List[Dict]:
        """Snapshot 그룹화"""
        if 'is_snapshot' not in orderbook_df.columns:
            return []
        
        snapshots = orderbook_df[orderbook_df['is_snapshot'] == True]
        if snapshots.empty:
            return []
        
        snapshot_groups = []
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
            
            snapshot_groups.append({
                'timestamp': int(ts),
                'local_timestamp': int(group['local_timestamp'].iloc[0]),
                'bids': bids,
                'asks': asks
            })
        
        snapshot_groups.sort(key=lambda x: x['local_timestamp'])
        return snapshot_groups

    def get_next_event(self) -> Optional[Event]:
        """다음 이벤트 가져오기 (최적화)"""
        
        # 각 스트림의 다음 timestamp 가져오기 (없으면 inf)
        ob_ts = self.ob_timestamps[self.ob_idx] if self.ob_idx < self.ob_len else np.inf
        tr_ts = self.tr_timestamps[self.tr_idx] if self.tr_idx < self.tr_len else np.inf
        tk_ts = self.tk_timestamps[self.tk_idx] if self.tk_idx < self.tk_len else np.inf
        snap_ts = self.snapshot_timestamps[self.snap_idx] if self.snap_idx < self.snap_len else np.inf
        
        # 모두 inf면 종료
        min_ts = min(ob_ts, tr_ts, tk_ts, snap_ts)
        if min_ts == np.inf:
            return None
        
        # 가장 빠른 이벤트 선택 및 반환
        if min_ts == snap_ts:
            event = self._create_snapshot_event(self.snapshot_groups[self.snap_idx])
            self.snap_idx += 1
            return event
        
        if min_ts == ob_ts:
            event = Event(
                event_type=EventType.ORDERBOOK,
                timestamp=int(self.ob_event_timestamps[self.ob_idx]),
                local_timestamp=int(self.ob_timestamps[self.ob_idx]),
                data={
                    'is_snapshot': False,
                    'price': self.ob_prices[self.ob_idx],
                    'amount': self.ob_amounts[self.ob_idx],
                    'side': self.ob_sides[self.ob_idx]
                }
            )
            self.ob_idx += 1
            return event
        
        if min_ts == tr_ts:
            event = Event(
                event_type=EventType.TRADE,
                timestamp=int(self.tr_event_timestamps[self.tr_idx]),
                local_timestamp=int(self.tr_timestamps[self.tr_idx]),
                data={
                    'price': self.tr_prices[self.tr_idx],
                    'amount': self.tr_amounts[self.tr_idx],
                    'side': self.tr_sides[self.tr_idx]
                }
            )
            self.tr_idx += 1
            return event
        
        if min_ts == tk_ts:
            row = self.ticker.iloc[self.tk_idx]
            event = Event(
                event_type=EventType.TICKER,
                timestamp=int(row['timestamp']),
                local_timestamp=int(row['local_timestamp']),
                data=row.to_dict()
            )
            self.tk_idx += 1
            return event
        
        return None
    
    def _create_snapshot_event(self, snapshot_group: Dict) -> Event:
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
    
    def has_more_events(self) -> bool:
        return (
            self.ob_idx < self.ob_len or
            self.tr_idx < self.tr_len or
            self.tk_idx < self.tk_len or
            self.snap_idx < self.snap_len
        )
    
    def get_progress(self) -> Dict[str, str]:
        return {
            'orderbook': f"{self.ob_idx:,} / {self.ob_len:,}",
            'trades': f"{self.tr_idx:,} / {self.tr_len:,}",
            'ticker': f"{self.tk_idx:,} / {self.tk_len:,}",
            'snapshots': f"{self.snap_idx} / {self.snap_len}"
        }
    
    def reset(self):
        self.ob_idx = 0
        self.tr_idx = 0
        self.tk_idx = 0
        self.snap_idx = 0