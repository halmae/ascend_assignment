import pandas as pd
from pathlib import Path
from typing import Dict, Optional

class DataLoader:
    """
    Class for Loading Research / Validation 
    """

    def __init__(self, data_dir: str = "data/research"):
        """
        Args:
            data_dir : path for data directory
        """
        self.data_dir = Path(data_dir)
        self.streams = {}


    def load_stream(self, stream_name: str) -> pd.DataFrame:
        """
        
        Args:
            stream_name: 'orderbook', 'trades', 'liquidations', 'ticker'

        Returns:
            DataFrame
        """
        file_path = self.data_dir / f"{stream_name}.csv.gz"

        if not file_path.exists():
            raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")
        
        print(f"Loading {stream_name}...")
        df = pd.read_csv(file_path)

        # Timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='us')
        df['local_timestamp'] = pd.to_datetime(df['local_timestamp'], unit='us')

        # In time-order
        # df = df.sort_values('timestamp').reset_index(drop=True)

        self.streams[stream_name] = df
        return df
    

    def load_all_streams(self) -> Dict[str, pd.DataFrame]:
        """
        Load all streams
        """
        stream_names = ['orderbook', 'trades', 'liquidations', 'ticker']

        for name in stream_names:
            try:
                self.load_stream(name)
            except FileNotFoundError as e:
                print(f" ! {e}")

        return self.streams
    

    def get_unified_timeline(self) -> pd.DataFrame:
        """
        모든 스트림을 시간순으로 통합
        실시간 시뮬레이션용
        """
        events = []

        for stream_name, df in self.streams.items():
            for _, row in df.iterrows():
                events.append({
                    'timestamp': row['timestamp'],
                    'stream' : stream_name,
                    'data': row.to_dict()
                })

        events_df = pd.DataFrame(events).sort_values('timestamp').reset_index(drop=True)

        return events_df