"""
데이터 로딩 모듈
CSV 파일 또는 WebSocket 데이터 로딩
"""
import pandas as pd
from pathlib import Path
from typing import Dict, Optional, Literal


class DataLoader:
    """
    연구용(Research) 및 검증용(Validation) 데이터 로더
    """

    def __init__(self, 
                 data_dir: str = "data/research",
                 mode: Literal["research", "validation"] = "research"):
        """
        Args:
            data_dir: 데이터 디렉토리 경로
            mode: 'research' (CSV) 또는 'validation' (WebSocket)
        """
        self.data_dir = Path(data_dir)
        self.mode = mode
        self.streams = {}

    def load_stream(self, 
                    stream_name: str,
                    convert_timestamp: bool = True) -> pd.DataFrame:
        """
        개별 스트림 로드
        
        Args:
            stream_name: 'orderbook', 'trades', 'liquidations', 'ticker'
            convert_timestamp: timestamp를 datetime으로 변환할지 여부
                              (StreamProcessor에서는 int로 유지 필요)

        Returns:
            DataFrame
        """
        # .csv.gz 먼저 시도
        file_path = self.data_dir / f"{stream_name}.csv.gz"
        
        if not file_path.exists():
            # .csv 시도
            file_path = self.data_dir / f"{stream_name}.csv"
            if not file_path.exists():
                raise FileNotFoundError(f"파일을 찾을 수 없습니다: {stream_name}")
        
        print(f"📂 Loading {stream_name} from {file_path}...")
        df = pd.read_csv(file_path)

        # Timestamp 처리
        if convert_timestamp:
            # Datetime으로 변환 (분석용)
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='us')
            if 'local_timestamp' in df.columns:
                df['local_timestamp'] = pd.to_datetime(df['local_timestamp'], unit='us')
        else:
            # Integer로 유지 (스트리밍용)
            if 'timestamp' in df.columns:
                df['timestamp'] = df['timestamp'].astype('int64')
            if 'local_timestamp' in df.columns:
                df['local_timestamp'] = df['local_timestamp'].astype('int64')

        self.streams[stream_name] = df
        print(f"✅ Loaded {len(df):,} rows")
        
        return df

    def load_all_streams(self, 
                        convert_timestamp: bool = True,
                        streams: list = None) -> Dict[str, pd.DataFrame]:
        """
        모든 스트림 로드
        
        Args:
            convert_timestamp: timestamp 변환 여부
            streams: 로드할 스트림 리스트 (None이면 전체)
            
        Returns:
            스트림 이름 → DataFrame 딕셔너리
        """
        if streams is None:
            stream_names = ['orderbook', 'trades', 'liquidations', 'ticker']
        else:
            stream_names = streams

        for name in stream_names:
            try:
                self.load_stream(name, convert_timestamp=convert_timestamp)
            except FileNotFoundError as e:
                print(f"⚠️ {e}")

        return self.streams

    def get_stream(self, stream_name: str) -> Optional[pd.DataFrame]:
        """
        로드된 스트림 가져오기
        
        Args:
            stream_name: 스트림 이름
            
        Returns:
            DataFrame 또는 None
        """
        return self.streams.get(stream_name)

    def get_unified_timeline(self) -> pd.DataFrame:
        """
        모든 스트림을 시간순으로 통합
        실시간 시뮬레이션용
        
        Returns:
            통합된 DataFrame (timestamp, stream, data 컬럼)
        """
        events = []

        for stream_name, df in self.streams.items():
            for _, row in df.iterrows():
                events.append({
                    'timestamp': row['timestamp'],
                    'local_timestamp': row.get('local_timestamp', row['timestamp']),
                    'stream': stream_name,
                    'data': row.to_dict()
                })

        if not events:
            raise ValueError("로드된 스트림이 없습니다. load_all_streams()를 먼저 호출하세요.")

        events_df = pd.DataFrame(events)
        events_df = events_df.sort_values('local_timestamp').reset_index(drop=True)

        print(f"✅ Unified timeline: {len(events_df):,} events")
        
        return events_df
    
    def get_summary(self) -> pd.DataFrame:
        """
        로드된 스트림 요약 정보
        
        Returns:
            요약 DataFrame
        """
        summary = []
        
        for stream_name, df in self.streams.items():
            summary.append({
                'Stream': stream_name,
                'Rows': len(df),
                'Columns': len(df.columns),
                'Memory (MB)': df.memory_usage(deep=True).sum() / 1e6,
                'Start Time': df['timestamp'].min() if 'timestamp' in df.columns else None,
                'End Time': df['timestamp'].max() if 'timestamp' in df.columns else None
            })
        
        return pd.DataFrame(summary)