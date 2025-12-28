"""
ASCEND Challenge - 데이터 탐색 및 분석
Phase 0: 데이터 이해 및 가설 수립을 위한 탐색적 분석
"""

import pandas as pd
import numpy as np
from pathlib import Path
import gzip
import json
from datetime import datetime, timedelta
from collections import Counter

class DataExplorer:
    """4가지 데이터 스트림을 탐색하는 클래스"""
    
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.streams = {
            'trades': None,
            'orderbook': None,
            'liquidations': None,
            'ticker': None
        }
        
    def load_sample(self, stream_name: str, nrows: int = 10000):
        """데이터 샘플 로드 (메모리 절약을 위해 일부만)"""
        file_path = self.data_dir / f"{stream_name}.csv.gz"
        
        print(f"\n{'='*60}")
        print(f"📂 Loading {stream_name}.csv.gz...")
        print(f"{'='*60}")
        
        try:
            # 압축 파일 읽기
            df = pd.read_csv(file_path, nrows=nrows)
            self.streams[stream_name] = df
            
            print(f"✅ Loaded {len(df):,} rows")
            print(f"📊 Columns: {list(df.columns)}")
            print(f"💾 Memory: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
            
            return df
        except Exception as e:
            print(f"❌ Error loading {stream_name}: {e}")
            return None
    
    def analyze_structure(self, stream_name: str):
        """데이터 구조 분석"""
        df = self.streams.get(stream_name)
        if df is None:
            print(f"⚠️  {stream_name} not loaded")
            return
        
        print(f"\n{'='*60}")
        print(f"🔍 STRUCTURE ANALYSIS: {stream_name.upper()}")
        print(f"{'='*60}\n")
        
        # 1. 기본 정보
        print("📋 Basic Info:")
        print(f"   Rows: {len(df):,}")
        print(f"   Columns: {len(df.columns)}")
        print(f"   Dtypes:\n{df.dtypes}\n")
        
        # 2. 샘플 데이터
        print("📝 Sample Data (first 3 rows):")
        print(df.head(3))
        print()
        
        # 3. 결측값
        null_counts = df.isnull().sum()
        if null_counts.any():
            print("⚠️  Null Values:")
            print(null_counts[null_counts > 0])
        else:
            print("✅ No null values")
        print()
        
        # 4. 기술 통계
        print("📈 Numerical Statistics:")
        print(df.describe())
        print()
        
        return df
    
    def analyze_timestamps(self, stream_name: str, timestamp_col: str = 'timestamp'):
        """타임스탬프 분석 - Dirty Data의 핵심"""
        df = self.streams.get(stream_name)
        if df is None or timestamp_col not in df.columns:
            print(f"⚠️  Cannot analyze timestamps for {stream_name}")
            return
        
        print(f"\n{'='*60}")
        print(f"⏰ TIMESTAMP ANALYSIS: {stream_name.upper()}")
        print(f"{'='*60}\n")
        
        # 타임스탬프를 숫자로 변환 (밀리초 또는 마이크로초)
        ts = df[timestamp_col].values
        
        # 1. 기본 정보
        print("📅 Time Range:")
        print(f"   First: {ts[0]}")
        print(f"   Last: {ts[-1]}")
        print(f"   Duration: {ts[-1] - ts[0]:,} units")
        print()
        
        # 2. Out-of-order 체크
        diff = np.diff(ts)
        out_of_order = np.sum(diff < 0)
        out_of_order_pct = (out_of_order / len(diff)) * 100
        
        print("🔀 Out-of-Order Check:")
        print(f"   Total events: {len(ts):,}")
        print(f"   Out-of-order: {out_of_order:,} ({out_of_order_pct:.3f}%)")
        
        if out_of_order > 0:
            print(f"   ⚠️  DIRTY DATA DETECTED!")
            # 가장 큰 역전 찾기
            worst_idx = np.argmin(diff)
            print(f"   Worst case at index {worst_idx}:")
            print(f"      Before: {ts[worst_idx]}")
            print(f"      After: {ts[worst_idx + 1]}")
            print(f"      Diff: {diff[worst_idx]:,}")
        else:
            print(f"   ✅ All timestamps in order")
        print()
        
        # 3. 간격 분석
        intervals = diff[diff > 0]  # 양수만
        print("⏱️  Interval Statistics (positive only):")
        print(f"   Mean: {np.mean(intervals):.2f}")
        print(f"   Median: {np.median(intervals):.2f}")
        print(f"   Std: {np.std(intervals):.2f}")
        print(f"   Min: {np.min(intervals):.2f}")
        print(f"   Max: {np.max(intervals):.2f}")
        print()
        
        # 4. 간격 분포 (히스토그램)
        print("📊 Interval Distribution:")
        percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
        for p in percentiles:
            val = np.percentile(intervals, p)
            print(f"   {p:2d}th percentile: {val:.2f}")
        print()
        
        # 5. 중복 타임스탬프
        duplicates = len(ts) - len(np.unique(ts))
        dup_pct = (duplicates / len(ts)) * 100
        print(f"🔁 Duplicate Timestamps:")
        print(f"   Count: {duplicates:,} ({dup_pct:.3f}%)")
        if duplicates > 0:
            print(f"   ⚠️  DUPLICATE EVENTS DETECTED!")
        print()
        
        return {
            'out_of_order': out_of_order,
            'out_of_order_pct': out_of_order_pct,
            'duplicates': duplicates,
            'dup_pct': dup_pct,
            'mean_interval': np.mean(intervals),
            'median_interval': np.median(intervals)
        }
    
    def analyze_liquidations(self):
        """청산 이벤트 분석 - 프로젝트의 핵심"""
        df = self.streams.get('liquidations')
        if df is None:
            print("⚠️  Liquidations not loaded")
            return
        
        print(f"\n{'='*60}")
        print(f"💥 LIQUIDATION ANALYSIS")
        print(f"{'='*60}\n")
        
        # 1. 기본 통계
        print("📊 Basic Statistics:")
        print(f"   Total liquidations: {len(df):,}")
        
        if 'quantity' in df.columns or 'qty' in df.columns:
            qty_col = 'quantity' if 'quantity' in df.columns else 'qty'
            print(f"   Total quantity: {df[qty_col].sum():,.2f}")
            print(f"   Mean quantity: {df[qty_col].mean():.2f}")
            print(f"   Median quantity: {df[qty_col].median():.2f}")
            print(f"   Max quantity: {df[qty_col].max():.2f}")
        
        if 'side' in df.columns:
            print(f"\n   By Side:")
            print(df['side'].value_counts())
        print()
        
        # 2. 대규모 청산 탐지
        if 'quantity' in df.columns or 'qty' in df.columns:
            qty_col = 'quantity' if 'quantity' in df.columns else 'qty'
            
            # 상위 1% 를 "대규모"로 정의
            threshold_99 = df[qty_col].quantile(0.99)
            threshold_95 = df[qty_col].quantile(0.95)
            
            massive_liq = df[df[qty_col] >= threshold_99]
            large_liq = df[df[qty_col] >= threshold_95]
            
            print("🔥 Large Liquidation Events:")
            print(f"   99th percentile threshold: {threshold_99:.2f}")
            print(f"   Massive liquidations (>99%): {len(massive_liq):,}")
            print(f"   95th percentile threshold: {threshold_95:.2f}")
            print(f"   Large liquidations (>95%): {len(large_liq):,}")
            print()
            
            # 가장 큰 청산 이벤트들
            print("💣 Top 5 Largest Liquidations:")
            top5 = df.nlargest(5, qty_col)
            for idx, row in top5.iterrows():
                print(f"   {row[qty_col]:.2f} @ {row.get('price', 'N/A')}")
            print()
        
        # 3. 시간별 분포 (타임스탬프가 있다면)
        if 'timestamp' in df.columns:
            ts_stats = self.analyze_timestamps('liquidations')
            
            # 청산이 집중된 구간 찾기
            timestamps = df['timestamp'].values
            diff = np.diff(timestamps)
            
            # 짧은 시간에 많은 청산 = cascade
            print("⚡ Liquidation Cascade Detection:")
            print("   Looking for rapid succession of liquidations...")
            
            # 1초 내에 발생한 청산 그룹
            one_second = 1000000  # 마이크로초 기준
            if diff.max() > one_second:  # 밀리초 기준이면
                one_second = 1000
            
            rapid_events = np.sum(diff < one_second)
            rapid_pct = (rapid_events / len(diff)) * 100
            
            print(f"   Events within 1 second of previous: {rapid_events:,} ({rapid_pct:.2f}%)")
            print()
        
        return df
    
    def analyze_orderbook(self):
        """오더북 분석 - 유동성 및 spread"""
        df = self.streams.get('orderbook')
        if df is None:
            print("⚠️  Orderbook not loaded")
            return
        
        print(f"\n{'='*60}")
        print(f"📖 ORDERBOOK ANALYSIS")
        print(f"{'='*60}\n")
        
        print("📊 Columns available:")
        print(df.columns.tolist())
        print()
        
        # Bid/Ask spread 분석
        if 'best_bid' in df.columns and 'best_ask' in df.columns:
            df['spread'] = df['best_ask'] - df['best_bid']
            df['spread_pct'] = (df['spread'] / df['best_bid']) * 100
            
            print("💰 Bid-Ask Spread Analysis:")
            print(f"   Mean spread: {df['spread'].mean():.2f}")
            print(f"   Median spread: {df['spread'].median():.2f}")
            print(f"   Mean spread %: {df['spread_pct'].mean():.4f}%")
            print(f"   Max spread: {df['spread'].max():.2f}")
            print(f"   Max spread %: {df['spread_pct'].max():.4f}%")
            print()
            
            # Crossed market 탐지 (bid > ask)
            crossed = df[df['best_bid'] > df['best_ask']]
            if len(crossed) > 0:
                print(f"⚠️  CROSSED MARKET DETECTED!")
                print(f"   Count: {len(crossed):,} ({len(crossed)/len(df)*100:.3f}%)")
                print(f"   This is DIRTY DATA!")
                print()
            
            # Wide spread (비정상적으로 큰 spread)
            spread_99 = df['spread_pct'].quantile(0.99)
            wide_spread = df[df['spread_pct'] > spread_99]
            print(f"📏 Wide Spread Events (>99th percentile):")
            print(f"   Threshold: {spread_99:.4f}%")
            print(f"   Count: {len(wide_spread):,}")
            print()
        
        # Depth 분석
        depth_cols = [col for col in df.columns if 'qty' in col or 'quantity' in col]
        if depth_cols:
            print(f"📚 Depth Information:")
            print(f"   Available depth columns: {depth_cols}")
            # 추가 분석...
        
        return df
    
    def analyze_trades(self):
        """거래 분석"""
        df = self.streams.get('trades')
        if df is None:
            print("⚠️  Trades not loaded")
            return
        
        print(f"\n{'='*60}")
        print(f"💱 TRADE ANALYSIS")
        print(f"{'='*60}\n")
        
        print("📊 Basic Statistics:")
        print(f"   Total trades: {len(df):,}")
        
        if 'price' in df.columns:
            print(f"   Price range: {df['price'].min():.2f} - {df['price'].max():.2f}")
            print(f"   Mean price: {df['price'].mean():.2f}")
            
            # Fat-finger 가격 탐지
            price_mean = df['price'].mean()
            price_std = df['price'].std()
            
            # 평균에서 5 표준편차 이상 벗어난 가격
            outliers = df[np.abs(df['price'] - price_mean) > 5 * price_std]
            
            if len(outliers) > 0:
                print(f"\n⚠️  FAT-FINGER PRICES DETECTED!")
                print(f"   Count: {len(outliers):,}")
                print(f"   Extreme prices:")
                for idx, row in outliers.head(5).iterrows():
                    print(f"      {row['price']:.2f}")
                print()
        
        if 'quantity' in df.columns or 'qty' in df.columns:
            qty_col = 'quantity' if 'quantity' in df.columns else 'qty'
            print(f"   Total volume: {df[qty_col].sum():,.2f}")
            print(f"   Mean size: {df[qty_col].mean():.4f}")
        
        if 'side' in df.columns:
            print(f"\n   By Side:")
            print(df['side'].value_counts())
        
        return df
    
    def generate_hypothesis(self):
        """분석 결과를 바탕으로 가설 생성 제안"""
        print(f"\n{'='*60}")
        print(f"💡 HYPOTHESIS SUGGESTIONS")
        print(f"{'='*60}\n")
        
        print("Based on the data analysis, here are potential hypotheses:\n")
        
        # Liquidation 관련
        liq = self.streams.get('liquidations')
        if liq is not None:
            print("1️⃣  LIQUIDATION-BASED HYPOTHESIS:")
            print("   H1: After a liquidation event exceeding X quantity,")
            print("       the orderbook becomes unreliable for Y seconds.")
            print()
            print("   H2: When multiple liquidations occur within Z milliseconds,")
            print("       it indicates a cascade and decision should be HALTED.")
            print()
        
        # Orderbook 관련
        ob = self.streams.get('orderbook')
        if ob is not None and 'spread' in ob.columns:
            spread_99 = ob['spread_pct'].quantile(0.99)
            print("2️⃣  ORDERBOOK-BASED HYPOTHESIS:")
            print(f"   H3: When bid-ask spread exceeds {spread_99:.4f}%,")
            print("       the market is illiquid and decisions should be RESTRICTED.")
            print()
            print("   H4: Crossed market (bid > ask) indicates data corruption,")
            print("       triggering immediate QUARANTINE.")
            print()
        
        # Timestamp 관련
        print("3️⃣  DATA QUALITY HYPOTHESIS:")
        print("   H5: Out-of-order timestamps indicate system overload,")
        print("       degrading Trust State from TRUSTED to DEGRADED.")
        print()
        print("   H6: When event delays exceed P milliseconds,")
        print("       real-time decision-making becomes unreliable.")
        print()
        
        print("4️⃣  COMBINED HYPOTHESIS:")
        print("   H7: The combination of:")
        print("       - Large liquidation")
        print("       - Wide spread")
        print("       - Out-of-order data")
        print("       should immediately HALT all decisions until recovery.")
        print()
        
        print("\n" + "="*60)
        print("Next Step: Define specific thresholds (X, Y, Z, P) from data")
        print("="*60)


def main():
    """메인 실행 함수"""
    print("""
╔══════════════════════════════════════════════════════════╗
║                                                          ║
║          ASCEND Challenge - Data Explorer                ║
║       Phase 0: Understanding Your Data                   ║
║                                                          ║
╚══════════════════════════════════════════════════════════╝
""")
    
    # 데이터 경로 설정 (사용자가 수정해야 함)
    data_dir = "data/validation"
    
    print(f"📂 Data directory: {data_dir}")
    print(f"⏳ Loading data samples...\n")
    
    explorer = DataExplorer(data_dir)
    
    # 1. 모든 스트림 로드 (샘플)
    for stream in ['trades', 'orderbook', 'liquidations', 'ticker']:
        explorer.load_sample(stream, nrows=50000)  # 5만 행씩 샘플링
    
    print("\n" + "="*60)
    print("✅ All streams loaded! Starting analysis...")
    print("="*60)
    
    # 2. 각 스트림 분석
    for stream in ['liquidations', 'orderbook', 'trades', 'ticker']:
        if explorer.streams[stream] is not None:
            explorer.analyze_structure(stream)
            explorer.analyze_timestamps(stream)
    
    # 3. 특화 분석
    explorer.analyze_liquidations()
    explorer.analyze_orderbook()
    explorer.analyze_trades()
    
    # 4. 가설 제안
    explorer.generate_hypothesis()
    
    print("\n✅ Analysis complete!")
    print("💾 Review the output and start forming your hypotheses.")
    
    return explorer


if __name__ == "__main__":
    explorer = main()