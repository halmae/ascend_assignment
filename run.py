#!/usr/bin/env python3
"""
Decision Engine - 통합 실행 파일

================================================================================
Single Decision Engine:
- Historical과 Realtime을 동일한 코드로 처리
- --mode 옵션으로 선택
================================================================================

사용법:
    # Historical Validation
    python run.py --mode historical --data ./data/research --output ./output/research
    python run.py --mode historical --data ./data/validation --output ./output/validation
    
    # Realtime Validation
    python run.py --mode realtime --duration 60
    python run.py --mode realtime --duration 1200 --symbol btcusdt
    
    # 설정 확인
    python run.py --show-config
"""
import sys
import asyncio
import argparse
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))

from src.config import THRESHOLDS, print_thresholds, get_thresholds_dict
from src.data_source import create_data_source, HistoricalDataSource, RealtimeDataSource
from src.processor import Processor


def run_historical(data_dir: str, output_dir: str, name: str = "Historical"):
    """Historical Validation 실행"""
    print("=" * 70)
    print(f"🚀 {name} Validation")
    print("=" * 70)
    print(f"Data:   {data_dir}")
    print(f"Output: {output_dir}")
    print(f"\n[Thresholds from config.py]")
    print(f"  liquidation_cooldown_ms: {THRESHOLDS.liquidation_cooldown_ms}")
    print(f"  integrity_repair_bps:    {THRESHOLDS.integrity_repair_threshold_bps}")
    print("=" * 70)
    
    # 데이터 소스 생성
    source = create_data_source('historical', data_dir=data_dir)
    
    # 프로세서 생성
    processor = Processor(mode=name.lower())
    
    # 이벤트 처리
    event_count = 0
    log_interval = 500_000
    
    for event in source.get_events():
        result = processor.process_event(event)
        event_count += 1
        
        # Liquidation 알림 (quantity > 0인 경우만)
        if result and result.get('type') == 'LIQUIDATION' and result.get('quantity', 0) > 0:
            print(f"\n  ⚠️ LIQUIDATION: {result['side']} {result['quantity']:.4f} @ {result.get('price', 0):,.2f}")
        
        # 주기적 상태 출력
        if event_count % log_interval == 0:
            print(f"\n  📊 Processed {event_count:,} events...")
            processor.print_status()
    
    # 결과
    processor.print_status()
    processor.save_outputs(output_dir)
    processor.print_summary()
    
    return processor.get_result()


async def run_realtime(symbol: str, duration_sec: int, output_dir: str):
    """Realtime Validation 실행"""
    print("=" * 70)
    print("🚀 Realtime Validation")
    print("=" * 70)
    print(f"Symbol:   {symbol.upper()}")
    print(f"Duration: {duration_sec}초")
    print(f"Output:   {output_dir}")
    print(f"\n[Robustness Features]")
    print(f"  ✅ 자동 재연결")
    print(f"  ✅ 중복 메시지 필터링")
    print(f"  ✅ Out-of-order 허용")
    print(f"\n[Thresholds from config.py]")
    print(f"  liquidation_cooldown_ms: {THRESHOLDS.liquidation_cooldown_ms}")
    print(f"  integrity_repair_bps:    {THRESHOLDS.integrity_repair_threshold_bps}")
    print("=" * 70)
    
    # 데이터 소스 생성
    source = create_data_source('realtime', symbol=symbol, duration_sec=duration_sec)
    
    # 프로세서 생성
    processor = Processor(mode='realtime')
    
    # 이벤트 처리 (비동기)
    async for event in source.get_events_async():
        result = processor.process_event(event)
        
        # Liquidation 알림 (quantity > 0인 경우만)
        if result and result.get('type') == 'LIQUIDATION' and result.get('quantity', 0) > 0:
            print(f"\n  ⚠️ LIQUIDATION: {result['side']} {result['quantity']:.4f} @ {result.get('price', 0):,.2f}")
        
        # 주기적 상태 출력
        if processor.should_log_status(interval_sec=10.0):
            processor.print_status()
    
    # 결과
    processor.print_status()
    processor.save_outputs(output_dir)
    processor.print_summary()
    
    # 데이터 소스 통계
    print(f"\n[Data Source Statistics]")
    print(f"  Events Total:       {source.stats.events_total:,}")
    print(f"  Events Processed:   {source.stats.events_processed:,}")
    print(f"  Duplicates Filtered: {source.stats.duplicates_filtered:,}")
    print(f"  Out-of-Order:       {source.stats.out_of_order:,}")
    print(f"  Reconnects:         {source.stats.reconnects:,}")
    
    return processor.get_result()


def main():
    parser = argparse.ArgumentParser(
        description='Decision Engine - Historical/Realtime Validation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Historical
  python run.py --mode historical --data ./data/research --output ./output/research
  python run.py --mode historical --data ./data/validation --output ./output/validation
  
  # Realtime
  python run.py --mode realtime --duration 60
  python run.py --mode realtime --duration 1200 --symbol btcusdt
  
  # Show config
  python run.py --show-config
        """
    )
    
    # Mode selection
    parser.add_argument('--mode', choices=['historical', 'realtime'],
                        help='Validation mode')
    
    # Historical options
    parser.add_argument('--data', type=str,
                        help='Data directory (historical mode)')
    parser.add_argument('--name', type=str, default='Dataset',
                        help='Dataset name (historical mode)')
    
    # Realtime options
    parser.add_argument('--symbol', type=str, default='btcusdt',
                        help='Trading symbol (realtime mode)')
    parser.add_argument('--duration', type=int, default=60,
                        help='Duration in seconds (realtime mode)')
    
    # Common options
    parser.add_argument('--output', type=str, default='./output',
                        help='Output directory')
    parser.add_argument('--show-config', action='store_true',
                        help='Show current thresholds and exit')
    
    args = parser.parse_args()
    
    # Show config
    if args.show_config:
        print_thresholds()
        return
    
    # Validate args
    if not args.mode:
        parser.print_help()
        return
    
    # Run
    if args.mode == 'historical':
        if not args.data:
            print("Error: --data is required for historical mode")
            return
        
        output_dir = f"{args.output}/{args.name.lower()}"
        run_historical(args.data, output_dir, args.name)
    
    elif args.mode == 'realtime':
        output_dir = f"{args.output}/realtime"
        try:
            asyncio.run(run_realtime(args.symbol, args.duration, output_dir))
        except KeyboardInterrupt:
            print("\n\n⚠️ 사용자에 의해 중단됨")


if __name__ == "__main__":
    main()