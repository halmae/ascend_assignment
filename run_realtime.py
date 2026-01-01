#!/usr/bin/env python3
"""
Phase 2: Realtime Validation

================================================================================
Single Decision Engine:
- config.py의 THRESHOLDS 사용
- Historical과 동일한 파라미터 적용
================================================================================

사용법:
    python run_realtime.py                    # 기본 60초
    python run_realtime.py --duration 300     # 5분
    python run_realtime.py --show-config
"""
import sys
import asyncio
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src1.config import REALTIME_CONFIG, print_thresholds, RealtimeConfig
from src1.realtime_processor import run_realtime


def main():
    parser = argparse.ArgumentParser(description='Phase 2: Realtime Validation')
    parser.add_argument('--symbol', default=None, help='Trading symbol (default: btcusdt)')
    parser.add_argument('--duration', type=int, default=None, help='Duration in seconds')
    parser.add_argument('--output', default=None, help='Output directory')
    parser.add_argument('--show-config', action='store_true', help='Show current thresholds')
    
    args = parser.parse_args()
    
    if args.show_config:
        print_thresholds()
        return
    
    # config.py 기본값 사용, CLI로 override
    config = RealtimeConfig(
        symbol=args.symbol or REALTIME_CONFIG.symbol,
        duration_sec=args.duration or REALTIME_CONFIG.duration_sec,
        output_dir=args.output or REALTIME_CONFIG.output_dir,
    )
    
    try:
        asyncio.run(run_realtime(config))
    except KeyboardInterrupt:
        print("\n\n⚠️ 사용자에 의해 중단됨")
        sys.exit(0)


if __name__ == "__main__":
    main()