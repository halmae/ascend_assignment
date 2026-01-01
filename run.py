#!/usr/bin/env python3
"""
Decision Engine - 통합 실행 파일 (v3)

================================================================================
기능:
- Progress bar만 콘솔 출력 (깔끔)
- State transitions, decisions는 실시간 파일 기록
- Research vs Validation 비교
================================================================================

사용법:
    # Research와 Validation 비교
    python run.py --mode historical --research ./data/research --validation ./data/validation
    
    # 단일 데이터셋
    python run.py --mode historical --data ./data/research --name Research
    
    # Realtime
    python run.py --mode realtime --duration 60
"""
import sys
import asyncio
import argparse
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent))

from src.config import THRESHOLDS, print_thresholds, get_thresholds_dict
from src.data_source import create_data_source
from src.processor import Processor, ProcessingResult


# =============================================================================
# Progress Display
# =============================================================================

class ProgressDisplay:
    """깔끔한 Progress 표시"""
    
    def __init__(self, name: str = "Processing", total_estimate: int = 0):
        self.name = name
        self.total_estimate = total_estimate
        self.current = 0
        self.start_time = time.time()
        self.last_print_time = 0
        self.print_interval = 0.3  # 0.3초마다 업데이트
        
        # 이벤트 카운트
        self.ob_count = 0
        self.trade_count = 0
        self.ticker_count = 0
        self.liq_count = 0
    
    def update(self, event_type: str = None, processor: Processor = None):
        """업데이트"""
        self.current += 1
        
        # 이벤트 타입별 카운트
        if event_type:
            et = event_type.lower()
            if 'orderbook' in et or 'snapshot' in et:
                self.ob_count += 1
            elif 'trade' in et:
                self.trade_count += 1
            elif 'ticker' in et:
                self.ticker_count += 1
            elif 'liquidation' in et:
                self.liq_count += 1
        
        # 주기적 출력
        current_time = time.time()
        if current_time - self.last_print_time >= self.print_interval:
            self._print(processor)
            self.last_print_time = current_time
    
    def _print(self, processor: Processor = None):
        """Progress 출력"""
        elapsed = time.time() - self.start_time
        rate = self.current / elapsed if elapsed > 0 else 0
        
        # ETA 계산
        if self.total_estimate > 0 and rate > 0:
            remaining = (self.total_estimate - self.current) / rate
            eta_str = f"ETA:{remaining:>5.0f}s"
            pct = min(100, self.current / self.total_estimate * 100)
            bar_len = 25
            filled = int(bar_len * pct / 100)
            bar = "█" * filled + "░" * (bar_len - filled)
            progress_str = f"[{bar}] {pct:>5.1f}%"
        else:
            eta_str = ""
            progress_str = ""
        
        # Decision 정보 (processor에서 가져오기)
        if processor:
            total_dec = sum(processor.decision_counts.values())
            if total_dec > 0:
                allowed_pct = processor.decision_counts['ALLOWED'] / total_dec * 100
                halted_pct = processor.decision_counts['HALTED'] / total_dec * 100
                decision_str = f"A:{allowed_pct:>4.1f}% H:{halted_pct:>4.1f}%"
                trans_str = f"Trans:{processor.state_transitions_count:>4}"
            else:
                decision_str = ""
                trans_str = ""
        else:
            decision_str = ""
            trans_str = ""
        
        # 한 줄로 출력
        line = (f"\r  {self.name:>10}: {self.current:>10,} | "
                f"OB:{self.ob_count:>8,} TK:{self.ticker_count:>6,} LQ:{self.liq_count:>3} | "
                f"{rate:>7,.0f}/s | {elapsed:>5.1f}s {eta_str:>10} | "
                f"{decision_str:>16} {trans_str:>10} {progress_str}")
        
        print(line, end='', flush=True)
    
    def finish(self, processor: Processor = None):
        """완료"""
        elapsed = time.time() - self.start_time
        rate = self.current / elapsed if elapsed > 0 else 0
        
        if processor:
            total_dec = sum(processor.decision_counts.values())
            if total_dec > 0:
                allowed_pct = processor.decision_counts['ALLOWED'] / total_dec * 100
                halted_pct = processor.decision_counts['HALTED'] / total_dec * 100
                decision_str = f"A:{allowed_pct:.1f}% H:{halted_pct:.1f}%"
            else:
                decision_str = ""
        else:
            decision_str = ""
        
        print(f"\r  {self.name:>10}: {self.current:>10,} | ✅ 완료 | "
              f"{rate:,.0f}/s | {elapsed:.1f}s | {decision_str}" + " " * 40)


# =============================================================================
# Utilities
# =============================================================================

def estimate_total_events(data_dir: str) -> int:
    """이벤트 수 추정"""
    data_path = Path(data_dir)
    total_size = 0
    
    for name in ['orderbook', 'trades', 'ticker', 'liquidations']:
        for ext in ['.csv', '.csv.gz']:
            path = data_path / f"{name}{ext}"
            if path.exists():
                total_size += path.stat().st_size
    
    # ~100 bytes per event
    return max(int(total_size / 100), 100000)


def print_header(title: str, data_info: Dict = None):
    """헤더 출력"""
    print("\n" + "=" * 70)
    print(f"🚀 {title}")
    print("=" * 70)
    
    if data_info:
        for k, v in data_info.items():
            print(f"  {k}: {v}")
    
    print(f"\n[Thresholds]")
    print(f"  allowed_lateness_ms:        {THRESHOLDS.allowed_lateness_ms}")
    print(f"  ar1_min_samples:            {THRESHOLDS.ar1_min_samples}")
    print(f"  ar1_fit_quality_valid:      {THRESHOLDS.ar1_fit_quality_valid}")
    print(f"  ar1_fit_quality_invalid:    {THRESHOLDS.ar1_fit_quality_invalid}")
    print(f"  ar1_forecast_error_valid:   {THRESHOLDS.ar1_forecast_error_valid_mult}σ")
    print(f"  ar1_forecast_error_invalid: {THRESHOLDS.ar1_forecast_error_invalid_mult}σ")
    print("=" * 70 + "\n")


def print_summary(result: ProcessingResult, output_dir: str):
    """요약 출력"""
    total = result.total_decisions
    
    print("\n" + "=" * 70)
    print(f"📊 {result.mode.upper()} 결과")
    print("=" * 70)
    
    print("\n[Decision Distribution]")
    for dec, count in result.decision_counts.items():
        pct = count / total * 100 if total > 0 else 0
        bar = "█" * int(pct / 2.5)
        print(f"  {dec:12}: {count:>8,} ({pct:>5.1f}%) {bar}")
    
    print(f"\n[Sanitization]")
    for san, count in result.sanitization_counts.items():
        pct = count / total * 100 if total > 0 else 0
        print(f"  {san:12}: {count:>8,} ({pct:>5.1f}%)")
    
    print(f"\n[Stats]")
    print(f"  Tickers:           {result.stats.get('tickers', 0):>10,}")
    print(f"  Liquidations:      {result.stats.get('liquidations', 0):>10,}")
    print(f"  Out-of-Order:      {result.stats.get('out_of_order', 0):>10,}")
    print(f"  State Transitions: {result.state_transitions_count:>10,}")
    print(f"  Processing Time:   {result.processing_time_sec:>10.1f}s")
    
    print(f"\n[Output Files]")
    print(f"  📁 {output_dir}/")
    print(f"     ├── state_transitions.jsonl")
    print(f"     ├── decisions.jsonl")
    print(f"     ├── liquidations.jsonl")
    print(f"     └── summary.json")
    
    print("=" * 70)


# =============================================================================
# Historical Validation
# =============================================================================

def run_historical_single(data_dir: str, output_dir: str, name: str) -> ProcessingResult:
    """단일 Historical 실행"""
    total_estimate = estimate_total_events(data_dir)
    
    print_header(f"{name} Validation", {
        'Data': data_dir,
        'Output': output_dir,
        'Est. Events': f"~{total_estimate:,}"
    })
    
    # 프로세서 (output_dir 전달하여 실시간 로깅)
    processor = Processor(mode=name.lower(), output_dir=output_dir)
    
    # 데이터 소스
    source = create_data_source('historical', data_dir=data_dir)
    
    # Progress
    progress = ProgressDisplay(name=name[:10], total_estimate=total_estimate)
    
    try:
        for event in source.get_events():
            processor.process_event(event)
            event_type = event.event_type.value if hasattr(event.event_type, 'value') else str(event.event_type)
            progress.update(event_type, processor)
        
        progress.finish(processor)
        
    finally:
        # Summary 저장 및 파일 닫기
        processor.save_summary()
        processor.close()
    
    result = processor.get_result()
    print_summary(result, output_dir)
    
    return result


def run_historical_comparison(research_dir: str, validation_dir: str, output_base: str):
    """Research vs Validation 비교"""
    print("\n" + "=" * 70)
    print("📊 Historical Validation: Research vs Validation")
    print("=" * 70)
    print(f"  Research:   {research_dir}")
    print(f"  Validation: {validation_dir}")
    print(f"  Output:     {output_base}")
    print("=" * 70)
    
    # 1. Research
    research_result = run_historical_single(
        research_dir, f"{output_base}/research", "Research"
    )
    
    # 2. Validation
    validation_result = run_historical_single(
        validation_dir, f"{output_base}/validation", "Validation"
    )
    
    # 3. 비교
    print_comparison(research_result, validation_result)
    
    return research_result, validation_result


def print_comparison(r: ProcessingResult, v: ProcessingResult):
    """비교 테이블"""
    r_total = r.total_decisions
    v_total = v.total_decisions
    
    print("\n" + "=" * 70)
    print("📈 COMPARISON: Research vs Validation")
    print("=" * 70)
    
    print("\n┌───────────────────────────────────────────────────────────────────┐")
    print("│                    DECISION DISTRIBUTION                          │")
    print("├────────────┬───────────────────┬───────────────────┬──────────────┤")
    print("│            │     Research      │    Validation     │     Delta    │")
    print("├────────────┼───────────────────┼───────────────────┼──────────────┤")
    
    for dec in ['ALLOWED', 'RESTRICTED', 'HALTED']:
        r_cnt = r.decision_counts.get(dec, 0)
        v_cnt = v.decision_counts.get(dec, 0)
        r_pct = r_cnt / r_total * 100 if r_total > 0 else 0
        v_pct = v_cnt / v_total * 100 if v_total > 0 else 0
        delta = v_pct - r_pct
        
        print(f"│ {dec:10} │ {r_cnt:>7,} ({r_pct:>5.1f}%) │ {v_cnt:>7,} ({v_pct:>5.1f}%) │ {delta:>+9.1f}%  │")
    
    print("└────────────┴───────────────────┴───────────────────┴──────────────┘")
    
    print("\n┌───────────────────────────────────────────────────────────────────┐")
    print("│                         STATISTICS                                │")
    print("├──────────────────┬──────────────┬──────────────┬─────────────────┤")
    print("│                  │   Research   │  Validation  │      Ratio      │")
    print("├──────────────────┼──────────────┼──────────────┼─────────────────┤")
    
    stats_compare = [
        ('Total Decisions', r_total, v_total),
        ('Liquidations', r.stats.get('liquidations', 0), v.stats.get('liquidations', 0)),
        ('Out-of-Order', r.stats.get('out_of_order', 0), v.stats.get('out_of_order', 0)),
        ('State Transitions', r.state_transitions_count, v.state_transitions_count),
        ('Processing (sec)', r.processing_time_sec, v.processing_time_sec),
    ]
    
    for label, r_val, v_val in stats_compare:
        ratio = v_val / r_val if r_val > 0 else 0
        if isinstance(r_val, float):
            print(f"│ {label:16} │ {r_val:>12.1f} │ {v_val:>12.1f} │ {ratio:>13.2f}x │")
        else:
            print(f"│ {label:16} │ {r_val:>12,} │ {v_val:>12,} │ {ratio:>13.2f}x │")
    
    print("└──────────────────┴──────────────┴──────────────┴─────────────────┘")
    
    # 해석
    r_allowed = r.decision_counts.get('ALLOWED', 0) / r_total * 100 if r_total > 0 else 0
    v_allowed = v.decision_counts.get('ALLOWED', 0) / v_total * 100 if v_total > 0 else 0
    delta_allowed = v_allowed - r_allowed
    
    print("\n[📋 해석]")
    if abs(delta_allowed) < 5:
        print(f"  ✅ ALLOWED 비율 일관됨 (Δ={delta_allowed:+.1f}%)")
    elif delta_allowed < -10:
        print(f"  ⚠️ Validation에서 ALLOWED 감소 (Δ={delta_allowed:+.1f}%) - Dirty data 영향")
    else:
        print(f"  ⚠️ Validation에서 ALLOWED 증가 (Δ={delta_allowed:+.1f}%)")
    
    v_ooo = v.stats.get('out_of_order', 0)
    r_ooo = r.stats.get('out_of_order', 0)
    if v_ooo > r_ooo * 2 and v_ooo > 100:
        print(f"  ⚠️ Out-of-Order 증가 (R:{r_ooo:,} → V:{v_ooo:,})")
    
    print("")


# =============================================================================
# Realtime Validation
# =============================================================================

async def run_realtime(symbol: str, duration_sec: int, output_dir: str, source_ref: list) -> ProcessingResult:
    """
    Realtime 실행
    
    duration_sec=0: 무한 실행 (Ctrl+C로 종료)
    """
    duration_str = "무한 (Ctrl+C로 종료)" if duration_sec == 0 else f"{duration_sec}초"
    
    print_header("Realtime Validation", {
        'Symbol': symbol.upper(),
        'Duration': duration_str,
        'Output': output_dir,
    })
    
    print("  💡 Ctrl+C를 누르면 안전하게 종료됩니다.\n")
    
    # 프로세서
    processor = Processor(mode='realtime', output_dir=output_dir)
    
    # 데이터 소스
    source = create_data_source('realtime', symbol=symbol, duration_sec=duration_sec)
    source_ref.append(source)  # 외부에서 stop() 호출 가능하도록
    
    # Progress
    progress = ProgressDisplay(name="Realtime")
    
    try:
        async for event in source.get_events_async():
            processor.process_event(event)
            event_type = event.event_type.value if hasattr(event.event_type, 'value') else str(event.event_type)
            progress.update(event_type, processor)
    
    except asyncio.CancelledError:
        print("\n\n  🛑 종료 신호 수신...")
    
    finally:
        progress.finish(processor)
        processor.save_summary()
        processor.close()
    
    result = processor.get_result()
    print_summary(result, output_dir)
    
    # 데이터 소스 통계
    print(f"\n[Data Source Stats]")
    print(f"  Total Events:    {source.stats.events_total:,}")
    print(f"  Duplicates:      {source.stats.duplicates_filtered:,}")
    print(f"  Out-of-Order:    {source.stats.out_of_order:,}")
    print(f"  Reconnects:      {source.stats.reconnects:,}")
    
    return result


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Decision Engine',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Research vs Validation 비교
  python run.py --mode historical --research ./data/research --validation ./data/validation
  
  # 단일 데이터셋
  python run.py --mode historical --data ./data/research --name Research
  
  # Realtime (무한 실행, Ctrl+C로 종료)
  python run.py --mode realtime
  
  # Realtime (시간 제한)
  python run.py --mode realtime --duration 60
  
  # 설정 확인
  python run.py --show-config
        """
    )
    
    parser.add_argument('--mode', choices=['historical', 'realtime'])
    parser.add_argument('--research', type=str, help='Research data dir')
    parser.add_argument('--validation', type=str, help='Validation data dir')
    parser.add_argument('--data', type=str, help='Single data dir')
    parser.add_argument('--name', type=str, default='Dataset')
    parser.add_argument('--symbol', type=str, default='btcusdt')
    parser.add_argument('--duration', type=int, default=0, help='Duration in seconds (0=infinite)')
    parser.add_argument('--output', type=str, default='./output')
    parser.add_argument('--show-config', action='store_true')
    
    args = parser.parse_args()
    
    if args.show_config:
        print_thresholds()
        return
    
    if not args.mode:
        parser.print_help()
        return
    
    if args.mode == 'historical':
        if args.research and args.validation:
            run_historical_comparison(args.research, args.validation, args.output)
        elif args.data:
            run_historical_single(args.data, f"{args.output}/{args.name.lower()}", args.name)
        else:
            print("Error: --research & --validation 또는 --data 필요")
    
    elif args.mode == 'realtime':
        source_ref = []  # DataSource 참조 저장용
        
        async def run_with_signal_handler():
            """Signal handler와 함께 실행"""
            import signal
            
            loop = asyncio.get_event_loop()
            task = asyncio.create_task(
                run_realtime(args.symbol, args.duration, f"{args.output}/realtime", source_ref)
            )
            
            def signal_handler():
                print("\n\n  ⚠️ Ctrl+C 감지! 안전하게 종료 중...")
                if source_ref:
                    source_ref[0].stop()
                task.cancel()
            
            # Unix signal handler
            try:
                loop.add_signal_handler(signal.SIGINT, signal_handler)
                loop.add_signal_handler(signal.SIGTERM, signal_handler)
            except NotImplementedError:
                # Windows에서는 signal handler가 제한적
                pass
            
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        try:
            asyncio.run(run_with_signal_handler())
        except KeyboardInterrupt:
            print("\n\n  ✅ 종료 완료")


if __name__ == "__main__":
    main()