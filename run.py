#!/usr/bin/env python3
"""
Decision Engine - 통합 실행 파일 (v4 - BufferedProcessor)

================================================================================
v4 변경사항:
- BufferedProcessor 사용 (Time Alignment Policy 적용)
- EventBuffer를 통한 out-of-order 이벤트 정렬
- watermark 기반 처리
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
from src.buffered_processor import BufferedProcessor, ProcessingResult


# =============================================================================
# Progress Display
# =============================================================================

class ProgressDisplay:
    """Progress 표시 (단순화 버전 - 처리된 이벤트 수만 표시)"""
    
    def __init__(self, name: str = "Processing", log_interval: int = 100000):
        self.name = name
        self.log_interval = log_interval
        self.current = 0
        self.start_time = time.time()
        self.last_print_time = 0
        self.print_interval = 2.0  # 2초마다 업데이트
        
        # 이벤트 카운트
        self.ob_count = 0
        self.trade_count = 0
        self.ticker_count = 0
        self.liq_count = 0
    
    def update(self, event_type: str = None, processor: BufferedProcessor = None):
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
        
        # 주기적 출력 (시간 또는 이벤트 수 기준)
        current_time = time.time()
        if (current_time - self.last_print_time >= self.print_interval or 
            self.current % self.log_interval == 0):
            self._print(processor)
            self.last_print_time = current_time
    
    def _print(self, processor: BufferedProcessor = None):
        """Progress 출력"""
        elapsed = time.time() - self.start_time
        rate = self.current / elapsed if elapsed > 0 else 0
        
        # Decision 정보
        if processor:
            total_dec = sum(processor.decision_counts.values())
            if total_dec > 0:
                allowed_pct = processor.decision_counts['ALLOWED'] / total_dec * 100
                halted_pct = processor.decision_counts['HALTED'] / total_dec * 100
                decision_str = f"A:{allowed_pct:>4.1f}% H:{halted_pct:>4.1f}%"
                trans_str = f"Trans:{processor.state_transitions_count}"
            else:
                decision_str = ""
                trans_str = ""
        else:
            decision_str = ""
            trans_str = ""
        
        # 한 줄로 출력 (% 없이, TR 포함)
        line = (f"\r  {self.name:>10}: {self.current:>10,} events | "
                f"OB:{self.ob_count:>7,} TK:{self.ticker_count:>6,} TR:{self.trade_count:>8,} LQ:{self.liq_count:>4} | "
                f"{rate:>6,.0f}/s | {elapsed:>5.1f}s | {decision_str} {trans_str}")
        
        print(line, end='', flush=True)
    
    def finish(self, processor: BufferedProcessor = None):
        """완료"""
        elapsed = time.time() - self.start_time
        rate = self.current / elapsed if elapsed > 0 else 0
        
        if processor:
            total_dec = sum(processor.decision_counts.values())
            if total_dec > 0:
                allowed_pct = processor.decision_counts['ALLOWED'] / total_dec * 100
                halted_pct = processor.decision_counts['HALTED'] / total_dec * 100
                decision_str = f"A:{allowed_pct:.1f}% H:{halted_pct:.1f}%"
                trans_str = f"Trans:{processor.state_transitions_count}"
            else:
                decision_str = ""
                trans_str = ""
        else:
            decision_str = ""
            trans_str = ""
        
        # 완료 메시지
        line = (f"\r  {self.name:>10}: {self.current:>10,} events | "
                f"OB:{self.ob_count:>7,} TK:{self.ticker_count:>6,} TR:{self.trade_count:>8,} LQ:{self.liq_count:>4} | "
                f"{rate:>6,.0f}/s | {elapsed:>5.1f}s | {decision_str} {trans_str} ✅ DONE")
        print(line + " " * 20)


# =============================================================================
# Utilities
# =============================================================================


def print_header(title: str, data_info: Dict = None):
    """헤더 출력"""
    print("\n" + "=" * 70)
    print(f"🚀 {title}")
    print("=" * 70)
    
    if data_info:
        for k, v in data_info.items():
            print(f"  {k}: {v}")
    
    print(f"\n[Time Alignment Policy]")
    print(f"  buffer_max_size:       {THRESHOLDS.buffer_max_size}")
    print(f"  window_size_ms:        {THRESHOLDS.window_size_ms}")
    print(f"  allowed_lateness_ms:   {THRESHOLDS.allowed_lateness_ms}")
    
    print(f"\n[Volatility Stability]")
    print(f"  volatility_window_size:        {THRESHOLDS.volatility_window_size}")
    print(f"  volatility_min_samples:        {THRESHOLDS.volatility_min_samples}")
    print(f"  volatility_valid_threshold:    {THRESHOLDS.volatility_valid_threshold} bps (p90)")
    print(f"  volatility_weakening_threshold:{THRESHOLDS.volatility_weakening_threshold} bps (p95)")
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
    
    print(f"\n[Buffer Stats]")
    buf = result.buffer_stats
    print(f"  Total Received:    {buf.get('total_received', 0):>10,}")
    print(f"  Total Emitted:     {buf.get('total_emitted', 0):>10,}")
    print(f"  Dropped (Late):    {buf.get('dropped_late', 0):>10,}")
    print(f"  Out-of-Order:      {buf.get('out_of_order_received', 0):>10,}")
    print(f"  Max Buffer Size:   {buf.get('max_buffer_size', 0):>10,}")
    
    print(f"\n[Stats]")
    print(f"  Tickers:           {result.stats.get('tickers', 0):>10,}")
    print(f"  Liquidations:      {result.stats.get('liquidations', 0):>10,}")
    print(f"  State Transitions: {result.state_transitions_count:>10,}")
    print(f"  Processing Time:   {result.processing_time_sec:>10.1f}s")
    
    print(f"\n[Output Files]")
    print(f"  📁 {output_dir}/")
    print(f"     ├── state_transitions.jsonl")
    print(f"     ├── decisions.jsonl")
    print(f"     └── summary.json")
    
    print("=" * 70)


# =============================================================================
# Historical Validation
# =============================================================================

def run_historical_single(data_dir: str, output_dir: str, name: str) -> ProcessingResult:
    """단일 Historical 실행 (BufferedProcessor 사용)"""
    
    print_header(f"{name} Validation", {
        'Data': data_dir,
        'Output': output_dir,
    })
    
    # BufferedProcessor (output_dir 전달하여 실시간 로깅)
    processor = BufferedProcessor(mode=name.lower(), output_dir=output_dir)
    
    # 데이터 소스
    source = create_data_source('historical', data_dir=data_dir)
    
    # Progress (단순화 - % 없이)
    progress = ProgressDisplay(name=name[:10], log_interval=100000)
    
    try:
        for event in source.get_events():
            # BufferedProcessor는 여러 결과를 반환할 수 있음
            results = processor.process_event(event)
            event_type = event.event_type.value if hasattr(event.event_type, 'value') else str(event.event_type)
            progress.update(event_type, processor)
        
        # 버퍼에 남은 이벤트 처리 (중요!)
        remaining_results = processor.flush()
        
        progress.finish(processor)
        
    finally:
        # Summary 저장 및 파일 닫기
        processor.save_summary()
        processor.close()
    
    result = processor.get_result()
    print_summary(result, output_dir)
    
    return result


def run_historical_comparison(research_dir: str, validation_dir: str, output_base: str):
    """Research vs Validation 비교 + Historical 통합 출력"""
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
    
    # 3. Historical 통합 (Research + Validation 순차 처리)
    print("\n" + "=" * 70)
    print("📦 Historical 통합 출력 생성 중...")
    print("=" * 70)
    
    create_historical_combined(
        research_dir, validation_dir, 
        f"{output_base}/historical",
        research_result, validation_result
    )
    
    # 4. 비교
    print_comparison(research_result, validation_result)
    
    return research_result, validation_result


def create_historical_combined(research_dir: str, validation_dir: str, 
                                output_dir: str,
                                research_result: ProcessingResult,
                                validation_result: ProcessingResult):
    """Historical 통합 출력 생성 (Research + Validation 결과 병합)"""
    import json
    from pathlib import Path
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    research_path = Path(output_dir).parent / "research"
    validation_path = Path(output_dir).parent / "validation"
    
    # 1. state_transitions.jsonl 병합
    with open(output_path / "state_transitions.jsonl", 'w') as out_f:
        # Research
        research_trans = research_path / "state_transitions.jsonl"
        if research_trans.exists():
            with open(research_trans) as f:
                for line in f:
                    data = json.loads(line)
                    data['source'] = 'research'
                    out_f.write(json.dumps(data) + '\n')
        
        # Validation
        validation_trans = validation_path / "state_transitions.jsonl"
        if validation_trans.exists():
            with open(validation_trans) as f:
                for line in f:
                    data = json.loads(line)
                    data['source'] = 'validation'
                    out_f.write(json.dumps(data) + '\n')
    
    # 2. decisions.jsonl 병합
    with open(output_path / "decisions.jsonl", 'w') as out_f:
        research_dec = research_path / "decisions.jsonl"
        if research_dec.exists():
            with open(research_dec) as f:
                for line in f:
                    data = json.loads(line)
                    data['source'] = 'research'
                    out_f.write(json.dumps(data) + '\n')
        
        validation_dec = validation_path / "decisions.jsonl"
        if validation_dec.exists():
            with open(validation_dec) as f:
                for line in f:
                    data = json.loads(line)
                    data['source'] = 'validation'
                    out_f.write(json.dumps(data) + '\n')
    
    # 3. summary.json 통합
    r = research_result
    v = validation_result
    
    r_total = r.total_decisions
    v_total = v.total_decisions
    combined_total = r_total + v_total
    
    combined_summary = {
        'mode': 'historical',
        'datasets': ['research', 'validation'],
        'combined': {
            'total_decisions': combined_total,
            'decision_counts': {
                k: r.decision_counts.get(k, 0) + v.decision_counts.get(k, 0)
                for k in ['ALLOWED', 'RESTRICTED', 'HALTED']
            },
            'decision_rates': {
                k: round((r.decision_counts.get(k, 0) + v.decision_counts.get(k, 0)) / combined_total * 100, 2)
                if combined_total > 0 else 0
                for k in ['ALLOWED', 'RESTRICTED', 'HALTED']
            },
            'state_transitions': r.state_transitions_count + v.state_transitions_count,
            'processing_time_sec': round(r.processing_time_sec + v.processing_time_sec, 2),
        },
        'research': {
            'total_decisions': r_total,
            'decision_counts': r.decision_counts,
            'decision_rates': {
                k: round(cnt / r_total * 100, 2) if r_total > 0 else 0
                for k, cnt in r.decision_counts.items()
            },
            'stats': r.stats,
            'state_transitions': r.state_transitions_count,
        },
        'validation': {
            'total_decisions': v_total,
            'decision_counts': v.decision_counts,
            'decision_rates': {
                k: round(cnt / v_total * 100, 2) if v_total > 0 else 0
                for k, cnt in v.decision_counts.items()
            },
            'stats': v.stats,
            'state_transitions': v.state_transitions_count,
        },
        'comparison': {
            'allowed_delta': round(
                (v.decision_counts.get('ALLOWED', 0) / v_total * 100 if v_total > 0 else 0) -
                (r.decision_counts.get('ALLOWED', 0) / r_total * 100 if r_total > 0 else 0), 1
            ),
            'halted_delta': round(
                (v.decision_counts.get('HALTED', 0) / v_total * 100 if v_total > 0 else 0) -
                (r.decision_counts.get('HALTED', 0) / r_total * 100 if r_total > 0 else 0), 1
            ),
            'out_of_order_ratio': round(
                v.stats.get('out_of_order', 0) / r.stats.get('out_of_order', 1), 2
            ),
            'liquidation_ratio': round(
                v.stats.get('liquidations', 0) / max(r.stats.get('liquidations', 1), 1), 2
            ),
        },
        'thresholds': get_thresholds_dict(),
    }
    
    with open(output_path / "summary.json", 'w') as f:
        json.dump(combined_summary, f, indent=2)
    
    print(f"  ✅ {output_dir}/state_transitions.jsonl")
    print(f"  ✅ {output_dir}/decisions.jsonl")
    print(f"  ✅ {output_dir}/summary.json")


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
        ('Out-of-Order', r.buffer_stats.get('out_of_order_received', 0), v.buffer_stats.get('out_of_order_received', 0)),
        ('Dropped (Late)', r.buffer_stats.get('dropped_late', 0), v.buffer_stats.get('dropped_late', 0)),
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
    
    v_ooo = v.buffer_stats.get('out_of_order_received', 0)
    r_ooo = r.buffer_stats.get('out_of_order_received', 0)
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
    
    # BufferedProcessor
    processor = BufferedProcessor(mode='realtime', output_dir=output_dir)
    
    # 데이터 소스
    source = create_data_source('realtime', symbol=symbol, duration_sec=duration_sec)
    source_ref.append(source)  # 외부에서 stop() 호출 가능하도록
    
    # Progress
    progress = ProgressDisplay(name="Realtime")
    
    try:
        async for event in source.get_events_async():
            results = processor.process_event(event)
            event_type = event.event_type.value if hasattr(event.event_type, 'value') else str(event.event_type)
            progress.update(event_type, processor)
    
    except asyncio.CancelledError:
        print("\n\n  🛑 종료 신호 수신...")
    
    finally:
        # 버퍼에 남은 이벤트 처리
        remaining_results = processor.flush()
        
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
  
  # Docker 실행
  docker run -v /path/to/data:/data your-image historical
  docker run your-image realtime
        """
    )
    
    # Positional argument for Docker compatibility
    parser.add_argument('docker_mode', nargs='?', choices=['historical', 'realtime'],
                        help='Mode for Docker (historical/realtime)')
    
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
    
    # Docker mode support (positional argument)
    if args.docker_mode:
        args.mode = args.docker_mode
        if args.mode == 'historical' and not args.data and not args.research:
            # Docker 기본 경로
            args.data = '/data'
            args.name = 'Historical'
            args.output = '/output'
    
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