"""
Effective Orderbook Analysis 실행 스크립트 (v2 - Memory Efficient)

대용량 데이터셋 처리를 위한 메모리 최적화 버전

사용법:
    # 단일 처리 (권장)
    python run_analysis.py --single ./data/research --name Research
    python run_analysis.py --single ./data/validation --name Validation
    
    # 순차 비교 (메모리 절약)
    python run_analysis.py --research ./data/research --validation ./data/validation --sequential
    
    # 병렬 비교 (메모리 많을 때만)
    python run_analysis.py --research ./data/research --validation ./data/validation
"""
import sys
import gc
import argparse
import json
from pathlib import Path
from typing import Optional, Dict

sys.path.insert(0, str(Path(__file__).parent))

from src.stream_processor import EffectiveOrderbookProcessor
from src.memory_efficient_streamer import MemoryEfficientStreamer, ChunkConfig, create_streamer
from src.results import ProcessingResult, compare_results


def process_dataset_memory_efficient(data_dir: str, 
                                      dataset_name: str,
                                      output_dir: Optional[str] = None) -> Dict:
    """
    메모리 효율적인 단일 데이터셋 처리
    
    처리 후 결과만 반환하고 모든 리소스 해제
    """
    print(f"\n{'='*70}")
    print(f"🚀 Processing: {dataset_name}")
    print(f"📂 Directory: {data_dir}")
    print(f"{'='*70}")
    
    # 1. 메모리 효율적 스트리머 생성
    chunk_config = ChunkConfig(
        orderbook_chunk_size=2_000_000,   # 200만 rows씩
        trades_chunk_size=500_000,         # 50만 rows씩
        ticker_chunk_size=20_000,          # 2만 rows씩
        liquidation_chunk_size=5_000,      # 5천 rows씩
    )
    
    streamer = create_streamer(data_dir, chunk_config=chunk_config)
    
    # 2. Processor 생성
    processor = EffectiveOrderbookProcessor(
        dataset_name=dataset_name,
        stale_threshold_ms=50.0,
        liquidation_cooldown_ms=5000.0
    )
    
    # 3. 이벤트 처리
    event_count = 0
    last_progress_report = 0
    report_interval = 500_000  # 50만 이벤트마다 보고
    
    try:
        while streamer.has_more_events():
            event = streamer.get_next_event()
            if event:
                processor.add_event(event)
                event_count += 1
                
                # 진행 상황 보고
                if event_count - last_progress_report >= report_interval:
                    progress = streamer.get_progress()
                    ob_size = 0
                    if processor.current_orderbook:
                        ob_size = len(processor.current_orderbook.bid_levels) + len(processor.current_orderbook.ask_levels)
                    
                    print(f"  📊 Processed {event_count:,} events | OB: {progress.get('orderbook_events', 0):,} | "
                          f"TR: {progress.get('trade_events', 0):,} | TK: {progress.get('ticker_events', 0):,} | "
                          f"OB size: {ob_size:,}")
                    last_progress_report = event_count
        
        # 남은 버퍼 처리
        processor.process_buffer()
        
    except KeyboardInterrupt:
        print("\n⚠️ 처리 중단됨")
    
    # 4. 결과 추출 (메모리 해제 전)
    result = processor.get_result()
    result.print_summary()
    
    # 5. 결과 저장
    if output_dir:
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        
        print(f"\n📁 결과 저장 중...")
        result.save_outputs(str(out_path))
        result.to_json(str(out_path / f"{dataset_name.lower()}_full.json"))
    
    # 결과 요약만 추출 (메모리 절약)
    summary = {
        'dataset_name': result.dataset_name,
        'processing_time_sec': result.processing_time_sec,
        'stats': result.stats,
        'decision_counts': result.decision_counts,
        'tradability_counts': result.tradability_counts,
        'trade_validity_rate': result.trade_validity_rate,
        'allowed_rate': result.allowed_rate,
        'halted_rate': result.halted_rate,
        'state_transitions_count': len(result.state_transitions),
        'decisions_count': len(result.decisions_log),
    }
    
    # 6. 메모리 해제
    print("\n🧹 메모리 정리 중...")
    streamer.close()
    del streamer
    del processor
    del result
    gc.collect()
    
    print(f"✅ {dataset_name} 처리 완료\n")
    
    return summary


def run_sequential_comparison(research_dir: str, 
                               validation_dir: str,
                               output_base: str = "./output"):
    """
    순차적 비교 (메모리 절약)
    
    Research 처리 → 결과 저장 → 메모리 해제 → Validation 처리 → 비교
    """
    print("\n" + "="*70)
    print("📊 Sequential Comparison Mode (메모리 절약)")
    print("="*70)
    
    # Research 처리
    research_output = f"{output_base}/historical"
    research_summary = process_dataset_memory_efficient(
        research_dir, "Research", research_output
    )
    
    # Validation 처리  
    validation_output = f"{output_base}/validation"
    validation_summary = process_dataset_memory_efficient(
        validation_dir, "Validation", validation_output
    )
    
    # 비교 출력
    print_comparison(research_summary, validation_summary)
    
    # 비교 결과 저장
    comparison = {
        'research': research_summary,
        'validation': validation_summary,
        'comparison': calculate_comparison(research_summary, validation_summary)
    }
    
    comparison_file = f"{output_base}/comparison.json"
    with open(comparison_file, 'w') as f:
        json.dump(comparison, f, indent=2)
    print(f"\n📁 비교 결과 저장: {comparison_file}")


def print_comparison(research: Dict, validation: Dict):
    """비교 결과 출력"""
    print(f"\n{'='*75}")
    print(f"📊 Research vs Validation 비교")
    print(f"{'='*75}")
    
    print(f"\n{'지표':<40} {'Research':>12} {'Validation':>12} {'차이':>10}")
    print(f"{'-'*75}")
    
    # Trade Validity
    r_tv = research.get('trade_validity_rate', 0)
    v_tv = validation.get('trade_validity_rate', 0)
    print(f"{'Trade Validity Rate':<40} {r_tv:>11.1%} {v_tv:>11.1%} {v_tv - r_tv:>+9.1%}")
    
    print(f"{'-'*75}")
    
    # Decision Distribution
    r_allowed = research.get('allowed_rate', 0)
    v_allowed = validation.get('allowed_rate', 0)
    r_halted = research.get('halted_rate', 0)
    v_halted = validation.get('halted_rate', 0)
    
    print(f"{'ALLOWED %':<40} {r_allowed:>11.1%} {v_allowed:>11.1%} {v_allowed - r_allowed:>+9.1%}")
    print(f"{'HALTED %':<40} {r_halted:>11.1%} {v_halted:>11.1%} {v_halted - r_halted:>+9.1%}")
    
    print(f"{'-'*75}")
    
    # State Transitions
    r_trans = research.get('state_transitions_count', 0)
    v_trans = validation.get('state_transitions_count', 0)
    print(f"{'State Transitions':<40} {r_trans:>11,} {v_trans:>11,}")
    
    print(f"\n{'='*75}")
    
    # 인사이트
    print(f"\n[핵심 인사이트]")
    
    if v_tv - r_tv < -0.05:
        print(f"  ⚠️ Validation에서 Trade Validity가 {-(v_tv - r_tv):.1%}p 낮음 → Dirty Data 영향")
    
    if v_allowed - r_allowed < -0.05:
        print(f"  ⚠️ Validation에서 ALLOWED가 {-(v_allowed - r_allowed):.1%}p 낮음 → Uncertainty 증가")
    
    if v_halted - r_halted > 0.05:
        print(f"  ⚠️ Validation에서 HALTED가 {v_halted - r_halted:.1%}p 높음 → 판단 중단 구간 증가")
    
    if v_allowed >= 0.8:
        print(f"  ✅ Validation에서도 80% 이상 ALLOWED 유지")


def calculate_comparison(research: Dict, validation: Dict) -> Dict:
    """비교 지표 계산"""
    return {
        'trade_validity_diff': validation.get('trade_validity_rate', 0) - research.get('trade_validity_rate', 0),
        'allowed_diff': validation.get('allowed_rate', 0) - research.get('allowed_rate', 0),
        'halted_diff': validation.get('halted_rate', 0) - research.get('halted_rate', 0),
        'transitions_diff': validation.get('state_transitions_count', 0) - research.get('state_transitions_count', 0),
    }


def run_single(data_dir: str, dataset_name: str = "Dataset", output_dir: str = "./output"):
    """단일 데이터셋 처리"""
    out_path = f"{output_dir}/{dataset_name.lower()}"
    process_dataset_memory_efficient(data_dir, dataset_name, out_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Effective Orderbook Analysis (Memory Efficient)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  # 단일 처리
  python run_analysis.py --single ./data/research --name Research
  python run_analysis.py --single ./data/validation --name Validation
  
  # 순차 비교 (권장 - 메모리 절약)
  python run_analysis.py --research ./data/research --validation ./data/validation
  
  # 출력 디렉토리 지정
  python run_analysis.py --single ./data/research --name Research --output ./results
        """
    )
    parser.add_argument("--research", type=str, help="Research 데이터 디렉토리")
    parser.add_argument("--validation", type=str, help="Validation 데이터 디렉토리")
    parser.add_argument("--single", type=str, help="단일 데이터셋 처리")
    parser.add_argument("--name", type=str, default="Dataset", help="데이터셋 이름")
    parser.add_argument("--output", type=str, default="./output", help="출력 디렉토리")
    
    args = parser.parse_args()
    
    if args.single:
        run_single(args.single, args.name, args.output)
    elif args.research and args.validation:
        run_sequential_comparison(args.research, args.validation, args.output)
    else:
        parser.print_help()