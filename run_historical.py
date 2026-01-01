#!/usr/bin/env python3
"""
Phase 1: Historical Validation

================================================================================
Single Decision Engine:
- config.py의 THRESHOLDS 사용
- Realtime과 동일한 파라미터 적용
================================================================================

사용법:
    python run_historical.py --research ./data/research --validation ./data/validation
    python run_historical.py --single ./data/research --name Research
    python run_historical.py --show-config
"""
import sys
import gc
import json
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.config import THRESHOLDS, HISTORICAL_CONFIG, print_thresholds, get_thresholds_dict
from src.stream_processor import StreamProcessor
from src.data_loader import DataLoader


def process_dataset(data_dir: str, dataset_name: str, output_dir: str) -> dict:
    """데이터셋 처리"""
    print(f"\n{'='*60}")
    print(f"🚀 Processing: {dataset_name}")
    print(f"📂 Directory: {data_dir}")
    print(f"{'='*60}")
    
    # 데이터 로더 생성
    loader = DataLoader(data_dir)
    
    # 프로세서 생성 (config.py 참조)
    processor = StreamProcessor(dataset_name=dataset_name)
    
    # 이벤트 처리
    event_count = 0
    for event in loader.iterate_events():
        processor.process_event(event)
        event_count += 1
        
        if event_count % HISTORICAL_CONFIG.log_interval == 0:
            print(f"  Processed {event_count:,} events...")
    
    # 결과
    result = processor.get_result()
    processor.print_summary()
    
    # 저장
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    
    # state_transitions.jsonl
    with open(out_path / "state_transitions.jsonl", 'w') as f:
        for t in result.state_transitions:
            f.write(json.dumps(t) + '\n')
    
    # decisions.jsonl
    with open(out_path / "decisions.jsonl", 'w') as f:
        for d in result.decisions_log:
            f.write(json.dumps(d) + '\n')
    
    # summary.json
    summary = {
        'phase': 'historical',
        'dataset': dataset_name,
        'thresholds_used': get_thresholds_dict(),
        'stats': result.stats,
        'decision_distribution': {
            'counts': result.decision_counts,
            'rates': {
                'ALLOWED': result.allowed_rate,
                'HALTED': result.halted_rate,
            }
        },
        'state_transitions_count': len(result.state_transitions),
    }
    with open(out_path / "summary.json", 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n📁 결과 저장: {out_path}")
    
    # 메모리 해제
    del loader
    del processor
    gc.collect()
    
    return summary


def print_comparison(research: dict, validation: dict):
    """비교 결과 출력"""
    print(f"\n{'='*70}")
    print(f"📊 Research vs Validation 비교")
    print(f"{'='*70}")
    
    r_allowed = research['decision_distribution']['rates']['ALLOWED']
    v_allowed = validation['decision_distribution']['rates']['ALLOWED']
    r_halted = research['decision_distribution']['rates']['HALTED']
    v_halted = validation['decision_distribution']['rates']['HALTED']
    
    print(f"\n{'지표':<30} {'Research':>12} {'Validation':>12} {'차이':>10}")
    print(f"{'-'*70}")
    print(f"{'ALLOWED %':<30} {r_allowed:>11.1%} {v_allowed:>11.1%} {v_allowed - r_allowed:>+9.1%}")
    print(f"{'HALTED %':<30} {r_halted:>11.1%} {v_halted:>11.1%} {v_halted - r_halted:>+9.1%}")
    print(f"{'='*70}")


def main():
    parser = argparse.ArgumentParser(description='Phase 1: Historical Validation')
    parser.add_argument('--research', help='Research data directory')
    parser.add_argument('--validation', help='Validation data directory')
    parser.add_argument('--single', help='Single dataset directory')
    parser.add_argument('--name', default='Dataset', help='Dataset name')
    parser.add_argument('--output', default='./output', help='Output directory')
    parser.add_argument('--show-config', action='store_true', help='Show current thresholds')
    
    args = parser.parse_args()
    
    if args.show_config:
        print_thresholds()
        return
    
    print("=" * 60)
    print("🚀 Phase 1: Historical Validation")
    print("=" * 60)
    print(f"\n[Thresholds from config.py]")
    print(f"  liquidation_cooldown_ms: {THRESHOLDS.liquidation_cooldown_ms}")
    print(f"  integrity_repair_bps:    {THRESHOLDS.integrity_repair_threshold_bps}")
    
    results = {}
    
    if args.single:
        process_dataset(args.single, args.name, f"{args.output}/{args.name.lower()}")
    elif args.research and args.validation:
        results['research'] = process_dataset(args.research, 'Research', f"{args.output}/research")
        results['validation'] = process_dataset(args.validation, 'Validation', f"{args.output}/validation")
        print_comparison(results['research'], results['validation'])
    elif args.research:
        process_dataset(args.research, 'Research', f"{args.output}/research")
    elif args.validation:
        process_dataset(args.validation, 'Validation', f"{args.output}/validation")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()