"""
Research vs Validation 비교 분석 실행 스크립트

사용법:
    python run_analysis.py --research /path/to/research --validation /path/to/validation
    
또는 Python에서:
    from run_analysis import run_comparison
    results = run_comparison(research_dir, validation_dir)
"""
import sys
import argparse
from pathlib import Path

# src 모듈 경로 추가
sys.path.insert(0, str(Path(__file__).parent))

from src.data_loader import DataLoader
from src.stream_streamer import DataFrameStreamer
from src.stream_processor import StreamProcessor
from src.results import ProcessingResult, compare_results


def process_dataset(data_dir: str, dataset_name: str) -> ProcessingResult:
    """
    단일 데이터셋 처리
    
    Args:
        data_dir: 데이터 디렉토리 경로
        dataset_name: 데이터셋 이름 (결과 식별용)
    
    Returns:
        ProcessingResult
    """
    print(f"\n{'='*60}")
    print(f"🚀 Processing: {dataset_name}")
    print(f"📂 Directory: {data_dir}")
    print(f"{'='*60}")
    
    # 1. 데이터 로드
    loader = DataLoader(data_dir=data_dir)
    loader.load_all_streams(convert_timestamp=False)
    
    # 2. Streamer 생성
    streamer = DataFrameStreamer.from_loader(loader)
    
    # 3. Processor 생성
    processor = StreamProcessor(dataset_name=dataset_name)
    
    # 4. 이벤트 처리
    event_count = 0
    while streamer.has_more_events():
        event = streamer.get_next_event()
        if event:
            processor.add_event(event)
            event_count += 1
            
            # 진행 상황 출력 (10000개마다)
            if event_count % 10000 == 0:
                progress = streamer.get_progress()
                print(f"  Processed {event_count:,} events... "
                      f"(OB: {progress['orderbook']}, Trades: {progress['trades']})")
    
    # 남은 버퍼 처리
    processor.process_buffer()
    
    # 5. 결과 반환
    result = processor.get_result()
    result.print_summary()
    
    return result


def run_comparison(research_dir: str, validation_dir: str) -> dict:
    """
    Research와 Validation 데이터 비교 분석
    
    Args:
        research_dir: Research 데이터 디렉토리
        validation_dir: Validation 데이터 디렉토리
    
    Returns:
        {'research': ProcessingResult, 'validation': ProcessingResult}
    """
    # Research 처리
    research_result = process_dataset(research_dir, "Research")
    
    # Validation 처리
    validation_result = process_dataset(validation_dir, "Validation")
    
    # 비교
    compare_results(research_result, validation_result)
    
    return {
        'research': research_result,
        'validation': validation_result
    }


def run_single(data_dir: str, dataset_name: str = "Dataset") -> ProcessingResult:
    """
    단일 데이터셋만 처리
    
    Args:
        data_dir: 데이터 디렉토리
        dataset_name: 데이터셋 이름
    
    Returns:
        ProcessingResult
    """
    return process_dataset(data_dir, dataset_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream Processor Analysis")
    parser.add_argument("--research", type=str, help="Research 데이터 디렉토리")
    parser.add_argument("--validation", type=str, help="Validation 데이터 디렉토리")
    parser.add_argument("--single", type=str, help="단일 데이터셋 처리")
    parser.add_argument("--name", type=str, default="Dataset", help="데이터셋 이름")
    
    args = parser.parse_args()
    
    if args.single:
        # 단일 데이터셋 처리
        result = run_single(args.single, args.name)
    elif args.research and args.validation:
        # 비교 분석
        results = run_comparison(args.research, args.validation)
    else:
        print("사용법:")
        print("  단일 처리: python run_analysis.py --single /path/to/data --name MyDataset")
        print("  비교 분석: python run_analysis.py --research /path/to/research --validation /path/to/validation")