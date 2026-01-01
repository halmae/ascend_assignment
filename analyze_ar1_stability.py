#!/usr/bin/env python3
"""
Research Analysis: AR(1) Fit Quality 분포 분석

================================================================================
핵심 질문:
1. 평상시 fit_quality 분포는 어떤가?
2. Liquidation 근처에서 fit_quality가 유의미하게 떨어지는가?
3. 적정 threshold (VALID/INVALID)는 얼마인가?

이 분석 결과로 config.py의 AR(1) 임계값을 data-driven하게 설정
================================================================================

사용법:
    python analyze_ar1_stability.py --data ./data/research --output ./output/ar1_analysis.json
"""
import sys
import json
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import deque
import argparse

import pandas as pd
import numpy as np

# src 경로 추가
sys.path.insert(0, str(Path(__file__).parent))

from src.data_source import create_data_source
from src.data_types import Event, OrderbookState
from src.enums import EventType
from src.uncertainty import AR1Calculator


# =============================================================================
# Data Collection
# =============================================================================

@dataclass
class AR1Sample:
    """AR(1) 샘플"""
    timestamp: int
    spread_bps: float
    fit_quality: Optional[float]
    phi: float
    residual_std: float
    forecast_error: float
    n_samples: int


@dataclass
class LiquidationEvent:
    """Liquidation 이벤트"""
    timestamp: int
    side: str
    quantity: float
    price: float


class AR1Collector:
    """
    AR(1) 데이터 수집기
    
    Orderbook 이벤트마다 spread 계산 → AR(1) 업데이트 → fit_quality 기록
    """
    
    def __init__(self, window_size: int = 100, min_samples: int = 20):
        self.orderbook = OrderbookState(
            timestamp=0,
            bid_levels={},
            ask_levels={}
        )
        
        # AR(1) Calculator
        self.ar1_calculator = AR1Calculator(
            window_size=window_size,
            min_samples=min_samples
        )
        
        # 수집된 데이터
        self.ar1_samples: List[AR1Sample] = []
        self.liquidations: List[LiquidationEvent] = []
        
        # 샘플링 (매 orderbook 이벤트는 너무 많음 → 100개마다 1개)
        self.orderbook_count = 0
        self.sample_interval = 100
        
        # Progress
        self.event_count = 0
        self.last_print_time = time.time()
    
    def process_event(self, event: Event):
        """이벤트 처리"""
        self.event_count += 1
        
        if event.event_type == EventType.SNAPSHOT:
            # 전체 교체
            self.orderbook = OrderbookState(
                timestamp=event.timestamp,
                bid_levels={float(p): float(q) for p, q in event.data.get('bids', [])},
                ask_levels={float(p): float(q) for p, q in event.data.get('asks', [])}
            )
            # Snapshot 시 AR(1) 리셋 (연속성 끊김)
            self.ar1_calculator.reset()
            self._update_ar1(event.timestamp)
        
        elif event.event_type == EventType.ORDERBOOK:
            # Incremental update
            for level in event.data.get('bids', []):
                price, qty = float(level[0]), float(level[1])
                if qty == 0:
                    self.orderbook.bid_levels.pop(price, None)
                else:
                    self.orderbook.bid_levels[price] = qty
            
            for level in event.data.get('asks', []):
                price, qty = float(level[0]), float(level[1])
                if qty == 0:
                    self.orderbook.ask_levels.pop(price, None)
                else:
                    self.orderbook.ask_levels[price] = qty
            
            self.orderbook.timestamp = event.timestamp
            self._update_ar1(event.timestamp)
        
        elif event.event_type == EventType.LIQUIDATION:
            data = event.data
            if data.get('quantity', 0) > 0:
                self.liquidations.append(LiquidationEvent(
                    timestamp=event.timestamp,
                    side=data.get('side', 'unknown'),
                    quantity=float(data.get('quantity', 0)),
                    price=float(data.get('price', 0)),
                ))
        
        # Progress 출력
        if time.time() - self.last_print_time > 0.5:
            self._print_progress()
            self.last_print_time = time.time()
    
    def _update_ar1(self, timestamp: int):
        """AR(1) 업데이트 및 샘플 기록"""
        if not self.orderbook.bid_levels or not self.orderbook.ask_levels:
            return
        
        best_bid = self.orderbook.get_best_bid()
        best_ask = self.orderbook.get_best_ask()
        
        if best_bid is None or best_ask is None:
            return
        
        if best_bid >= best_ask:
            return  # Crossed market
        
        mid_price = (best_bid + best_ask) / 2
        spread = best_ask - best_bid
        spread_bps = spread / mid_price * 10000
        
        # AR(1) 업데이트
        self.ar1_calculator.update(spread_bps)
        self.orderbook_count += 1
        
        # 샘플링
        if self.orderbook_count % self.sample_interval == 0:
            ar1_result = self.ar1_calculator.compute()
            
            self.ar1_samples.append(AR1Sample(
                timestamp=timestamp,
                spread_bps=spread_bps,
                fit_quality=ar1_result.fit_quality,
                phi=ar1_result.phi,
                residual_std=ar1_result.residual_std,
                forecast_error=ar1_result.forecast_error,
                n_samples=ar1_result.n_samples,
            ))
    
    def _print_progress(self):
        """진행 상황 출력"""
        print(f"\r  Processing: {self.event_count:>10,} events | "
              f"AR1 samples: {len(self.ar1_samples):>8,} | "
              f"Liquidations: {len(self.liquidations):>4}",
              end='', flush=True)


# =============================================================================
# Analysis
# =============================================================================

def analyze_fit_quality_distribution(
    samples: List[AR1Sample],
    liquidations: List[LiquidationEvent],
    buffer_us: int = 30_000_000  # 30초
) -> Dict:
    """
    fit_quality 분포 분석
    
    - 전체 분포
    - Liquidation 근처 vs 평상시 비교
    """
    print("\n\n📈 fit_quality 분포 분석...")
    
    # None 제외한 유효 샘플
    valid_samples = [s for s in samples if s.fit_quality is not None]
    
    print(f"  전체 샘플: {len(samples):,}")
    print(f"  유효 샘플 (fit_quality != None): {len(valid_samples):,}")
    print(f"  무효 샘플 (변동 없음 등): {len(samples) - len(valid_samples):,}")
    
    if not valid_samples:
        return {'error': 'No valid fit_quality samples'}
    
    # DataFrame 변환
    df = pd.DataFrame([
        {
            'timestamp': s.timestamp,
            'fit_quality': s.fit_quality,
            'phi': s.phi,
            'residual_std': s.residual_std,
            'forecast_error': s.forecast_error,
            'spread_bps': s.spread_bps,
        }
        for s in valid_samples
    ])
    
    # Spread 분포 출력 (디버깅용)
    print(f"\n  [Spread 분포 (bps)]")
    print(f"    Mean:   {df['spread_bps'].mean():.4f}")
    print(f"    Std:    {df['spread_bps'].std():.4f}")
    print(f"    Min:    {df['spread_bps'].min():.4f}")
    print(f"    Max:    {df['spread_bps'].max():.4f}")
    
    # 전체 분포
    all_fit = df['fit_quality']
    all_stats = {
        'count': len(all_fit),
        'mean': float(all_fit.mean()),
        'std': float(all_fit.std()),
        'median': float(all_fit.median()),
        'p05': float(all_fit.quantile(0.05)),
        'p10': float(all_fit.quantile(0.10)),
        'p25': float(all_fit.quantile(0.25)),
        'p75': float(all_fit.quantile(0.75)),
        'p90': float(all_fit.quantile(0.90)),
        'p95': float(all_fit.quantile(0.95)),
        'min': float(all_fit.min()),
        'max': float(all_fit.max()),
    }
    
    result = {'all': all_stats}
    
    # Liquidation 근처 vs 평상시
    if liquidations:
        liq_timestamps = [l.timestamp for l in liquidations]
        
        is_near_liq = np.zeros(len(df), dtype=bool)
        for liq_ts in liq_timestamps:
            mask = (df['timestamp'] >= liq_ts - buffer_us) & \
                   (df['timestamp'] <= liq_ts + buffer_us)
            is_near_liq |= mask.values
        
        normal_df = df[~is_near_liq]
        liq_df = df[is_near_liq]
        
        if len(normal_df) > 0:
            normal_fit = normal_df['fit_quality']
            result['normal'] = {
                'count': len(normal_fit),
                'mean': float(normal_fit.mean()),
                'std': float(normal_fit.std()),
                'median': float(normal_fit.median()),
                'p05': float(normal_fit.quantile(0.05)),
                'p10': float(normal_fit.quantile(0.10)),
                'p25': float(normal_fit.quantile(0.25)),
            }
        
        if len(liq_df) > 0:
            liq_fit = liq_df['fit_quality']
            result['near_liquidation'] = {
                'count': len(liq_fit),
                'mean': float(liq_fit.mean()),
                'std': float(liq_fit.std()),
                'median': float(liq_fit.median()),
                'p05': float(liq_fit.quantile(0.05)),
                'p10': float(liq_fit.quantile(0.10)),
                'p25': float(liq_fit.quantile(0.25)),
            }
        
        print(f"\n  평상시: {len(normal_df):,}")
        print(f"  Liquidation 근처 (±30s): {len(liq_df):,}")
    
    return result


def analyze_phi_distribution(samples: List[AR1Sample]) -> Dict:
    """φ (autocorrelation) 분포 분석"""
    print("\n📊 φ 분포 분석...")
    
    valid_samples = [s for s in samples if s.fit_quality is not None]
    
    if not valid_samples:
        return {'error': 'No valid samples'}
    
    phi_values = [s.phi for s in valid_samples]
    
    return {
        'count': len(phi_values),
        'mean': float(np.mean(phi_values)),
        'std': float(np.std(phi_values)),
        'median': float(np.median(phi_values)),
        'p05': float(np.percentile(phi_values, 5)),
        'p95': float(np.percentile(phi_values, 95)),
        'min': float(np.min(phi_values)),
        'max': float(np.max(phi_values)),
        'pct_high_phi': float(np.mean([abs(p) > 0.9 for p in phi_values]) * 100),
    }


def analyze_forecast_error_distribution(samples: List[AR1Sample]) -> Dict:
    """Forecast error 분포 분석"""
    print("\n📊 Forecast error 분포 분석...")
    
    # fit_quality가 유효하고, residual_std가 충분히 큰 샘플만
    MIN_RESIDUAL_STD = 0.01  # 최소 0.01 bps
    valid_samples = [
        s for s in samples 
        if s.fit_quality is not None 
        and s.residual_std > MIN_RESIDUAL_STD
    ]
    
    if not valid_samples:
        return {'error': 'No valid samples with sufficient residual_std'}
    
    # forecast_error / residual_std (σ 배수)
    error_ratios = [s.forecast_error / s.residual_std for s in valid_samples]
    
    return {
        'count': len(error_ratios),
        'mean': float(np.mean(error_ratios)),
        'std': float(np.std(error_ratios)),
        'median': float(np.median(error_ratios)),
        'p75': float(np.percentile(error_ratios, 75)),
        'p90': float(np.percentile(error_ratios, 90)),
        'p95': float(np.percentile(error_ratios, 95)),
        'p99': float(np.percentile(error_ratios, 99)),
        'pct_above_2_5': float(np.mean([r > 2.5 for r in error_ratios]) * 100),
        'pct_above_4': float(np.mean([r > 4.0 for r in error_ratios]) * 100),
    }


def compute_threshold_recommendations(fit_quality_dist: Dict) -> Dict:
    """
    Threshold 권장값 계산
    
    로직:
    - VALID threshold: 평상시 p10 (90%가 이 이상)
    - INVALID threshold: 평상시 p05 (5%만 이 이하)
    """
    print("\n🎯 Threshold 권장값 계산...")
    
    recommendations = {}
    
    if 'normal' in fit_quality_dist:
        normal = fit_quality_dist['normal']
        
        # VALID: 평상시 분포의 10th percentile
        # "평상시에도 이 정도 이하는 거의 없다"
        valid_threshold = normal['p10']
        
        # INVALID: 평상시 분포의 5th percentile
        # "평상시에 이 이하면 매우 이상하다"
        invalid_threshold = normal['p05']
        
        recommendations['fit_quality_valid'] = round(valid_threshold, 2)
        recommendations['fit_quality_invalid'] = round(invalid_threshold, 2)
        
        print(f"  평상시 p10: {valid_threshold:.3f} → VALID threshold 권장")
        print(f"  평상시 p05: {invalid_threshold:.3f} → INVALID threshold 권장")
    
    if 'near_liquidation' in fit_quality_dist:
        liq = fit_quality_dist['near_liquidation']
        normal = fit_quality_dist.get('normal', {})
        
        if normal:
            # Separation 분석
            normal_mean = normal['mean']
            liq_mean = liq['mean']
            separation = normal_mean - liq_mean
            
            recommendations['separation'] = {
                'normal_mean': round(normal_mean, 3),
                'liquidation_mean': round(liq_mean, 3),
                'difference': round(separation, 3),
            }
            
            print(f"\n  분포 분리도:")
            print(f"    평상시 평균: {normal_mean:.3f}")
            print(f"    Liquidation 평균: {liq_mean:.3f}")
            print(f"    차이: {separation:.3f}")
            
            if separation > 0.1:
                print(f"  → ✅ fit_quality가 liquidation 근처에서 유의미하게 떨어짐")
            else:
                print(f"  → ⚠️ 분포 차이가 크지 않음 (AR(1)만으로는 부족할 수 있음)")
    
    return recommendations


# =============================================================================
# Output
# =============================================================================

def print_results(
    fit_quality_dist: Dict,
    phi_dist: Dict,
    forecast_error_dist: Dict,
    recommendations: Dict
):
    """결과 출력"""
    print("\n" + "=" * 70)
    print("📊 AR(1) ANALYSIS RESULTS")
    print("=" * 70)
    
    # fit_quality 분포
    print("\n[1. fit_quality 분포]")
    
    if 'all' in fit_quality_dist:
        all_stats = fit_quality_dist['all']
        print(f"\n  전체:")
        print(f"    Count:  {all_stats['count']:,}")
        print(f"    Mean:   {all_stats['mean']:.4f}")
        print(f"    Std:    {all_stats['std']:.4f}")
        print(f"    Median: {all_stats['median']:.4f}")
        print(f"    P05:    {all_stats['p05']:.4f}")
        print(f"    P10:    {all_stats['p10']:.4f}")
        print(f"    P25:    {all_stats['p25']:.4f}")
    
    if 'normal' in fit_quality_dist:
        normal = fit_quality_dist['normal']
        print(f"\n  평상시 (Liquidation ±30s 제외):")
        print(f"    Count:  {normal['count']:,}")
        print(f"    Mean:   {normal['mean']:.4f}")
        print(f"    P05:    {normal['p05']:.4f}")
        print(f"    P10:    {normal['p10']:.4f}")
    
    if 'near_liquidation' in fit_quality_dist:
        liq = fit_quality_dist['near_liquidation']
        print(f"\n  Liquidation 근처 (±30s):")
        print(f"    Count:  {liq['count']:,}")
        print(f"    Mean:   {liq['mean']:.4f}")
        print(f"    P05:    {liq['p05']:.4f}")
        print(f"    P10:    {liq['p10']:.4f}")
    
    # φ 분포
    print("\n[2. φ (autocorrelation) 분포]")
    print(f"  Mean:   {phi_dist['mean']:.4f}")
    print(f"  Std:    {phi_dist['std']:.4f}")
    print(f"  Median: {phi_dist['median']:.4f}")
    print(f"  |φ| > 0.9 비율: {phi_dist['pct_high_phi']:.1f}%")
    
    # Forecast error 분포
    print("\n[3. Forecast Error 분포 (σ 배수)]")
    print(f"  Mean:   {forecast_error_dist['mean']:.2f}σ")
    print(f"  Median: {forecast_error_dist['median']:.2f}σ")
    print(f"  P90:    {forecast_error_dist['p90']:.2f}σ")
    print(f"  P95:    {forecast_error_dist['p95']:.2f}σ")
    print(f"  > 2.5σ 비율: {forecast_error_dist['pct_above_2_5']:.1f}%")
    print(f"  > 4.0σ 비율: {forecast_error_dist['pct_above_4']:.1f}%")
    
    # 권장값
    print("\n" + "=" * 70)
    print("💡 CONFIG.PY 권장값")
    print("=" * 70)
    
    if 'fit_quality_valid' in recommendations:
        print(f"\n  # AR(1) Stability Thresholds")
        print(f"  ar1_fit_quality_valid:   {recommendations['fit_quality_valid']}")
        print(f"  ar1_fit_quality_invalid: {recommendations['fit_quality_invalid']}")
        
        # Forecast error는 분포 기반
        print(f"\n  # Forecast Error Thresholds (현재 설정 유지)")
        print(f"  ar1_forecast_error_valid_mult:   2.5  # p90 기준")
        print(f"  ar1_forecast_error_invalid_mult: 4.0  # p99+ 기준")
    
    print("\n" + "=" * 70)


def save_results(output_path: str, fit_quality_dist: Dict, phi_dist: Dict, 
                 forecast_error_dist: Dict, recommendations: Dict):
    """결과 저장"""
    results = {
        'timestamp': datetime.now().isoformat(),
        'fit_quality_distribution': fit_quality_dist,
        'phi_distribution': phi_dist,
        'forecast_error_distribution': forecast_error_dist,
        'recommendations': recommendations,
    }
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n✅ 결과 저장: {output_path}")


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='AR(1) fit_quality 분포 분석')
    parser.add_argument('--data', type=str, default='./data/research',
                        help='데이터 디렉토리')
    parser.add_argument('--output', type=str, default='./output/ar1_analysis.json',
                        help='결과 저장 경로')
    parser.add_argument('--window', type=int, default=100,
                        help='AR(1) window size')
    parser.add_argument('--min-samples', type=int, default=20,
                        help='AR(1) minimum samples')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("🔬 AR(1) Stability 분석")
    print("=" * 70)
    print(f"  Data: {args.data}")
    print(f"  Output: {args.output}")
    print(f"  AR(1) window: {args.window}")
    print(f"  AR(1) min_samples: {args.min_samples}")
    print("=" * 70)
    
    # 1. 데이터 수집
    print("\n📂 데이터 수집 중...")
    
    collector = AR1Collector(
        window_size=args.window,
        min_samples=args.min_samples
    )
    source = create_data_source('historical', data_dir=args.data)
    
    start_time = time.time()
    for event in source.get_events():
        collector.process_event(event)
    
    elapsed = time.time() - start_time
    print(f"\n  ✅ 완료: {collector.event_count:,} events in {elapsed:.1f}s")
    print(f"     AR1 samples: {len(collector.ar1_samples):,}")
    print(f"     Liquidations: {len(collector.liquidations)}")
    
    if not collector.ar1_samples:
        print("\n❌ AR1 샘플 없음. 분석 중단.")
        return
    
    # 2. fit_quality 분포 분석
    fit_quality_dist = analyze_fit_quality_distribution(
        collector.ar1_samples,
        collector.liquidations
    )
    
    if 'error' in fit_quality_dist:
        print(f"\n❌ {fit_quality_dist['error']}")
        return
    
    # 3. φ 분포 분석
    phi_dist = analyze_phi_distribution(collector.ar1_samples)
    
    # 4. Forecast error 분포 분석
    forecast_error_dist = analyze_forecast_error_distribution(collector.ar1_samples)
    
    # 5. Threshold 권장값 계산
    recommendations = compute_threshold_recommendations(fit_quality_dist)
    
    # 6. 결과 출력 및 저장
    print_results(fit_quality_dist, phi_dist, forecast_error_dist, recommendations)
    save_results(args.output, fit_quality_dist, phi_dist, forecast_error_dist, recommendations)


if __name__ == "__main__":
    main()