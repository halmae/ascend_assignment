"""
Liquidation Impact Analyzer

목적:
1. Liquidation 전후 Quantity 변화 측정
2. Stability 지표로 사용할 적절한 Quantity 도출
3. Spread Volatility의 Liquidation 예측력 검증

핵심 아이디어:
- 단순 임계값 대신, Quantity 간 "비율의 안정성"을 측정
- AR(1) 기반 Anomaly Detection으로 급변 감지
- Liquidation과의 실제 상관관계 검증
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Deque
from collections import deque
import numpy as np
import json


@dataclass
class QuantitySnapshot:
    """특정 시점의 모든 Quantity 스냅샷"""
    timestamp: int
    
    # Orderbook 구조
    best_bid: float = 0.0
    best_ask: float = 0.0
    spread: float = 0.0
    mid_price: float = 0.0
    
    # Depth
    bid_depth: float = 0.0
    ask_depth: float = 0.0
    total_depth: float = 0.0
    imbalance: float = 0.0  # (bid - ask) / total
    
    # Derived
    spread_bps: float = 0.0  # spread / mid_price * 10000
    relative_spread: float = 0.0  # spread / mid_price
    
    # Liquidation 관련
    time_to_next_liq_ms: Optional[float] = None  # 다음 Liquidation까지 시간
    time_since_last_liq_ms: Optional[float] = None  # 이전 Liquidation 이후 시간
    is_pre_liquidation: bool = False  # 100ms 이내 Liquidation 예정
    is_post_liquidation: bool = False  # 5000ms 이내 Liquidation 발생
    
    def to_dict(self) -> Dict:
        return {
            'timestamp': self.timestamp,
            'spread': self.spread,
            'spread_bps': self.spread_bps,
            'mid_price': self.mid_price,
            'bid_depth': self.bid_depth,
            'ask_depth': self.ask_depth,
            'imbalance': self.imbalance,
            'time_to_next_liq_ms': self.time_to_next_liq_ms,
            'time_since_last_liq_ms': self.time_since_last_liq_ms,
        }


@dataclass
class RollingStats:
    """Rolling 통계 계산기 (AR 기반 Anomaly Detection 포함)"""
    window_size: int = 100
    values: Deque[float] = field(default_factory=lambda: deque(maxlen=100))
    timestamps: Deque[int] = field(default_factory=lambda: deque(maxlen=100))
    
    def add(self, value: float, timestamp: int):
        self.values.append(value)
        self.timestamps.append(timestamp)
    
    def get_mean(self) -> float:
        if not self.values:
            return 0.0
        return np.mean(list(self.values))
    
    def get_std(self) -> float:
        if len(self.values) < 2:
            return 0.0
        return np.std(list(self.values))
    
    def get_cv(self) -> float:
        """Coefficient of Variation"""
        mean = self.get_mean()
        if mean == 0:
            return 0.0
        return self.get_std() / abs(mean)
    
    def get_ar1_coefficient(self) -> float:
        """
        AR(1) coefficient 계산
        
        y_t = c + φ * y_{t-1} + ε_t
        
        φ가 1에 가까우면 → 시계열이 persistent (정상 상태)
        φ가 급변하면 → regime change (이상 상태)
        """
        if len(self.values) < 10:
            return 0.0
        
        values = np.array(list(self.values))
        y_t = values[1:]
        y_t_1 = values[:-1]
        
        # 간단한 OLS: φ = Cov(y_t, y_{t-1}) / Var(y_{t-1})
        cov = np.cov(y_t, y_t_1)[0, 1]
        var = np.var(y_t_1)
        
        if var == 0:
            return 0.0
        
        return cov / var
    
    def get_ar1_residual_std(self) -> float:
        """
        AR(1) 잔차의 표준편차
        
        잔차가 크면 → 예측 어려움 → 불안정
        """
        if len(self.values) < 10:
            return 0.0
        
        phi = self.get_ar1_coefficient()
        values = np.array(list(self.values))
        
        # 잔차 계산: ε_t = y_t - φ * y_{t-1}
        y_t = values[1:]
        y_t_1 = values[:-1]
        residuals = y_t - phi * y_t_1
        
        return np.std(residuals)
    
    def is_anomaly(self, current_value: float, threshold_sigma: float = 3.0) -> Tuple[bool, float]:
        """
        현재 값이 Anomaly인지 판단
        
        Moving average 대비 편차가 threshold_sigma * std 초과 시 Anomaly
        (단순하지만 robust한 방법)
        
        Returns:
            (is_anomaly, z_score)
        """
        if len(self.values) < 10:
            return False, 0.0
        
        mean = self.get_mean()
        std = self.get_std()
        
        if std == 0:
            return False, 0.0
        
        z_score = abs(current_value - mean) / std
        
        return z_score > threshold_sigma, z_score
    
    def get_recent_change_ratio(self, lookback: int = 5) -> float:
        """
        최근 변화율
        
        최근 lookback 개의 평균 / 전체 평균
        → 1.0에서 벗어나면 급변 중
        """
        if len(self.values) < lookback + 5:
            return 1.0
        
        recent_mean = np.mean(list(self.values)[-lookback:])
        overall_mean = self.get_mean()
        
        if overall_mean == 0:
            return 1.0
        
        return recent_mean / overall_mean


@dataclass 
class RatioTracker:
    """
    두 Quantity의 비율 추적
    
    핵심 아이디어:
    "두 양이 각각 변해도, 비율이 안정적이면 시장은 정상"
    "비율이 급변하면 regime change"
    
    예: spread / depth 비율
        - 정상: spread↑, depth↓ 동시 발생 (자연스러운 관계)
        - 이상: spread↑, depth 변화 없음 (비정상적 spread 확대)
    """
    name: str = ""
    window_size: int = 100
    ratios: Deque[float] = field(default_factory=lambda: deque(maxlen=100))
    timestamps: Deque[int] = field(default_factory=lambda: deque(maxlen=100))
    
    def add(self, numerator: float, denominator: float, timestamp: int):
        if denominator == 0:
            return
        ratio = numerator / denominator
        self.ratios.append(ratio)
        self.timestamps.append(timestamp)
    
    def get_mean(self) -> float:
        if not self.ratios:
            return 0.0
        return np.mean(list(self.ratios))
    
    def get_std(self) -> float:
        if len(self.ratios) < 2:
            return 0.0
        return np.std(list(self.ratios))
    
    def get_cv(self) -> float:
        """비율의 변동계수 - 이게 작으면 안정적"""
        mean = self.get_mean()
        if mean == 0:
            return 0.0
        return self.get_std() / abs(mean)
    
    def get_stability_score(self) -> float:
        """
        안정성 점수 (0~1)
        
        CV가 작을수록 1에 가까움
        """
        cv = self.get_cv()
        # CV가 0이면 완전 안정 (1.0), CV가 1 이상이면 불안정 (0.0)
        return max(0.0, 1.0 - cv)
    
    def is_ratio_broken(self, threshold_cv: float = 0.3) -> bool:
        """비율 관계가 깨졌는지 판단"""
        return self.get_cv() > threshold_cv


class LiquidationImpactAnalyzer:
    """
    Liquidation 전후 Quantity 변화 분석기
    
    핵심 기능:
    1. Liquidation 이벤트 전후로 Quantity 스냅샷 수집
    2. 전후 비교를 통한 Impact 측정
    3. Stability 지표로 사용할 Quantity 후보 평가
    """
    
    def __init__(self, 
                 pre_window_ms: float = 1000.0,   # Liquidation 전 1초
                 post_window_ms: float = 5000.0,  # Liquidation 후 5초
                 snapshot_interval_ms: float = 100.0):  # 100ms마다 스냅샷
        
        self.pre_window_ms = pre_window_ms
        self.post_window_ms = post_window_ms
        self.snapshot_interval_ms = snapshot_interval_ms
        
        # Liquidation 이벤트 기록
        self.liquidation_events: List[Dict] = []
        
        # Quantity 스냅샷 (시간순)
        self.quantity_snapshots: Deque[QuantitySnapshot] = deque(maxlen=10000)
        
        # Rolling 통계 (실시간 Anomaly Detection용)
        self.spread_stats = RollingStats(window_size=100)
        self.depth_stats = RollingStats(window_size=100)
        self.imbalance_stats = RollingStats(window_size=100)
        
        # 비율 추적 (Stability 지표 후보)
        self.spread_depth_ratio = RatioTracker(name="spread/total_depth")
        self.bid_ask_depth_ratio = RatioTracker(name="bid_depth/ask_depth")
        self.spread_imbalance_ratio = RatioTracker(name="spread/|imbalance|")
        
        # 분석 결과
        self.impact_results: List[Dict] = []
    
    def record_snapshot(self, 
                        timestamp: int,
                        best_bid: float,
                        best_ask: float,
                        bid_depth: float,
                        ask_depth: float):
        """Quantity 스냅샷 기록"""
        
        if best_bid >= best_ask or best_bid == 0:
            return  # Invalid state
        
        spread = best_ask - best_bid
        mid_price = (best_bid + best_ask) / 2
        total_depth = bid_depth + ask_depth
        imbalance = (bid_depth - ask_depth) / total_depth if total_depth > 0 else 0
        
        snapshot = QuantitySnapshot(
            timestamp=timestamp,
            best_bid=best_bid,
            best_ask=best_ask,
            spread=spread,
            mid_price=mid_price,
            spread_bps=(spread / mid_price) * 10000,
            relative_spread=spread / mid_price,
            bid_depth=bid_depth,
            ask_depth=ask_depth,
            total_depth=total_depth,
            imbalance=imbalance
        )
        
        self.quantity_snapshots.append(snapshot)
        
        # Rolling 통계 업데이트
        self.spread_stats.add(spread, timestamp)
        self.depth_stats.add(total_depth, timestamp)
        self.imbalance_stats.add(imbalance, timestamp)
        
        # 비율 업데이트
        self.spread_depth_ratio.add(spread, total_depth, timestamp)
        if ask_depth > 0:
            self.bid_ask_depth_ratio.add(bid_depth, ask_depth, timestamp)
        if abs(imbalance) > 0.01:  # imbalance가 의미 있을 때만
            self.spread_imbalance_ratio.add(spread, abs(imbalance), timestamp)
    
    def record_liquidation(self, 
                           timestamp: int,
                           side: str,
                           quantity: float,
                           price: float):
        """Liquidation 이벤트 기록"""
        self.liquidation_events.append({
            'timestamp': timestamp,
            'side': side,
            'quantity': quantity,
            'price': price
        })
    
    def check_anomaly(self) -> Dict:
        """
        현재 상태가 Anomaly인지 체크
        
        Returns:
            {
                'is_anomaly': bool,
                'anomaly_sources': List[str],
                'details': Dict
            }
        """
        anomalies = []
        details = {}
        
        # 1. Spread Anomaly (AR 기반)
        if self.spread_stats.values:
            current_spread = self.spread_stats.values[-1]
            is_spread_anomaly, spread_z = self.spread_stats.is_anomaly(current_spread, threshold_sigma=3.0)
            details['spread_z_score'] = spread_z
            if is_spread_anomaly:
                anomalies.append('spread_ar_anomaly')
        
        # 2. Depth Anomaly
        if self.depth_stats.values:
            current_depth = self.depth_stats.values[-1]
            is_depth_anomaly, depth_z = self.depth_stats.is_anomaly(current_depth, threshold_sigma=3.0)
            details['depth_z_score'] = depth_z
            if is_depth_anomaly:
                anomalies.append('depth_ar_anomaly')
        
        # 3. 비율 안정성 체크
        if self.spread_depth_ratio.is_ratio_broken(threshold_cv=0.3):
            anomalies.append('spread_depth_ratio_broken')
            details['spread_depth_cv'] = self.spread_depth_ratio.get_cv()
        
        if self.bid_ask_depth_ratio.is_ratio_broken(threshold_cv=0.5):
            anomalies.append('bid_ask_ratio_broken')
            details['bid_ask_cv'] = self.bid_ask_depth_ratio.get_cv()
        
        # 4. Recent Change Ratio
        spread_change = self.spread_stats.get_recent_change_ratio(lookback=5)
        details['spread_recent_change'] = spread_change
        if spread_change > 2.0 or spread_change < 0.5:
            anomalies.append('spread_sudden_change')
        
        return {
            'is_anomaly': len(anomalies) > 0,
            'anomaly_sources': anomalies,
            'details': details
        }
    
    def get_stability_metrics(self) -> Dict:
        """
        현재 Stability 메트릭 반환
        
        기존: spread_volatility (단순 CV)
        제안: 여러 비율의 안정성 종합 점수
        """
        return {
            # 기존 방식
            'spread_cv': self.spread_stats.get_cv(),
            'spread_ar1_coef': self.spread_stats.get_ar1_coefficient(),
            'spread_ar1_residual_std': self.spread_stats.get_ar1_residual_std(),
            
            # 비율 기반 안정성
            'spread_depth_stability': self.spread_depth_ratio.get_stability_score(),
            'bid_ask_depth_stability': self.bid_ask_depth_ratio.get_stability_score(),
            
            # 종합 점수 (제안)
            'composite_stability': self._calculate_composite_stability()
        }
    
    def _calculate_composite_stability(self) -> float:
        """
        종합 Stability 점수 (0~1)
        
        여러 지표의 가중 평균:
        - AR(1) 잔차 기반 예측 가능성 (40%)
        - Spread/Depth 비율 안정성 (30%)
        - Bid/Ask 비율 안정성 (30%)
        """
        # AR(1) 기반 점수: 잔차 std가 작을수록 좋음
        ar_residual = self.spread_stats.get_ar1_residual_std()
        spread_mean = self.spread_stats.get_mean()
        if spread_mean > 0:
            ar_score = max(0, 1 - ar_residual / spread_mean)
        else:
            ar_score = 1.0
        
        # 비율 안정성 점수
        sd_stability = self.spread_depth_ratio.get_stability_score()
        ba_stability = self.bid_ask_depth_ratio.get_stability_score()
        
        # 가중 평균
        composite = 0.4 * ar_score + 0.3 * sd_stability + 0.3 * ba_stability
        
        return composite
    
    def analyze_liquidation_impact(self) -> Dict:
        """
        모든 Liquidation 이벤트에 대해 전후 Impact 분석
        
        Returns:
            {
                'total_liquidations': int,
                'avg_impact': Dict,
                'by_side': Dict,
                'quantity_correlations': Dict,
                'recommended_stability_metric': str
            }
        """
        if not self.liquidation_events or not self.quantity_snapshots:
            return {'error': 'Insufficient data'}
        
        impacts = []
        
        for liq in self.liquidation_events:
            liq_ts = liq['timestamp']
            
            # Pre-liquidation 스냅샷 찾기 (1초 전 ~ 직전)
            pre_snapshots = [
                s for s in self.quantity_snapshots
                if liq_ts - self.pre_window_ms * 1000 <= s.timestamp < liq_ts
            ]
            
            # Post-liquidation 스냅샷 찾기 (직후 ~ 5초 후)
            post_snapshots = [
                s for s in self.quantity_snapshots
                if liq_ts < s.timestamp <= liq_ts + self.post_window_ms * 1000
            ]
            
            if not pre_snapshots or not post_snapshots:
                continue
            
            # 평균 계산
            pre_avg = self._average_snapshots(pre_snapshots)
            post_avg = self._average_snapshots(post_snapshots)
            
            # Impact 계산 (변화율)
            impact = {
                'liquidation': liq,
                'pre_avg': pre_avg,
                'post_avg': post_avg,
                'spread_change_ratio': post_avg['spread'] / pre_avg['spread'] if pre_avg['spread'] > 0 else 1.0,
                'depth_change_ratio': post_avg['total_depth'] / pre_avg['total_depth'] if pre_avg['total_depth'] > 0 else 1.0,
                'imbalance_change': post_avg['imbalance'] - pre_avg['imbalance'],
                'spread_depth_ratio_change': (
                    (post_avg['spread'] / post_avg['total_depth']) / 
                    (pre_avg['spread'] / pre_avg['total_depth'])
                ) if pre_avg['total_depth'] > 0 and post_avg['total_depth'] > 0 and pre_avg['spread'] > 0 else 1.0
            }
            
            impacts.append(impact)
        
        if not impacts:
            return {'error': 'No valid impact data'}
        
        # 집계
        avg_spread_change = np.mean([i['spread_change_ratio'] for i in impacts])
        avg_depth_change = np.mean([i['depth_change_ratio'] for i in impacts])
        avg_sd_ratio_change = np.mean([i['spread_depth_ratio_change'] for i in impacts])
        
        # Side별 분석
        buy_impacts = [i for i in impacts if i['liquidation']['side'] == 'BUY']
        sell_impacts = [i for i in impacts if i['liquidation']['side'] == 'SELL']
        
        return {
            'total_liquidations': len(self.liquidation_events),
            'analyzed_liquidations': len(impacts),
            'avg_impact': {
                'spread_change_ratio': avg_spread_change,
                'depth_change_ratio': avg_depth_change,
                'spread_depth_ratio_change': avg_sd_ratio_change,
            },
            'by_side': {
                'BUY': {
                    'count': len(buy_impacts),
                    'avg_spread_change': np.mean([i['spread_change_ratio'] for i in buy_impacts]) if buy_impacts else 0,
                    'avg_imbalance_change': np.mean([i['imbalance_change'] for i in buy_impacts]) if buy_impacts else 0,
                },
                'SELL': {
                    'count': len(sell_impacts),
                    'avg_spread_change': np.mean([i['spread_change_ratio'] for i in sell_impacts]) if sell_impacts else 0,
                    'avg_imbalance_change': np.mean([i['imbalance_change'] for i in sell_impacts]) if sell_impacts else 0,
                }
            },
            'recommendation': self._generate_recommendation(impacts)
        }
    
    def _average_snapshots(self, snapshots: List[QuantitySnapshot]) -> Dict:
        """스냅샷 리스트의 평균"""
        if not snapshots:
            return {}
        
        return {
            'spread': np.mean([s.spread for s in snapshots]),
            'total_depth': np.mean([s.total_depth for s in snapshots]),
            'imbalance': np.mean([s.imbalance for s in snapshots]),
            'bid_depth': np.mean([s.bid_depth for s in snapshots]),
            'ask_depth': np.mean([s.ask_depth for s in snapshots]),
        }
    
    def _generate_recommendation(self, impacts: List[Dict]) -> Dict:
        """
        분석 결과 기반 권장사항 생성
        """
        # Spread 변화 분포
        spread_changes = [i['spread_change_ratio'] for i in impacts]
        spread_change_std = np.std(spread_changes)
        
        # Spread/Depth 비율 변화 분포
        sd_ratio_changes = [i['spread_depth_ratio_change'] for i in impacts]
        sd_ratio_change_std = np.std(sd_ratio_changes)
        
        # 어느 지표가 더 안정적인 변화를 보이는가?
        if sd_ratio_change_std < spread_change_std:
            recommended_metric = "spread_depth_ratio"
            reason = "Spread/Depth 비율이 Liquidation 전후로 더 일관된 변화를 보임"
        else:
            recommended_metric = "spread_volatility"
            reason = "Spread 변화가 더 직접적인 지표"
        
        return {
            'recommended_metric': recommended_metric,
            'reason': reason,
            'spread_change_std': spread_change_std,
            'sd_ratio_change_std': sd_ratio_change_std,
            'suggested_threshold': {
                'spread_change': 1.0 + 2 * spread_change_std,  # 2σ 초과 시 이상
                'sd_ratio_change': 1.0 + 2 * sd_ratio_change_std
            }
        }
    
    def analyze_pre_liquidation_signals(self, 
                                         pre_window_ms: float = 500.0) -> Dict:
        """
        Liquidation 직전 신호 분석
        
        "어떤 Quantity 변화가 Liquidation을 예측하는가?"
        
        Args:
            pre_window_ms: Liquidation 전 분석 윈도우 (ms)
        """
        signals = []
        
        for liq in self.liquidation_events:
            liq_ts = liq['timestamp']
            
            # Liquidation 직전 스냅샷
            pre_snapshots = [
                s for s in self.quantity_snapshots
                if liq_ts - pre_window_ms * 1000 <= s.timestamp < liq_ts
            ]
            
            # 기준점: 더 이전 스냅샷 (1초~500ms 전)
            baseline_snapshots = [
                s for s in self.quantity_snapshots
                if liq_ts - 1000 * 1000 <= s.timestamp < liq_ts - pre_window_ms * 1000
            ]
            
            if not pre_snapshots or not baseline_snapshots:
                continue
            
            pre_avg = self._average_snapshots(pre_snapshots)
            baseline_avg = self._average_snapshots(baseline_snapshots)
            
            # 변화율 계산
            signal = {
                'liquidation_ts': liq_ts,
                'liquidation_side': liq['side'],
                'liquidation_qty': liq['quantity'],
                
                # 직전 변화
                'spread_increase': pre_avg['spread'] / baseline_avg['spread'] if baseline_avg['spread'] > 0 else 1.0,
                'depth_decrease': baseline_avg['total_depth'] / pre_avg['total_depth'] if pre_avg['total_depth'] > 0 else 1.0,
                'imbalance_shift': pre_avg['imbalance'] - baseline_avg['imbalance'],
                
                # 어느 쪽 depth가 감소?
                'bid_depth_ratio': pre_avg['bid_depth'] / baseline_avg['bid_depth'] if baseline_avg['bid_depth'] > 0 else 1.0,
                'ask_depth_ratio': pre_avg['ask_depth'] / baseline_avg['ask_depth'] if baseline_avg['ask_depth'] > 0 else 1.0,
            }
            signals.append(signal)
        
        if not signals:
            return {'error': 'No signal data'}
        
        # 분석
        # 1. Spread 증가가 선행 지표인가?
        spread_increases = [s['spread_increase'] for s in signals]
        
        # 2. Depth 감소가 선행 지표인가?
        depth_decreases = [s['depth_decrease'] for s in signals]
        
        # 3. Side별 Depth 변화
        buy_liqs = [s for s in signals if s['liquidation_side'] == 'BUY']
        sell_liqs = [s for s in signals if s['liquidation_side'] == 'SELL']
        
        return {
            'total_analyzed': len(signals),
            'spread_as_predictor': {
                'avg_increase_before_liq': np.mean(spread_increases),
                'std': np.std(spread_increases),
                'pct_above_1.5x': sum(1 for x in spread_increases if x > 1.5) / len(spread_increases),
            },
            'depth_as_predictor': {
                'avg_decrease_ratio': np.mean(depth_decreases),
                'std': np.std(depth_decreases),
                'pct_above_1.2x': sum(1 for x in depth_decreases if x > 1.2) / len(depth_decreases),
            },
            'side_specific': {
                'BUY_liq_ask_depth_impact': np.mean([s['ask_depth_ratio'] for s in buy_liqs]) if buy_liqs else None,
                'SELL_liq_bid_depth_impact': np.mean([s['bid_depth_ratio'] for s in sell_liqs]) if sell_liqs else None,
            },
            'recommendation': {
                'best_predictor': 'spread_increase' if np.mean(spread_increases) > np.mean(depth_decreases) else 'depth_decrease',
                'threshold_suggestion': {
                    'spread_increase': np.mean(spread_increases) + np.std(spread_increases),
                    'depth_decrease': np.mean(depth_decreases) + np.std(depth_decreases),
                }
            }
        }
    
    def export_analysis(self, filepath: str):
        """분석 결과 내보내기"""
        result = {
            'liquidation_impact': self.analyze_liquidation_impact(),
            'pre_liquidation_signals': self.analyze_pre_liquidation_signals(),
            'current_stability_metrics': self.get_stability_metrics(),
        }
        
        with open(filepath, 'w') as f:
            json.dump(result, f, indent=2, default=str)


def test_ar_anomaly_detection():
    """AR 기반 Anomaly Detection 테스트"""
    stats = RollingStats(window_size=50)
    
    # 정상 상태 시뮬레이션 (약간의 노이즈가 있는 안정적 시계열)
    np.random.seed(42)
    for i in range(50):
        value = 100 + np.random.normal(0, 5)  # 평균 100, std 5
        stats.add(value, i * 1000)
    
    print("=== AR Anomaly Detection Test ===")
    print(f"Mean: {stats.get_mean():.2f}")
    print(f"Std: {stats.get_std():.2f}")
    print(f"AR(1) coefficient: {stats.get_ar1_coefficient():.4f}")
    print(f"AR(1) residual std: {stats.get_ar1_residual_std():.4f}")
    
    # 정상 값 테스트
    normal_value = 105
    is_anomaly, z = stats.is_anomaly(normal_value)
    print(f"\nNormal value ({normal_value}): anomaly={is_anomaly}, z={z:.2f}")
    
    # 이상 값 테스트
    anomaly_value = 150
    is_anomaly, z = stats.is_anomaly(anomaly_value)
    print(f"Anomaly value ({anomaly_value}): anomaly={is_anomaly}, z={z:.2f}")


if __name__ == "__main__":
    test_ar_anomaly_detection()