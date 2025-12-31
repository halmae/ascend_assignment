"""
Consistency Check Logic
"""
from typing import Dict, Optional
from src.data_types import OrderbookState
from src.orderbook import OrderbookMetrics
from src.config import DEFAULT_CONSISTENCY_CONFIG


class ConsistencyChecker:
    """Ticker 데이터와 Orderbook 상태의 일관성 검증"""

    def __init__(self, config=None):
        """
        Args:
            config: ConsistencyConfig Instance
        """
        self.config = config or DEFAULT_CONSISTENCY_CONFIG

    def check_price_consistency(self, ticker_data: Dict, orderbook: OrderbookState) -> Dict:
        """
        가격 일관성 체크

        Args:
            ticker_data: Ticker 이벤트 데이터
            orderbook: 현재 Orderbook 상태

        Returns:
            {
                'score': float, # 0~1
                'details': {
                    'last_vs_mid': float,
                    'mark_vs_mid': float,
                    'index_vs_mid': float,
                    'mark_vs_index': float
                }
            }
        """
        spread_info = OrderbookMetrics.calculate_spread(orderbook)

        if spread_info is None:
            return {'score': 0.0, 'details': {}}
        
        mid_price = spread_info['mid_price']
        last_price = ticker_data.get('last_price', mid_price)
        mark_price = ticker_data.get('mark_price', mid_price)
        index_price = ticker_data.get('index_price', mid_price)

        def price_diff_pct(p1, p2):
            return abs(p1-p2)/p2 * 100 if p2 > 0 else 0
        
        last_vs_mid = price_diff_pct(last_price, mid_price)
        mark_vs_mid = price_diff_pct(mark_price, mid_price)
        index_vs_mid = price_diff_pct(index_price, mid_price)
        mark_vs_index = price_diff_pct(mark_price, index_price)

        def calc_score(diff, tolerance):
            if diff <= tolerance:
                return 1.0
            else:
                return max(0, 1.0 - (diff-tolerance) / tolerance)
            
        score_last_vs_mid = calc_score(
            last_vs_mid,
            self.config.price_tolerance_last_vs_mid
        )
        score_mark_vs_mid = calc_score(
            mark_vs_mid,
            self.config.price_tolerance_mark_vs_mid
        )
        score_index_vs_mid = calc_score(
            index_vs_mid,
            self.config.price_tolerance_index_vs_mid
        )
        score_mark_vs_index = calc_score(
            mark_vs_index,
            self.config.price_tolerance_mark_vs_index
        )

        # total score
        overall_score = (
            score_last_vs_mid +
            score_mark_vs_mid + 
            score_index_vs_mid +
            score_mark_vs_index
        ) / 4

        return {
            'score': overall_score,
            'details': {
                'last_vs_mid': score_last_vs_mid,
                'mark_vs_mid': score_mark_vs_mid,
                'index_vs_mid': score_index_vs_mid,
                'mark_vs_index': score_mark_vs_index
            }
        }
    

    def check_spread_consistency(self, ticker_data: Dict, orderbook: OrderbookState) -> Dict:
        """
        Spread 일관성 체크

        Returns:
            {
                'score': float, # 0~1
                'spread_bps': float,
                'category': str # 'excellent' | 'normal' | 'wide' | 'very_wide'
            }
        """
        spread_info = OrderbookMetrics.calculate_spread(orderbook)

        if spread_info is None:
            return {'score': 0.0, 'spread_bps': 0, 'category': 'unknown'}
        
        spread_bps = spread_info['relative_spread_bps']

        if spread_bps <= self.config.spread_excellent:
            category='excellent'
            score = 1.0
        elif spread_bps <= self.config.spread_normal:
            category='normal'
            score = 1.0
        elif spread_bps <= self.config.spread_wide:
            category='wide'
            score = 0.8
        elif spread_bps <= self.config.spread_very_wide:
            category='very_wide'
            score=0.5
        else:
            category = 'very_wide'
            score = max(0, 0.5 - (spread_bps - self.config.spread_very_wide) / 100)

        return {
            'score': score,
            'spread_bps': spread_bps,
            'category': category
        }
    

    def check_imbalance_vs_funding(self, ticker_data: Dict, orderbook: OrderbookState) -> Dict:
        """
        Imbalance vs Funding Rate 일관성 체크

        Returns:
            {
                'score': float,    # 0~1
                'funding_rate': float,
                'imbalance': float,
                'aligned': bool
            }
        """
        imbalance_info = OrderbookMetrics.calculate_imbalance(orderbook)
        funding_rate = ticker_data.get('funding_rate', 0)

        volume_imbalance = imbalance_info['volume_imbalance']

        # Funding rate와 imbalance의 방향 일치 여부
        # funding_rate > 0: Long이 많음 -> bullish imbalance 기대
        # funding_rate < 0: Short이 많음 -> bearish imbalance 기대
        
        if funding_rate > 0 and volume_imbalance > 0:
            # 둘다 bullish
            aligned = True
            score = min(1.0, abs(funding_rate) * 100 * abs(volume_imbalance))
        elif funding_rate < 0 and volume_imbalance < 0:
            # 둘다 bearish
            aligned = True
            score = min(1.0, abs(funding_rate) * 100 * abs(volume_imbalance))
        elif abs(funding_rate) < 0.0001:
            # Neutral
            aligned = True
            score = 1.0  - abs(volume_imbalance) * 0.5
        else:
            aligned = False
            score = 0.5

        return {
            'score': score,
            'funding_rate': funding_rate,
            'imbalance': volume_imbalance,
            'aligned': aligned
        }
    

    def check_depth_adequacy(self, orderbook: OrderbookState) -> Dict:
        """
        Depth 충분성 체크

        Returns:
            {
                'score': float,   # 0~1
                'bid_depth': float,
                'ask_depth': float,
                'adequate': bool
            }
        """
        depth_info = OrderbookMetrics.calculate_depth(orderbook)

        bid_depth = depth_info['bid_depth']
        ask_depth = depth_info['ask_depth']

        # 최소 depth 기준
        min_depth = self.config.min_depth_btc

        # Bid/Ask 각각 평가
        bid_score = min(1.0, bid_depth / min_depth) if min_depth > 0 else 0
        ask_score = min(1.0, ask_depth / min_depth) if min_depth > 0 else 0

        # Bid/Ask 균형 평가
        total_depth = bid_depth + ask_depth
        if total_depth > 0:
            balance = 1 - abs(bid_depth - ask_depth) / total_depth
        else:
            balance = 0

        # 전체 점수
        score = (bid_score + ask_score) / 2 * balance

        adequate = (bid_depth >= min_depth and ask_depth >= min_depth)

        return {
            'score': score,
            'bid_depth': bid_depth,
            'ask_depth': ask_depth,
            'adequate': adequate
        }
    

    def check_system_error_rate(self, 
                                total_events: int,
                                repairs: int,
                                quarantines: int) -> Dict:
        """
        시스템 에러율 체크
        
        Returns:
            {
                'score': float,  # 0~1
                'error_rate': float,
                'repairs': int,
                'quarantines': int
            }
        """
        if total_events == 0:
            return {
                'score': 1.0,
                'error_rate': 0.0,
                'repairs': 0,
                'quarantines': 0
            }
        
        error_rate = (repairs + quarantines) / total_events
        
        # 에러율이 낮을수록 높은 점수
        # 에러율 5% 이하: 1.0
        # 에러율 10% 이상: 0.5 이하
        score = max(0, 1.0 - error_rate * 5)
        
        return {
            'score': score,
            'error_rate': error_rate,
            'repairs': repairs,
            'quarantines': quarantines
        }
    
    def check_overall_consistency(self,
                                  ticker_data: Dict,
                                  orderbook: OrderbookState,
                                  total_events: int,
                                  repairs: int,
                                  quarantines: int) -> Dict:
        """
        전체 일관성 체크 (5가지 차원 통합)
        
        Returns:
            {
                'overall_score': float,  # 0~1
                'price': Dict,
                'spread': Dict,
                'imbalance_funding': Dict,
                'depth': Dict,
                'system': Dict
            }
        """
        # 5가지 차원 체크
        price_check = self.check_price_consistency(ticker_data, orderbook)
        spread_check = self.check_spread_consistency(ticker_data, orderbook)
        imbalance_check = self.check_imbalance_vs_funding(ticker_data, orderbook)
        depth_check = self.check_depth_adequacy(orderbook)
        system_check = self.check_system_error_rate(total_events, repairs, quarantines)
        
        # 가중 평균
        weights = self.config.weights
        overall_score = (
            weights['price'] * price_check['score'] +
            weights['spread'] * spread_check['score'] +
            weights['imbalance_funding'] * imbalance_check['score'] +
            weights['depth'] * depth_check['score'] +
            weights['system'] * system_check['score']
        )
        
        return {
            'overall_score': overall_score,
            'price': price_check,
            'spread': spread_check,
            'imbalance_funding': imbalance_check,
            'depth': depth_check,
            'system': system_check
        }