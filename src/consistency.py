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