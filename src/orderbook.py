"""
Calculation for Orderbook Metric
"""
from typing import Dict, Optional
from src.data_types import OrderbookState

class OrderbookMetrics:
    """Orderbook으로부터 계산되는 지표들"""

    @staticmethod
    def calculate_spread(orderbook: OrderbookState) -> Optional[Dict]:
        """
        Spread 계산

        Returns:
            {
                'best_bid': float,
                'best_ask': float,
                'mid_price': float,
                'absolute_spread': float,
                'relative_spread_bps': float
            }
        """
        if not orderbook.bid_levels or not orderbook.ask_levels:
            return None
        
        best_bid = max(orderbook.bid_levels.keys())
        best_ask = min(orderbook.ask_levels.keys())

        mid_price = (best_bid + best_ask) / 2
        absolute_spread = best_ask - best_bid
        relative_spread = (absolute_spread / mid_price) * 100

        return {
            'best_bid': best_bid,
            'best_ask': best_ask,
            'mid_price': mid_price,
            'absolute_spread': absolute_spread,
            'relative_spread_bps': relative_spread * 100   # basis points
        }
    

    @staticmethod
    def calculate_depth(orderbook: OrderbookState, n_levels: int=10) -> Dict:
        """
        Depth 계산
        
        Returns:
            {
                'bid_depth': float,
                'ask_depth': float,
                'total_depth': float,
                'bid_value_used': float,
                'ask_value_used': float
            }
        """
        bid_levels = sorted(
            orderbook.bid_levels.items(),
            key=lambda x: x[0],
            reverse=True
        )[:n_levels]

        ask_levels = sorted(
            orderbook.ask_levels.items(),
            key=lambda x: x[0]
        )[:n_levels]

        bid_depth = sum(amount for _, amount in bid_levels)
        ask_depth = sum(amount for _, amount in ask_levels)

        bid_value = sum(price * amount for price, amount in bid_levels)
        ask_value = sum(price * amount for price, amount in ask_levels)

        return {
            'bid_depth': bid_depth,
            'ask_depth': ask_depth,
            'total_depth': bid_depth + ask_depth,
            'bid_value_usd': bid_value,
            'ask_value_usd': ask_value
        }
    

    @staticmethod
    def calculate_imbalance(orderbook: OrderbookState, n_levels: int=10) -> Dict:
        """
        Orderbook Imbalance 계산

        Returns:
            {
                'volume_imbalance': float,    # -1 ~ 1
                'value_imbalance': float,     # -1 ~ 1
                'pressure': str,              # 'bullish' | 'bearish' | 'neutral'
                'bid_depth': float,
                'ask_depth': float
            }
        """
        depth = OrderbookMetrics.calculate_depth(orderbook, n_levels)

        bid_depth = depth['bid_depth']
        ask_depth = depth['ask_depth']
        bid_value = depth['bid_value_usd']
        ask_value = depth['ask_value_usd']

        # Volume imbalance
        total_volume = bid_depth + ask_depth
        volume_imbalance = (bid_depth - ask_depth) / total_volume if total_volume > 0 else 0

        # Volume imbalance
        total_value = bid_value + ask_value
        value_imbalance = (bid_value - ask_value) / total_value if total_value > 0 else 0

        # Pressure 판단
        if volume_imbalance > 0.1:
            pressure = 'bullish'
        elif volume_imbalance < -0.1:
            pressure = 'bearish'
        else:
            pressure = 'neutral'

        return {
            'volume_imbalance': volume_imbalance,
            'value_imbalance': value_imbalance,
            'pressure': pressure,
            'bid_depth': bid_depth,
            'ask_depth': ask_depth
        }