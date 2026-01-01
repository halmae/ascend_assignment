"""
Consistency Checker - 데이터 무결성 검사

Orderbook과 Ticker 간의 일관성 검사
"""
from typing import Dict, Optional, Tuple
from src.data_types import OrderbookState
from src.uncertainty import IntegrityUncertainty


class ConsistencyChecker:
    """
    데이터 일관성 검사기
    
    Sanitization Policy 판단을 위한 Integrity 정보 생성
    """
    
    def check_integrity(self,
                        ticker_data: Dict,
                        orderbook: OrderbookState) -> IntegrityUncertainty:
        """
        Integrity 검사 수행
        
        Args:
            ticker_data: {'last_price': float, 'funding_rate': float}
            orderbook: 현재 호가창 상태
        
        Returns:
            IntegrityUncertainty
        """
        integrity = IntegrityUncertainty()
        
        # Spread 유효성 검사 (Crossed market 체크)
        spread_valid, deviation_bps = self._check_spread_validity(orderbook, ticker_data)
        integrity.spread_valid = spread_valid
        integrity.price_deviation_bps = deviation_bps
        
        # Price in spread 검사
        integrity.price_in_spread = self._check_price_in_spread(
            ticker_data.get('last_price'),
            orderbook
        )
        
        # Funding rate imbalance 검사
        funding_rate = ticker_data.get('funding_rate')
        if funding_rate is not None:
            integrity.funding_imbalance = abs(funding_rate) > 0.001  # 0.1%
        
        return integrity
    
    def _check_spread_validity(self, 
                               orderbook: OrderbookState,
                               ticker_data: Dict) -> Tuple[bool, float]:
        """
        Spread 유효성 및 deviation 계산
        
        Returns:
            (spread_valid, deviation_bps)
        """
        if not orderbook.bid_levels or not orderbook.ask_levels:
            return True, 0.0
        
        best_bid = orderbook.get_best_bid()
        best_ask = orderbook.get_best_ask()
        
        if best_bid is None or best_ask is None:
            return True, 0.0
        
        # Crossed market 체크
        is_crossed = best_bid >= best_ask
        
        # Price deviation 계산
        last_price = ticker_data.get('last_price', 0)
        if last_price > 0:
            if is_crossed:
                # Crossed market: mid price를 best_bid와 best_ask 평균으로
                mid_price = (best_bid + best_ask) / 2
            else:
                mid_price = orderbook.get_mid_price()
            
            if mid_price and mid_price > 0:
                deviation_bps = abs(last_price - mid_price) / mid_price * 10000
            else:
                deviation_bps = float('inf')
        else:
            deviation_bps = 0.0
        
        return not is_crossed, deviation_bps
    
    def _check_price_in_spread(self,
                               last_price: Optional[float],
                               orderbook: OrderbookState) -> bool:
        """
        Last price가 spread 안에 있는지 검사
        """
        if last_price is None or last_price <= 0:
            return True
        
        best_bid = orderbook.get_best_bid()
        best_ask = orderbook.get_best_ask()
        
        if best_bid is None or best_ask is None:
            return True
        
        # Crossed market이면 무조건 False
        if best_bid >= best_ask:
            return False
        
        return best_bid <= last_price <= best_ask