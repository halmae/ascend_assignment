"""
Consistency Checker - 데이터 무결성 검사 (v2)

================================================================================
변경사항:
1. Imbalance-Funding 불일치 체크 추가
2. Orderbook depth 계산 추가
================================================================================
"""
from typing import Dict, Optional, Tuple
from src.config import THRESHOLDS
from src.data_types import OrderbookState
from src.uncertainty import IntegrityUncertainty


class ConsistencyChecker:
    """
    데이터 일관성 검사기 (v2)
    
    Sanitization Policy 판단을 위한 Integrity 정보 생성
    """
    
    def __init__(self):
        self.th = THRESHOLDS
    
    def check_integrity(self,
                        ticker_data: Dict,
                        orderbook: OrderbookState) -> IntegrityUncertainty:
        """
        Integrity 검사 수행
        
        체크 항목:
        1. Spread 유효성 (Crossed market)
        2. Price in spread
        3. Price deviation
        4. Imbalance-Funding 불일치 (새로 추가)
        """
        integrity = IntegrityUncertainty()
        
        # 1. Spread 유효성 검사
        spread_valid, deviation_bps = self._check_spread_validity(orderbook, ticker_data)
        integrity.spread_valid = spread_valid
        integrity.price_deviation_bps = deviation_bps
        
        # 2. Price in spread 검사
        integrity.price_in_spread = self._check_price_in_spread(
            ticker_data.get('last_price'),
            orderbook
        )
        
        # 3. Funding rate 저장
        funding_rate = ticker_data.get('funding_rate')
        integrity.funding_rate = funding_rate
        
        # 4. Orderbook imbalance 계산
        imbalance = self._calculate_imbalance(orderbook)
        integrity.orderbook_imbalance = imbalance
        
        # 5. Imbalance-Funding 불일치 체크
        integrity.imbalance_funding_mismatch = self._check_imbalance_funding_mismatch(
            imbalance, funding_rate
        )
        
        # 6. Funding imbalance (기존 호환)
        if funding_rate is not None:
            integrity.funding_imbalance = abs(funding_rate) > 0.001
        
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
    
    def _calculate_imbalance(self, orderbook: OrderbookState) -> float:
        """
        Orderbook imbalance 계산
        
        imbalance = (bid_depth - ask_depth) / (bid_depth + ask_depth)
        
        Returns:
            -1 ~ +1 (양수면 bid 우세, 음수면 ask 우세)
        """
        if not orderbook.bid_levels or not orderbook.ask_levels:
            return 0.0
        
        bid_depth = sum(orderbook.bid_levels.values())
        ask_depth = sum(orderbook.ask_levels.values())
        
        total_depth = bid_depth + ask_depth
        if total_depth == 0:
            return 0.0
        
        return (bid_depth - ask_depth) / total_depth
    
    def _check_imbalance_funding_mismatch(self,
                                          imbalance: float,
                                          funding_rate: Optional[float]) -> bool:
        """
        Imbalance-Funding 불일치 체크
        
        논리:
        - imbalance > 0 (bid 우세) → 매수 압력 → funding_rate 양수 예상
        - imbalance < 0 (ask 우세) → 매도 압력 → funding_rate 음수 예상
        - 방향이 반대면 불일치
        
        조건:
        - |imbalance| > imbalance_threshold
        - |funding_rate| > funding_rate_significant
        - sign(imbalance) != sign(funding_rate)
        
        Returns:
            True if mismatch detected
        """
        if funding_rate is None:
            return False
        
        # 임계값 이하면 의미 없음
        if abs(imbalance) <= self.th.imbalance_threshold:
            return False
        
        if abs(funding_rate) <= self.th.funding_rate_significant:
            return False
        
        # 부호 비교
        imbalance_sign = 1 if imbalance > 0 else -1
        funding_sign = 1 if funding_rate > 0 else -1
        
        # 불일치 = 부호가 다름
        return imbalance_sign != funding_sign