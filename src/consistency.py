"""
Consistency Checker - Integrity Uncertainty 측정

"이 trade는 현재 Orderbook 상태에서 발생 가능한 trade인가?"

세 가지 consistency metric:
1. Spread Validity: Orderbook이 crossed market이 아닌가?
2. Price-in-Spread: Last price가 mid price 근처인가?
3. Funding-Imbalance Alignment: Funding과 Imbalance 부호가 일치하는가?
"""
from typing import Dict, Optional
from src.uncertainty import IntegrityUncertainty


class ConsistencyChecker:
    """
    Consistency Checker
    
    Effective Orderbook이 "서로 모순되지 않는 시장 상태"를 표현하는지 검증
    """
    
    def check_integrity(self,
                        ticker_data: Dict,
                        orderbook: Optional['OrderbookState']) -> IntegrityUncertainty:
        """
        Integrity Uncertainty 측정
        
        Returns:
            IntegrityUncertainty with current check results
        """
        integrity = IntegrityUncertainty()
        
        # Orderbook이 없으면 모든 체크 실패
        if orderbook is None or not orderbook.bid_levels or not orderbook.ask_levels:
            integrity.spread_valid = False
            integrity.price_in_spread = False
            integrity.funding_imbalance_aligned = False
            return integrity
        
        # 1. Spread Validity (Crossed Market Check)
        integrity.spread_valid = self._check_spread_valid(orderbook)
        
        # 2. Price-in-Spread
        integrity.price_in_spread = self._check_price_in_spread(ticker_data, orderbook)
        
        # 3. Funding-Imbalance Alignment
        integrity.funding_imbalance_aligned = self._check_funding_imbalance(ticker_data, orderbook)
        
        return integrity
    
    
    def _check_spread_valid(self, orderbook: 'OrderbookState') -> bool:
        """
        Spread Validity: Orderbook이 crossed market이 아닌가?
        
        조건: best_bid < best_ask
        위반 시, Orderbook 구조 자체가 시장 미시구조를 위반한 상태
        """
        best_bid = max(orderbook.bid_levels.keys())
        best_ask = min(orderbook.ask_levels.keys())
        
        return best_bid < best_ask
    
    
    def _check_price_in_spread(self,
                                ticker_data: Dict,
                                orderbook: 'OrderbookState') -> bool:
        """
        Price-in-Spread: Ticker의 last price가 Orderbook의 mid price 근처인가?
        
        이는 Trade/Ticker가 Orderbook reference frame 내에서 설명 가능한지를 검증
        """
        last_price = ticker_data.get('last_price')
        if last_price is None:
            return True  # 데이터 없으면 skip
        
        best_bid = max(orderbook.bid_levels.keys())
        best_ask = min(orderbook.ask_levels.keys())
        
        if best_bid >= best_ask:
            return False  # Crossed market
        
        mid_price = (best_bid + best_ask) / 2
        spread = best_ask - best_bid
        
        # Threshold: spread * 10 또는 10bp 중 큰 값
        relative_threshold = spread * 10
        absolute_threshold = mid_price * 0.001  # 10bp
        threshold = max(relative_threshold, absolute_threshold)
        
        deviation = abs(last_price - mid_price)
        
        return deviation <= threshold
    
    
    def _check_funding_imbalance(self,
                                  ticker_data: Dict,
                                  orderbook: 'OrderbookState') -> bool:
        """
        Funding-Imbalance Alignment: Funding rate과 Orderbook imbalance의 부호가 일치하는가?
        
        논리:
        - Funding > 0 → 롱 포지션 많음 → bid pressure 예상 (imbalance > 0)
        - Funding < 0 → 숏 포지션 많음 → ask pressure 예상 (imbalance < 0)
        """
        funding_rate = ticker_data.get('funding_rate')
        if funding_rate is None:
            return True  # 데이터 없으면 skip
        
        # Orderbook imbalance 계산
        bid_depth = sum(orderbook.bid_levels.values())
        ask_depth = sum(orderbook.ask_levels.values())
        total = bid_depth + ask_depth
        
        if total == 0:
            return True
        
        imbalance = (bid_depth - ask_depth) / total
        
        # 중립 판정 기준
        funding_neutral = abs(funding_rate) < 0.0001  # 0.01% 미만
        imbalance_neutral = abs(imbalance) < 0.1  # 10% 미만
        
        # 둘 다 중립이거나 하나만 중립이면 PASS
        if funding_neutral or imbalance_neutral:
            return True
        
        # 둘 다 강한 신호 - 부호 비교
        funding_positive = funding_rate > 0
        imbalance_positive = imbalance > 0
        
        return funding_positive == imbalance_positive