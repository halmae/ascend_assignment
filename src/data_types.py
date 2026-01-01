"""
Data Types - 시스템에서 사용하는 데이터 구조
"""
from dataclasses import dataclass
from typing import Dict, Optional
from src.enums import EventType


@dataclass
class Event:
    """이벤트 데이터"""
    event_type: EventType
    timestamp: int          # microseconds (event time)
    local_timestamp: int    # microseconds (processing time)
    data: Dict


@dataclass
class OrderbookState:
    """Orderbook 상태"""
    timestamp: int
    bid_levels: Dict[float, float]    # {price: quantity}
    ask_levels: Dict[float, float]
    
    def get_best_bid(self) -> Optional[float]:
        """최고 매수 호가"""
        return max(self.bid_levels.keys()) if self.bid_levels else None
    
    def get_best_ask(self) -> Optional[float]:
        """최저 매도 호가"""
        return min(self.ask_levels.keys()) if self.ask_levels else None
    
    def get_mid_price(self) -> Optional[float]:
        """중간 가격"""
        bid = self.get_best_bid()
        ask = self.get_best_ask()
        if bid is None or ask is None:
            return None
        return (bid + ask) / 2
    
    def get_spread(self) -> Optional[float]:
        """스프레드 (절대값)"""
        bid = self.get_best_bid()
        ask = self.get_best_ask()
        if bid is None or ask is None:
            return None
        return ask - bid
    
    def get_spread_bps(self) -> Optional[float]:
        """스프레드 (basis points)"""
        spread = self.get_spread()
        mid = self.get_mid_price()
        if spread is None or mid is None or mid == 0:
            return None
        return (spread / mid) * 10000
    
    def is_crossed(self) -> bool:
        """Crossed market 여부"""
        bid = self.get_best_bid()
        ask = self.get_best_ask()
        if bid is None or ask is None:
            return False
        return bid >= ask
    
    def get_depth(self, side: str, n: int = 10) -> float:
        """Top N levels 총 depth"""
        levels = self.bid_levels if side == 'bid' else self.ask_levels
        sorted_levels = sorted(
            levels.items(), 
            key=lambda x: x[0], 
            reverse=(side == 'bid')
        )
        return sum(qty for _, qty in sorted_levels[:n])
    
    def clone(self, new_timestamp: Optional[int] = None) -> 'OrderbookState':
        """복사"""
        return OrderbookState(
            timestamp=new_timestamp or self.timestamp,
            bid_levels=self.bid_levels.copy(),
            ask_levels=self.ask_levels.copy()
        )