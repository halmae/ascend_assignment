"""
Defining data types
"""
from dataclasses import dataclass
from typing import Dict, Optional
from src.enums import EventType

@dataclass
class Event:
    """Event Data Class"""
    event_type: EventType
    timestamp: int    # microseconds
    local_timestamp: int
    data: Dict


@dataclass
class OrderbookState:
    """Orderbook State"""
    timestamp: int
    bid_levels: Dict[float, float]    # {price: amount}
    ask_levels: Dict[float, float]

    def get_depth(self, side: str, n: int = 10) -> float:
        """
        Top N levels의 총 depth
        
        Args:
            side: 'bid' or 'ask'
            n: level 수

        Returns:
            총 depth (BTC)
        """
        levels = self.bid_levels if side == 'bid' else self.ask_levels
        sorted_levels = sorted(levels.items(), key=lambda x: x[0], reverse=(side=='bid'))
        return sum(amount for _, amount in sorted_levels[:n])

    def get_best_bid(self) -> Optional[float]:
        """
        Best bid price 반환
        
        Returns:
            최고 매수 호가 (가장 높은 bid price) 또는 None
        """
        if not self.bid_levels:
            return None
        return max(self.bid_levels.keys())
    
    def get_best_ask(self) -> Optional[float]:
        """
        Best ask price 반환
        
        Returns:
            최저 매도 호가 (가장 낮은 ask price) 또는 None
        """
        if not self.ask_levels:
            return None
        return min(self.ask_levels.keys())
    
    def get_mid_price(self) -> Optional[float]:
        """
        Mid price 계산
        
        Returns:
            (best_bid + best_ask) / 2 또는 None
        """
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        
        if best_bid is None or best_ask is None:
            return None
        
        return (best_bid + best_ask) / 2
    
    def get_spread(self) -> Optional[float]:
        """
        Spread 계산
        
        Returns:
            best_ask - best_bid 또는 None
        """
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        
        if best_bid is None or best_ask is None:
            return None
        
        return best_ask - best_bid


    def clone(self, new_timestamp: int) -> 'OrderbookState':
        """
        Orderbook 복사

        Args:
            new_timestamp: 새 timestamp

        Returns:
            복사된 OrderbookState
        """
        return OrderbookState(
            timestamp=new_timestamp,
            bid_levels=self.bid_levels.copy(),
            ask_levels=self.ask_levels.copy()
        )