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