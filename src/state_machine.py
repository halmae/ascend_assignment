from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any

class DataTrustState(Enum):
    TRUSTED = "TRUSTED"
    DEGRADED = "DEGRADED"
    UNTRUSTED = "UNTRUSTED"

class HypothesisState(Enum):
    VALID = "VALID"
    WEAKENING = "WEAKENING"
    INVALID = "INVALID"

class DecisionPermission(Enum):
    ALLOWED = "ALLOWED"
    RESTRICTED = "RESTRICTED"
    HALTED = "HALTED"

@dataclass
class SystemState:
    """
    Docstring for SystemState
    """
    timestamp: datetime
    data_trust: DataTrustState
    hypothesis: HypothesisState
    decision: DecisionPermission
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp' : self.timestamp.isoformat(),
            'data_trust' : self.data_trust.value,
            'hypothesis' : self.hypothesis.value,
            'decision' : self.decision.value,
            'reason' : self.reason
        }
    

class StateMachine:

    def __init__(
            self,
            window: int = 3000,    # 3 sec window
            lateness: int = 1000,  # 1 sec lateness
            buffer: int = 100, # 100 event buffer
            watermark: int = 50
            ):
        # Time alignment parameter
        self.window = window
        self.lateness = lateness
        self.buffer = buffer
        self.watermark = watermark

        self.current_watermark = None

        self.event_buffer = []

        # Initial state.
        self.data_trust = DataTrustState.TRUSTED
        self.hypothesis = HypothesisState.VALID
        self.decision = DecisionPermission.ALLOWED

        # Statistics
        self.stats = {
            'total_events': 0,
            'accepted': 0,
            'repaired': 0,
            'quarantined': 0,
            'out_of_order': 0,
            'late_arrivals': 0
        }

        self.state_history = []

    
    def process_event(self, event: Dict[str, Any]) -> SystemState:
        """
        이벤트를 받아서 처리하고 현재 상태 반환

        - Historical : CSV에서 한 줄씩 읽어서 호출
        - Realtime : WebSocket에서 메세지 올 때마다 호출

        Args:
            event : {
                'timestamp': datetime,
                'stream': ''orderbook' | 'trades' | 'liquidations' | 'ticker',
                'data': {...}
            }

        Returns:
            SystemState
        """
        self.stats['total_events'] += 1

        # TODO : Actual Logic

        current_state = SystemState(
            timestamp=event['timestamp'],
            data_trust=self.data_trust,
            hypothesis=self.hypothesis,
            decision=self.decision,
            reason='Basic implementation'
        )

        # Log State
        self.state_history.append(current_state)

        return current_state
    

    def get_current_state(self) -> SystemState:
        return SystemState(
            timestamp=datetime.now(),
            data_trust=self.data_trust,
            hypothesis=self.hypothesis,
            decision=self.decision
        )
    

    def get_statistics(self) -> Dict[str, Any]:
        return self.stats.copy()