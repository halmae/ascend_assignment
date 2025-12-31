"""
Enum Definition
"""

from enum import Enum

class EventType(Enum):
    TRADE = "trade"
    ORDERBOOK = "orderbook"
    LIQUIDATION = "liquidation"
    TICKER = "ticker"


class DataTrustState(Enum):
    TRUSTED = "TRUSTED"
    DEGRADED = "DEGRADED"
    UNTRUSTED = "UNTRUSTED"


class HypothesisValidityState(Enum):
    VALID = "VALID"
    WEAKENING = "WEAKENING"
    INVALID = "INVALID"


class DecisionPermissionState(Enum):
    ALLOWED = "ALLOWED"
    RESTRICTED = "RESTRICTED"
    HALTED = "HALTED"


class RepairAction(Enum):
    ACCEPT = "ACCEPT"
    REPAIR = "REPAIR"
    QUARANTINE = "QUARANTINE"