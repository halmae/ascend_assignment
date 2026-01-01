"""
Enumerations - 시스템에서 사용하는 상태 정의
"""
from enum import Enum


class EventType(Enum):
    """이벤트 타입"""
    TRADE = "trade"
    ORDERBOOK = "orderbook"
    TICKER = "ticker"
    LIQUIDATION = "liquidation"
    SNAPSHOT = "snapshot"


class DataTrustState(Enum):
    """
    Data Trust 상태
    
    데이터를 얼마나 신뢰할 수 있는가?
    """
    TRUSTED = "TRUSTED"         # 완전 신뢰
    DEGRADED = "DEGRADED"       # 부분 신뢰 (주의 필요)
    UNTRUSTED = "UNTRUSTED"     # 신뢰 불가


class HypothesisState(Enum):
    """
    Hypothesis Validity 상태
    
    리서치 가설이 현재 유효한가?
    """
    VALID = "VALID"             # 가설 유효
    WEAKENING = "WEAKENING"     # 가설 약화 중
    INVALID = "INVALID"         # 가설 무효


class DecisionPermissionState(Enum):
    """
    Decision Permission 상태
    
    Data Trust + Hypothesis 조합으로 결정
    """
    ALLOWED = "ALLOWED"         # 판단 허용
    RESTRICTED = "RESTRICTED"   # 판단 제한 (주의)
    HALTED = "HALTED"           # 판단 중단


class SanitizationState(Enum):
    """
    Sanitization Policy 상태
    
    데이터 품질 분류
    """
    ACCEPT = "ACCEPT"           # 정상 데이터
    REPAIR = "REPAIR"           # 수정 가능 데이터
    QUARANTINE = "QUARANTINE"   # 신뢰 불가 데이터