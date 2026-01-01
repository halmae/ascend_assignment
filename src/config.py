"""
Configuration - 모든 중요 파라미터를 한 곳에서 관리

================================================================================
Single Decision Engine 원칙:
- Historical과 Realtime에서 동일한 파라미터 사용
- 이 파일의 값을 변경하면 양쪽 모두에 자동 적용
================================================================================

사용법:
    from src.config import THRESHOLDS, HISTORICAL_CONFIG, REALTIME_CONFIG
    
    # 파라미터 변경 시뮬레이션
    THRESHOLDS.liquidation_cooldown_ms = 3000  # 5000 → 3000으로 변경
"""
from dataclasses import dataclass, field
from typing import Optional


# =============================================================================
# DECISION THRESHOLDS - State Machine 핵심 파라미터
# =============================================================================

@dataclass
class Thresholds:
    """
    Decision Engine 핵심 임계값
    
    이 값들을 변경하면 Historical/Realtime 모두에 적용됨!
    
    구조:
    ├── Data Trust (데이터 신뢰도)
    │   ├── Freshness: 데이터 신선도
    │   └── Integrity: 데이터 무결성 (Sanitization)
    │
    └── Hypothesis Validity (가설 유효성)
        ├── Stability: Orderbook 안정성
        ├── Liquidation: 청산 후 쿨다운
        └── Spread: 스프레드 범위
    """
    
    # =========================================================================
    # DATA TRUST - Freshness (데이터 신선도)
    # =========================================================================
    # Latency 기반 신뢰도 판단
    
    # TRUSTED: avg_latency <= 이 값
    freshness_trusted_latency_ms: float = 20.0
    
    # DEGRADED: avg_latency <= 이 값 (초과하면 UNTRUSTED)
    freshness_degraded_latency_ms: float = 50.0
    
    # TRUSTED: stale_ratio <= 이 값
    freshness_trusted_stale_ratio: float = 0.05
    
    # DEGRADED: stale_ratio <= 이 값 (초과하면 UNTRUSTED)
    freshness_degraded_stale_ratio: float = 0.15
    
    # Stale 판단 기준 (이 값 초과하면 stale로 간주)
    stale_threshold_ms: float = 100.0
    
    # =========================================================================
    # DATA TRUST - Integrity (Sanitization Policy)
    # =========================================================================
    # Crossed market 시 REPAIR vs QUARANTINE 판단
    
    # REPAIR: price_deviation <= 이 값 (bps)
    # QUARANTINE: price_deviation > 이 값 → UNTRUSTED
    integrity_repair_threshold_bps: float = 5.0
    
    # 보조 지표: 윈도우 내 failure rate
    integrity_trusted_failure_rate: float = 0.02
    integrity_degraded_failure_rate: float = 0.10
    
    # =========================================================================
    # HYPOTHESIS - Stability (Orderbook 안정성)
    # =========================================================================
    # Spread volatility (CV) 기반 판단
    
    # VALID: spread_volatility <= 이 값
    stability_valid_volatility: float = 0.05
    
    # WEAKENING: spread_volatility <= 이 값 (초과하면 INVALID)
    stability_weakening_volatility: float = 0.15
    
    # =========================================================================
    # HYPOTHESIS - Liquidation Cooldown
    # =========================================================================
    # 대규모 청산 후 안정화 대기 시간
    
    # VALID: time_since_liquidation >= 이 값 (ms)
    liquidation_cooldown_ms: float = 5000.0
    
    # WEAKENING: time_since_liquidation >= 이 값 (ms)
    liquidation_weakening_ms: float = 2000.0
    
    # =========================================================================
    # HYPOTHESIS - Spread
    # =========================================================================
    # Orderbook spread 범위 판단
    
    # VALID: spread <= 이 값 (bps)
    spread_valid_bps: float = 10.0
    
    # WEAKENING: spread <= 이 값 (초과하면 INVALID)
    spread_weakening_bps: float = 30.0
    
    # =========================================================================
    # 버퍼/윈도우 크기
    # =========================================================================
    latency_window_size: int = 1000
    spread_history_size: int = 100
    integrity_history_size: int = 100


# 전역 인스턴스 (이것을 import해서 사용)
THRESHOLDS = Thresholds()


# =============================================================================
# HISTORICAL CONFIG - Phase 1 설정
# =============================================================================

@dataclass
class HistoricalConfig:
    """Phase 1: Historical Validation 설정"""
    
    # 데이터 경로 (기본값)
    research_dir: str = "./data/research"
    validation_dir: str = "./data/validation"
    output_dir: str = "./output"
    
    # 청크 크기 (메모리 최적화)
    orderbook_chunk_size: int = 2_000_000
    trades_chunk_size: int = 500_000
    ticker_chunk_size: int = 20_000
    liquidation_chunk_size: int = 5_000
    
    # 로깅 간격
    log_interval: int = 500_000


HISTORICAL_CONFIG = HistoricalConfig()


# =============================================================================
# REALTIME CONFIG - Phase 2 설정
# =============================================================================

@dataclass
class RealtimeConfig:
    """Phase 2: Realtime Validation 설정"""
    
    # WebSocket
    symbol: str = "btcusdt"
    websocket_url: str = "wss://fstream.binance.com"
    
    # 실행
    duration_sec: int = 60
    output_dir: str = "./output/realtime"
    
    # 로깅 간격
    log_interval: int = 100
    
    def get_stream_uri(self) -> str:
        """Combined Stream URI"""
        streams = [
            f"{self.symbol}@trade",
            f"{self.symbol}@depth@100ms",
            f"{self.symbol}@forceOrder",
            f"{self.symbol}@ticker",
        ]
        return f"{self.websocket_url}/stream?streams={'/'.join(streams)}"


REALTIME_CONFIG = RealtimeConfig()


# =============================================================================
# 헬퍼 함수
# =============================================================================

def print_thresholds():
    """현재 임계값 출력"""
    t = THRESHOLDS
    print("=" * 70)
    print("📋 Current Thresholds (config.py)")
    print("=" * 70)
    
    print("\n[Data Trust - Freshness]")
    print(f"  trusted_latency_ms:      {t.freshness_trusted_latency_ms}")
    print(f"  degraded_latency_ms:     {t.freshness_degraded_latency_ms}")
    print(f"  stale_threshold_ms:      {t.stale_threshold_ms}")
    
    print("\n[Data Trust - Integrity (Sanitization)]")
    print(f"  repair_threshold_bps:    {t.integrity_repair_threshold_bps}")
    
    print("\n[Hypothesis - Stability]")
    print(f"  valid_volatility:        {t.stability_valid_volatility}")
    print(f"  weakening_volatility:    {t.stability_weakening_volatility}")
    
    print("\n[Hypothesis - Liquidation]")
    print(f"  cooldown_ms:             {t.liquidation_cooldown_ms}")
    print(f"  weakening_ms:            {t.liquidation_weakening_ms}")
    
    print("\n[Hypothesis - Spread]")
    print(f"  valid_bps:               {t.spread_valid_bps}")
    print(f"  weakening_bps:           {t.spread_weakening_bps}")
    
    print("=" * 70)


def get_thresholds_dict() -> dict:
    """임계값을 딕셔너리로 반환 (JSON 저장용)"""
    t = THRESHOLDS
    return {
        'freshness': {
            'trusted_latency_ms': t.freshness_trusted_latency_ms,
            'degraded_latency_ms': t.freshness_degraded_latency_ms,
            'stale_threshold_ms': t.stale_threshold_ms,
        },
        'integrity': {
            'repair_threshold_bps': t.integrity_repair_threshold_bps,
        },
        'stability': {
            'valid_volatility': t.stability_valid_volatility,
            'weakening_volatility': t.stability_weakening_volatility,
        },
        'liquidation': {
            'cooldown_ms': t.liquidation_cooldown_ms,
            'weakening_ms': t.liquidation_weakening_ms,
        },
        'spread': {
            'valid_bps': t.spread_valid_bps,
            'weakening_bps': t.spread_weakening_bps,
        },
    }


# =============================================================================
# 실행 시 설정 출력
# =============================================================================
if __name__ == "__main__":
    print_thresholds()
    print("\n[Historical Config]")
    print(f"  output_dir: {HISTORICAL_CONFIG.output_dir}")
    print("\n[Realtime Config]")
    print(f"  symbol: {REALTIME_CONFIG.symbol}")
    print(f"  duration_sec: {REALTIME_CONFIG.duration_sec}")