"""
Configuration - 모든 중요 파라미터를 한 곳에서 관리

================================================================================
Single Decision Engine 원칙:
- Historical과 Realtime에서 동일한 파라미터 사용
- 이 파일의 값을 변경하면 양쪽 모두에 자동 적용
================================================================================

v3 변경사항:
- AR(1) 제거 (spread 변동이 너무 작아 무의미)
- Price Volatility (window=75) 기반 Stability로 교체
- EDA 결과: Cohen's d = 0.537 (Medium effect)
================================================================================
"""
from dataclasses import dataclass
from typing import Optional


# =============================================================================
# DECISION THRESHOLDS - State Machine 핵심 파라미터
# =============================================================================

@dataclass
class Thresholds:
    """
    Decision Engine 핵심 임계값
    
    구조:
    ├── Time Alignment Policy (과제 6.2)
    │   ├── buffer: N개의 이벤트를 모아서 정렬
    │   ├── window: 버퍼 내 event-time 범위 제한
    │   ├── watermark: emit된 마지막 event-time
    │   └── allowed_lateness: watermark 이전 허용 범위
    │
    ├── Data Trust (데이터 신뢰도)
    │   ├── Freshness: 데이터 신선도
    │   └── Integrity: Sanitization Policy (과제 6.3)
    │
    └── Hypothesis Validity (가설 유효성)
        └── Stability: Price Volatility 기반
    """
    
    # =========================================================================
    # TIME ALIGNMENT POLICY (과제 6.2)
    # =========================================================================
    # 핵심 개념:
    #   - event-time: 이벤트가 실제 발생한 시간 (서버 timestamp)
    #   - processing-time: 이벤트가 시스템에 도착한 시간 (local_timestamp)
    #   - buffer: N개의 이벤트를 모아서 event-time 기준으로 정렬
    #   - window: 버퍼 내 이벤트의 event-time 범위 제한
    #   - watermark: emit된 마지막 event-time (이 시점 이전은 처리 완료)
    #   - allowed_lateness: watermark 이전이지만 허용할 범위
    #
    # 동작 방식:
    #   1. 이벤트가 도착하면 버퍼에 추가 (event-time 기준 heap)
    #   2. 버퍼가 buffer_max_size에 도달하거나 window 초과 시:
    #      - 가장 오래된 이벤트들을 정렬하여 emit
    #      - watermark = emit된 마지막 event-time
    #   3. event-time < watermark - allowed_lateness인 이벤트는 drop (late)
    # =========================================================================
    
    # 버퍼 설정
    buffer_max_size: int = 1000           # 버퍼에 담을 최대 이벤트 수
    window_size_ms: float = 100.0         # 버퍼 내 event-time 범위 제한 (ms)
    allowed_lateness_ms: float = 10.0     # watermark 이전 이벤트 허용 범위 (ms)
    
    # =========================================================================
    # DATA TRUST - Freshness (데이터 신선도)
    # =========================================================================
    
    freshness_trusted_latency_ms: float = 20.0
    freshness_degraded_latency_ms: float = 50.0
    freshness_trusted_stale_ratio: float = 0.05
    freshness_degraded_stale_ratio: float = 0.15
    
    # =========================================================================
    # DATA TRUST - Integrity / Sanitization Policy (과제 6.3)
    # =========================================================================
    # Crossed Market 3단계 처리:
    #   - < accept_threshold: ACCEPT (시장 노이즈)
    #   - < quarantine_threshold: REPAIR (주의 필요)
    #   - >= quarantine_threshold: QUARANTINE (신뢰 불가)
    #
    # EDA 결과:
    #   Research: 5-36 bps, median 23 bps
    #   Validation: 5-754 bps, median 53 bps, 32%가 100+ bps
    # =========================================================================
    
    crossed_accept_threshold_bps: float = 10.0     # 미만: ACCEPT
    crossed_quarantine_threshold_bps: float = 30.0  # 이상: QUARANTINE, 미만: REPAIR
    
    # Price outside spread threshold
    price_outside_repair_bps: float = 5.0
    price_outside_quarantine_bps: float = 10.0
    
    imbalance_threshold: float = 0.3
    funding_rate_significant: float = 0.0001
    imbalance_funding_strict: bool = False
    
    # =========================================================================
    # HYPOTHESIS - Stability (Price Volatility 기반)
    # =========================================================================
    # 핵심 질문: "현재 시장이 안정적인가?"
    #
    # 지표: Rolling Price Volatility (bps)
    #   - mid_price의 변화율(returns)의 rolling std
    #   - window=75 (EDA 결과 최적)
    #
    # 판단 로직 (EDA 결과 기반):
    #   - 평상시: mean=0.276, std=0.187, p90=0.498, p95=0.617
    #   - Liq 근처: mean=0.404, std=0.282
    #
    #   - volatility <= p90 (0.50) → VALID
    #   - volatility <= p95 (0.62) → WEAKENING
    #   - volatility > p95 → INVALID
    #
    # ※ unknown (샘플 부족) → VALID (보수적)
    # =========================================================================
    
    # Window size (EDA 결과: 75가 최적)
    volatility_window_size: int = 75
    
    # 최소 샘플 수 (미만이면 unknown → VALID)
    volatility_min_samples: int = 20
    
    # VALID: volatility <= 이 값 (평상시 p90)
    volatility_valid_threshold: float = 0.50
    
    # WEAKENING: volatility <= 이 값 (평상시 p95)
    volatility_weakening_threshold: float = 0.62
    
    # INVALID: volatility > volatility_weakening_threshold
    
    # =========================================================================
    # 버퍼/윈도우 크기 (샘플 수)
    # =========================================================================
    latency_window_size: int = 1000
    integrity_history_size: int = 100


# 전역 인스턴스
THRESHOLDS = Thresholds()


# =============================================================================
# HISTORICAL CONFIG - Phase 1 설정
# =============================================================================

@dataclass
class HistoricalConfig:
    """Phase 1: Historical Validation 설정"""
    
    research_dir: str = "./data/research"
    validation_dir: str = "./data/validation"
    output_dir: str = "./output"
    
    orderbook_chunk_size: int = 2_000_000
    trades_chunk_size: int = 500_000
    ticker_chunk_size: int = 20_000
    liquidation_chunk_size: int = 5_000
    
    log_interval: int = 500_000


HISTORICAL_CONFIG = HistoricalConfig()


# =============================================================================
# REALTIME CONFIG - Phase 2 설정
# =============================================================================

@dataclass
class RealtimeConfig:
    """Phase 2: Realtime Validation 설정"""
    
    symbol: str = "btcusdt"
    websocket_url: str = "wss://fstream.binance.com"
    
    duration_sec: int = 60
    output_dir: str = "./output/realtime"
    
    log_interval: int = 100
    
    def get_stream_uri(self) -> str:
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
    print("📋 Current Thresholds (config.py v4 - 3-Level Crossed Market)")
    print("=" * 70)
    
    print("\n[Time Alignment Policy]")
    print(f"  buffer_max_size:         {t.buffer_max_size}")
    print(f"  window_size_ms:          {t.window_size_ms}")
    print(f"  allowed_lateness_ms:     {t.allowed_lateness_ms}")
    
    print("\n[Data Trust - Freshness]")
    print(f"  trusted_latency_ms:      {t.freshness_trusted_latency_ms}")
    print(f"  degraded_latency_ms:     {t.freshness_degraded_latency_ms}")
    print(f"  trusted_stale_ratio:     {t.freshness_trusted_stale_ratio}")
    print(f"  degraded_stale_ratio:    {t.freshness_degraded_stale_ratio}")
    
    print("\n[Data Trust - Integrity/Sanitization (3-Level)]")
    print(f"  Crossed Market:")
    print(f"    < {t.crossed_accept_threshold_bps} bps:  ACCEPT (noise)")
    print(f"    < {t.crossed_quarantine_threshold_bps} bps: REPAIR (caution)")
    print(f"    >= {t.crossed_quarantine_threshold_bps} bps: QUARANTINE (untrusted)")
    print(f"  Price Outside Spread:")
    print(f"    < {t.price_outside_quarantine_bps} bps:  REPAIR")
    print(f"    >= {t.price_outside_quarantine_bps} bps: QUARANTINE")
    
    print("\n[Hypothesis - Price Volatility Stability]")
    print(f"  volatility_window_size:      {t.volatility_window_size}")
    print(f"  volatility_min_samples:      {t.volatility_min_samples}")
    print(f"  volatility_valid_threshold:  {t.volatility_valid_threshold} bps")
    print(f"  volatility_weakening_threshold: {t.volatility_weakening_threshold} bps")
    
    print("=" * 70)


def get_thresholds_dict() -> dict:
    """임계값을 딕셔너리로 반환 (JSON 저장용)"""
    t = THRESHOLDS
    return {
        'time_alignment': {
            'buffer_max_size': t.buffer_max_size,
            'window_size_ms': t.window_size_ms,
            'allowed_lateness_ms': t.allowed_lateness_ms,
        },
        'freshness': {
            'trusted_latency_ms': t.freshness_trusted_latency_ms,
            'degraded_latency_ms': t.freshness_degraded_latency_ms,
            'trusted_stale_ratio': t.freshness_trusted_stale_ratio,
            'degraded_stale_ratio': t.freshness_degraded_stale_ratio,
        },
        'integrity': {
            'crossed_accept_threshold_bps': t.crossed_accept_threshold_bps,
            'crossed_quarantine_threshold_bps': t.crossed_quarantine_threshold_bps,
            'price_outside_quarantine_bps': t.price_outside_quarantine_bps,
            'imbalance_threshold': t.imbalance_threshold,
            'funding_rate_significant': t.funding_rate_significant,
        },
        'volatility_stability': {
            'window_size': t.volatility_window_size,
            'min_samples': t.volatility_min_samples,
            'valid_threshold': t.volatility_valid_threshold,
            'weakening_threshold': t.volatility_weakening_threshold,
        },
    }


# =============================================================================
# 실행 시 설정 출력
# =============================================================================
if __name__ == "__main__":
    print_thresholds()