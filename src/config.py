"""
Configuration - 모든 중요 파라미터를 한 곳에서 관리

================================================================================
Single Decision Engine 원칙:
- Historical과 Realtime에서 동일한 파라미터 사용
- 이 파일의 값을 변경하면 양쪽 모두에 자동 적용
================================================================================

개선사항 (v2):
1. Time Alignment Policy (과제 6.2) 추가
2. Sanitization Policy 강화 (음수 latency, imbalance-funding 불일치)
3. Stability를 z-score 기반으로 변경
4. Liquidation cooldown 제거 (→ 추후 Orderbook Health로 대체)
5. Spread 별도 파라미터 제거 (→ Stability에 통합)
================================================================================
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
    
    구조:
    ├── Time Alignment Policy (과제 6.2)
    │   ├── allowed_lateness
    │   ├── buffer
    │   └── window / watermark
    │
    ├── Data Trust (데이터 신뢰도)
    │   ├── Freshness: 데이터 신선도
    │   └── Integrity: Sanitization Policy (과제 6.3)
    │
    └── Hypothesis Validity (가설 유효성)
        └── Stability: "이 trade가 현재 시장에서 발생 가능한가?"
    
    ※ Liquidation cooldown 제거 → 추후 Orderbook Health로 대체
    ※ Spread 별도 파라미터 제거 → Stability에 통합
    """
    
    # =========================================================================
    # TIME ALIGNMENT POLICY (과제 6.2)
    # =========================================================================
    # event-time vs processing-time 정렬 정책
    
    # Allowed Lateness: 이 값 초과하면 "late" 이벤트
    # late 이벤트는 처리하되, freshness 계산에 페널티
    allowed_lateness_ms: float = 100.0
    
    # Buffer: out-of-order 이벤트 재정렬 대기 시간
    # 이 시간 내에 도착한 이벤트는 순서 재정렬
    buffer_duration_ms: float = 50.0
    
    # Window: 집계 윈도우 크기
    # Freshness, Integrity 등 계산 시 사용
    window_size_ms: float = 1000.0
    
    # Watermark: "이 시점 이전 이벤트는 더 이상 안 옴" 기준
    # watermark = max_event_time - watermark_delay
    # watermark 이전 이벤트가 도착하면 → QUARANTINE
    watermark_delay_ms: float = 200.0
    
    # =========================================================================
    # DATA TRUST - Freshness (데이터 신선도)
    # =========================================================================
    
    # TRUSTED: avg_latency <= 이 값
    freshness_trusted_latency_ms: float = 20.0
    
    # DEGRADED: avg_latency <= 이 값 (초과하면 UNTRUSTED)
    freshness_degraded_latency_ms: float = 50.0
    
    # TRUSTED: stale_ratio <= 이 값
    freshness_trusted_stale_ratio: float = 0.05
    
    # DEGRADED: stale_ratio <= 이 값 (초과하면 UNTRUSTED)
    freshness_degraded_stale_ratio: float = 0.15
    
    # =========================================================================
    # DATA TRUST - Integrity / Sanitization Policy (과제 6.3)
    # =========================================================================
    # 
    # Sanitization 분류:
    #   ACCEPT: 정상 데이터
    #   REPAIR: 수정 가능한 데이터 (minor issue)
    #   QUARANTINE: 신뢰 불가 데이터 → UNTRUSTED
    #
    # QUARANTINE 조건:
    #   1. 음수 latency (시간 역전)
    #   2. Watermark 이전 이벤트
    #   3. Crossed market + high deviation
    #   4. Imbalance-Funding 방향 불일치 (심각한 경우)
    # =========================================================================
    
    # Crossed market 시 REPAIR vs QUARANTINE 판단
    # deviation > 이 값 (bps) → QUARANTINE
    integrity_repair_threshold_bps: float = 5.0
    
    # Imbalance-Funding 불일치 체크
    # |imbalance| > 이 값 AND sign(imbalance) != sign(funding_rate) → 의심
    imbalance_threshold: float = 0.3
    
    # Funding rate 유의미 판단 기준
    funding_rate_significant: float = 0.0001  # 0.01%
    
    # Imbalance-Funding 불일치 시 QUARANTINE 할지 REPAIR 할지
    # True면 QUARANTINE, False면 REPAIR (경고만)
    imbalance_funding_strict: bool = False
    
    # =========================================================================
    # HYPOTHESIS - Stability (AR(1) 기반 Predictability)
    # =========================================================================
    # 핵심 질문: "spread dynamics가 예측 가능한가?"
    #
    # AR(1) 모델: s_t = φ * s_{t-1} + ε_t
    # fit_quality = 1 - (residual_variance / total_variance)
    #
    # 판단 로직:
    # - fit_quality = None (unknown): 샘플 부족 → VALID (보수적)
    # - fit_quality >= 0.6 AND forecast_error <= 2.5σ → VALID
    # - fit_quality <= 0.25 OR forecast_error >= 4σ → INVALID
    # - 그 외 → WEAKENING (gray zone)
    #
    # ※ φ는 단독 조건으로 사용 안 함 (fit_quality와 조합만)
    # =========================================================================
    
    # 최소 샘플 수 (미만이면 unknown → VALID 처리)
    ar1_min_samples: int = 20
    
    # VALID: fit_quality >= 이 값 AND forecast_error <= 2.5σ
    ar1_fit_quality_valid: float = 0.6
    
    # INVALID: fit_quality <= 이 값 OR forecast_error >= 4σ
    ar1_fit_quality_invalid: float = 0.25
    
    # Forecast error 배수
    ar1_forecast_error_valid_mult: float = 2.5   # ≤ 2.5σ → stable
    ar1_forecast_error_invalid_mult: float = 4.0  # ≥ 4σ → exploding
    
    # =========================================================================
    # 버퍼/윈도우 크기 (샘플 수)
    # =========================================================================
    latency_window_size: int = 1000       # Freshness 계산용
    spread_history_size: int = 100        # AR(1) window size
    integrity_history_size: int = 100     # Integrity failure rate 계산용
    
    # =========================================================================
    # LEGACY (AR(1) 도입으로 더 이상 사용 안 함, 호환성 유지)
    # =========================================================================
    stability_valid_zscore: float = 2.0
    stability_weakening_zscore: float = 3.0
    normal_spread_mean_bps: float = 1.0
    normal_spread_std_bps: float = 0.5
    normal_bid_depth_btc: float = 100.0
    normal_ask_depth_btc: float = 100.0


# 전역 인스턴스 (이것을 import해서 사용)
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
    
    # 청크 크기 (메모리 최적화)
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
    print("📋 Current Thresholds (config.py v2)")
    print("=" * 70)
    
    print("\n[Time Alignment Policy]")
    print(f"  allowed_lateness_ms:     {t.allowed_lateness_ms}")
    print(f"  buffer_duration_ms:      {t.buffer_duration_ms}")
    print(f"  window_size_ms:          {t.window_size_ms}")
    print(f"  watermark_delay_ms:      {t.watermark_delay_ms}")
    
    print("\n[Data Trust - Freshness]")
    print(f"  trusted_latency_ms:      {t.freshness_trusted_latency_ms}")
    print(f"  degraded_latency_ms:     {t.freshness_degraded_latency_ms}")
    print(f"  trusted_stale_ratio:     {t.freshness_trusted_stale_ratio}")
    print(f"  degraded_stale_ratio:    {t.freshness_degraded_stale_ratio}")
    
    print("\n[Data Trust - Integrity/Sanitization]")
    print(f"  repair_threshold_bps:    {t.integrity_repair_threshold_bps}")
    print(f"  imbalance_threshold:     {t.imbalance_threshold}")
    print(f"  funding_rate_significant:{t.funding_rate_significant}")
    print(f"  imbalance_funding_strict:{t.imbalance_funding_strict}")
    
    print("\n[Hypothesis - AR(1) Stability]")
    print(f"  ar1_min_samples:         {t.ar1_min_samples}")
    print(f"  ar1_fit_quality_valid:   {t.ar1_fit_quality_valid}")
    print(f"  ar1_fit_quality_invalid: {t.ar1_fit_quality_invalid}")
    print(f"  ar1_forecast_error_valid:{t.ar1_forecast_error_valid_mult}σ")
    print(f"  ar1_forecast_error_invalid:{t.ar1_forecast_error_invalid_mult}σ")
    
    print("=" * 70)


def get_thresholds_dict() -> dict:
    """임계값을 딕셔너리로 반환 (JSON 저장용)"""
    t = THRESHOLDS
    return {
        'time_alignment': {
            'allowed_lateness_ms': t.allowed_lateness_ms,
            'buffer_duration_ms': t.buffer_duration_ms,
            'window_size_ms': t.window_size_ms,
            'watermark_delay_ms': t.watermark_delay_ms,
        },
        'freshness': {
            'trusted_latency_ms': t.freshness_trusted_latency_ms,
            'degraded_latency_ms': t.freshness_degraded_latency_ms,
            'trusted_stale_ratio': t.freshness_trusted_stale_ratio,
            'degraded_stale_ratio': t.freshness_degraded_stale_ratio,
        },
        'integrity': {
            'repair_threshold_bps': t.integrity_repair_threshold_bps,
            'imbalance_threshold': t.imbalance_threshold,
            'funding_rate_significant': t.funding_rate_significant,
        },
        'ar1_stability': {
            'min_samples': t.ar1_min_samples,
            'fit_quality_valid': t.ar1_fit_quality_valid,
            'fit_quality_invalid': t.ar1_fit_quality_invalid,
            'forecast_error_valid_mult': t.ar1_forecast_error_valid_mult,
            'forecast_error_invalid_mult': t.ar1_forecast_error_invalid_mult,
        },
    }


def update_calibration(spread_mean: float, spread_std: float, 
                       bid_depth: float = None, ask_depth: float = None):
    """
    Research 데이터에서 학습한 calibration 값 업데이트
    
    Usage:
        # Research 분석 후
        update_calibration(spread_mean=1.2, spread_std=0.4)
    """
    THRESHOLDS.normal_spread_mean_bps = spread_mean
    THRESHOLDS.normal_spread_std_bps = spread_std
    
    if bid_depth is not None:
        THRESHOLDS.normal_bid_depth_btc = bid_depth
    if ask_depth is not None:
        THRESHOLDS.normal_ask_depth_btc = ask_depth
    
    print(f"✅ Calibration updated:")
    print(f"   spread_mean_bps: {spread_mean}")
    print(f"   spread_std_bps:  {spread_std}")


# =============================================================================
# 실행 시 설정 출력
# =============================================================================
if __name__ == "__main__":
    print_thresholds()