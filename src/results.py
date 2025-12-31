"""
처리 결과 구조화 모듈
Research vs Validation 비교 분석용
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import json


@dataclass
class ProcessingResult:
    """처리 결과 요약"""
    
    # 메타 정보
    dataset_name: str = ""
    processing_time_sec: float = 0.0
    
    # 기본 통계
    total_events: int = 0
    total_trades: int = 0
    total_tickers: int = 0
    total_orderbook_updates: int = 0
    total_snapshots: int = 0
    
    # Trade Validation 결과
    trade_accepts: int = 0
    trade_quarantines: int = 0
    
    # Consistency Check 결과 (3가지)
    check_failures: Dict[str, int] = field(default_factory=lambda: {
        'spread_valid': 0,
        'price_in_spread': 0,
        'funding_imbalance_aligned': 0
    })
    
    check_passes: Dict[str, int] = field(default_factory=lambda: {
        'spread_valid': 0,
        'price_in_spread': 0,
        'funding_imbalance_aligned': 0
    })
    
    # State 분포
    state_counts: Dict[str, int] = field(default_factory=lambda: {
        'TRUSTED': 0,
        'DEGRADED': 0,
        'UNTRUSTED': 0
    })
    
    # State 전이 기록
    state_transitions: List[Dict] = field(default_factory=list)
    
    # Lateness 통계
    lateness_stats: Dict[str, float] = field(default_factory=lambda: {
        'total_checks': 0,
        'within_allowed': 0,
        'degraded_range': 0,
        'exceeded': 0,
        'max_lateness_ms': 0.0,
        'sum_lateness_ms': 0.0,
    })
    avg_lateness_ms: float = 0.0
    
    # ====== 계산 속성들 ======
    
    @property
    def trade_accept_rate(self) -> float:
        total = self.trade_accepts + self.trade_quarantines
        return self.trade_accepts / total if total > 0 else 0.0
    
    @property
    def trade_quarantine_rate(self) -> float:
        total = self.trade_accepts + self.trade_quarantines
        return self.trade_quarantines / total if total > 0 else 0.0
    
    @property
    def trusted_rate(self) -> float:
        total = sum(self.state_counts.values())
        return self.state_counts['TRUSTED'] / total if total > 0 else 0.0
    
    @property
    def degraded_rate(self) -> float:
        total = sum(self.state_counts.values())
        return self.state_counts['DEGRADED'] / total if total > 0 else 0.0
    
    @property
    def untrusted_rate(self) -> float:
        total = sum(self.state_counts.values())
        return self.state_counts['UNTRUSTED'] / total if total > 0 else 0.0
    
    @property
    def lateness_within_allowed_rate(self) -> float:
        total = self.lateness_stats.get('total_checks', 0)
        return self.lateness_stats.get('within_allowed', 0) / total if total > 0 else 0.0
    
    @property
    def lateness_degraded_rate(self) -> float:
        total = self.lateness_stats.get('total_checks', 0)
        return self.lateness_stats.get('degraded_range', 0) / total if total > 0 else 0.0
    
    @property
    def lateness_exceeded_rate(self) -> float:
        total = self.lateness_stats.get('total_checks', 0)
        return self.lateness_stats.get('exceeded', 0) / total if total > 0 else 0.0
    
    def get_check_fail_rate(self, check_name: str) -> float:
        passes = self.check_passes.get(check_name, 0)
        fails = self.check_failures.get(check_name, 0)
        total = passes + fails
        return fails / total if total > 0 else 0.0
    
    # ====== 출력 메서드들 ======
    
    def to_dict(self) -> Dict:
        return {
            'dataset_name': self.dataset_name,
            'processing_time_sec': self.processing_time_sec,
            'total_events': self.total_events,
            'total_trades': self.total_trades,
            'total_tickers': self.total_tickers,
            'trade_accept_rate': self.trade_accept_rate,
            'trade_quarantine_rate': self.trade_quarantine_rate,
            'check_failures': self.check_failures,
            'state_counts': self.state_counts,
            'lateness_stats': self.lateness_stats,
            'avg_lateness_ms': self.avg_lateness_ms,
        }
    
    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)
    
    def print_summary(self):
        print(f"\n{'='*60}")
        print(f"📊 Processing Result: {self.dataset_name}")
        print(f"{'='*60}")
        
        print(f"\n[기본 통계]")
        print(f"  총 이벤트: {self.total_events:,}")
        print(f"  - Trades: {self.total_trades:,}")
        print(f"  - Tickers: {self.total_tickers:,}")
        print(f"  - Orderbook Updates: {self.total_orderbook_updates:,}")
        print(f"  - Snapshots: {self.total_snapshots:,}")
        print(f"  처리 시간: {self.processing_time_sec:.2f}초")
        
        print(f"\n[Trade Validation]")
        print(f"  Accept: {self.trade_accepts:,} ({self.trade_accept_rate:.1%})")
        print(f"  Quarantine: {self.trade_quarantines:,} ({self.trade_quarantine_rate:.1%})")
        
        print(f"\n[Consistency Check 실패율]")
        for check_name in self.check_failures.keys():
            fail_rate = self.get_check_fail_rate(check_name)
            fails = self.check_failures[check_name]
            emoji = "✅" if fail_rate < 0.01 else "⚠️" if fail_rate < 0.05 else "❌"
            print(f"  {emoji} {check_name}: {fails:,} fails ({fail_rate:.2%})")
        
        print(f"\n[Lateness 통계]")
        print(f"  평균: {self.avg_lateness_ms:.2f}ms")
        print(f"  최대: {self.lateness_stats.get('max_lateness_ms', 0):.2f}ms")
        print(f"  정상 (≤50ms): {self.lateness_within_allowed_rate:.1%}")
        print(f"  지연 (50-200ms): {self.lateness_degraded_rate:.1%}")
        print(f"  초과 (>200ms): {self.lateness_exceeded_rate:.1%}")
        
        print(f"\n[State 분포]")
        print(f"  TRUSTED: {self.state_counts['TRUSTED']:,} ({self.trusted_rate:.1%})")
        print(f"  DEGRADED: {self.state_counts['DEGRADED']:,} ({self.degraded_rate:.1%})")
        print(f"  UNTRUSTED: {self.state_counts['UNTRUSTED']:,} ({self.untrusted_rate:.1%})")


def compare_results(research: ProcessingResult, validation: ProcessingResult):
    """Research와 Validation 결과 비교"""
    print(f"\n{'='*70}")
    print(f"📊 Research vs Validation 비교")
    print(f"{'='*70}")
    
    # 헤더
    print(f"\n{'지표':<35} {'Research':>15} {'Validation':>15} {'차이':>10}")
    print(f"{'-'*70}")
    
    # Trade Validation
    print(f"{'Trade Accept Rate':<35} {research.trade_accept_rate:>14.1%} {validation.trade_accept_rate:>14.1%} {validation.trade_accept_rate - research.trade_accept_rate:>+9.1%}")
    print(f"{'Trade Quarantine Rate':<35} {research.trade_quarantine_rate:>14.1%} {validation.trade_quarantine_rate:>14.1%} {validation.trade_quarantine_rate - research.trade_quarantine_rate:>+9.1%}")
    
    print(f"{'-'*70}")
    
    # Consistency Check 실패율
    for check_name in research.check_failures.keys():
        r_rate = research.get_check_fail_rate(check_name)
        v_rate = validation.get_check_fail_rate(check_name)
        diff = v_rate - r_rate
        print(f"{check_name + ' fail rate':<35} {r_rate:>14.2%} {v_rate:>14.2%} {diff:>+9.2%}")
    
    print(f"{'-'*70}")
    
    # Lateness
    print(f"{'Avg Lateness (ms)':<35} {research.avg_lateness_ms:>14.2f} {validation.avg_lateness_ms:>14.2f} {validation.avg_lateness_ms - research.avg_lateness_ms:>+9.2f}")
    print(f"{'Lateness Within Allowed %':<35} {research.lateness_within_allowed_rate:>14.1%} {validation.lateness_within_allowed_rate:>14.1%} {validation.lateness_within_allowed_rate - research.lateness_within_allowed_rate:>+9.1%}")
    print(f"{'Lateness Exceeded %':<35} {research.lateness_exceeded_rate:>14.1%} {validation.lateness_exceeded_rate:>14.1%} {validation.lateness_exceeded_rate - research.lateness_exceeded_rate:>+9.1%}")
    
    print(f"{'-'*70}")
    
    # State 분포
    print(f"{'TRUSTED %':<35} {research.trusted_rate:>14.1%} {validation.trusted_rate:>14.1%} {validation.trusted_rate - research.trusted_rate:>+9.1%}")
    print(f"{'DEGRADED %':<35} {research.degraded_rate:>14.1%} {validation.degraded_rate:>14.1%} {validation.degraded_rate - research.degraded_rate:>+9.1%}")
    print(f"{'UNTRUSTED %':<35} {research.untrusted_rate:>14.1%} {validation.untrusted_rate:>14.1%} {validation.untrusted_rate - research.untrusted_rate:>+9.1%}")
    
    print(f"\n{'='*70}")
    
    # 핵심 인사이트
    print(f"\n[핵심 인사이트]")
    
    # Quarantine rate 차이
    q_diff = validation.trade_quarantine_rate - research.trade_quarantine_rate
    if q_diff > 0.01:
        print(f"  ⚠️ Validation에서 Trade Quarantine이 {q_diff:.1%}p 높음")
    
    # Lateness 차이
    l_diff = validation.lateness_exceeded_rate - research.lateness_exceeded_rate
    if l_diff > 0.01:
        print(f"  ⚠️ Validation에서 Lateness 초과가 {l_diff:.1%}p 높음")
    
    # UNTRUSTED 증가
    u_diff = validation.untrusted_rate - research.untrusted_rate
    if u_diff > 0.01:
        print(f"  ⚠️ Validation에서 UNTRUSTED 상태가 {u_diff:.1%}p 높음")
    
    # 가장 많이 실패한 체크
    max_fail_check = max(
        validation.check_failures.keys(),
        key=lambda k: validation.get_check_fail_rate(k)
    )
    max_fail_rate = validation.get_check_fail_rate(max_fail_check)
    if max_fail_rate > 0.01:
        print(f"  ⚠️ 가장 많이 실패한 체크: {max_fail_check} ({max_fail_rate:.2%})")