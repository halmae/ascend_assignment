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
    
    # Drop 통계 (stale 이벤트)
    events_dropped: int = 0
    orderbook_dropped: int = 0
    trades_dropped: int = 0
    
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
    
    # Lateness 통계 (이벤트 타입별)
    lateness_stats: Dict[str, Dict] = field(default_factory=dict)
    avg_lateness_by_type: Dict[str, float] = field(default_factory=dict)
    
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
    def orderbook_drop_rate(self) -> float:
        total = self.total_orderbook_updates + self.orderbook_dropped
        return self.orderbook_dropped / total if total > 0 else 0.0
    
    @property
    def trade_drop_rate(self) -> float:
        total = self.total_trades + self.trades_dropped
        return self.trades_dropped / total if total > 0 else 0.0
    
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
    
    def get_check_fail_rate(self, check_name: str) -> float:
        passes = self.check_passes.get(check_name, 0)
        fails = self.check_failures.get(check_name, 0)
        total = passes + fails
        return fails / total if total > 0 else 0.0
    
    def get_avg_lateness(self, event_type: str) -> float:
        return self.avg_lateness_by_type.get(event_type, 0.0)
    
    def get_max_lateness(self, event_type: str) -> float:
        if event_type in self.lateness_stats:
            return self.lateness_stats[event_type].get('max_ms', 0.0)
        return 0.0
    
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
            'orderbook_drop_rate': self.orderbook_drop_rate,
            'trade_drop_rate': self.trade_drop_rate,
            'check_failures': self.check_failures,
            'state_counts': self.state_counts,
            'lateness_stats': self.lateness_stats,
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
        
        print(f"\n[Event Drop (Stale 이벤트)]")
        print(f"  Orderbook dropped: {self.orderbook_dropped:,} ({self.orderbook_drop_rate:.2%})")
        print(f"  Trades dropped: {self.trades_dropped:,} ({self.trade_drop_rate:.2%})")
        
        print(f"\n[Event Lateness (ms)]")
        for event_type in ['orderbook', 'trade', 'ticker']:
            avg = self.get_avg_lateness(event_type)
            max_val = self.get_max_lateness(event_type)
            print(f"  {event_type}: avg={avg:.2f}, max={max_val:.2f}")
        
        print(f"\n[Trade Validation]")
        print(f"  Accept: {self.trade_accepts:,} ({self.trade_accept_rate:.1%})")
        print(f"  Quarantine: {self.trade_quarantines:,} ({self.trade_quarantine_rate:.1%})")
        
        print(f"\n[Consistency Check 실패율] (Effective Orderbook 기준)")
        for check_name in self.check_failures.keys():
            fail_rate = self.get_check_fail_rate(check_name)
            fails = self.check_failures[check_name]
            emoji = "✅" if fail_rate < 0.01 else "⚠️" if fail_rate < 0.05 else "❌"
            print(f"  {emoji} {check_name}: {fails:,} fails ({fail_rate:.2%})")
        
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
    print(f"\n{'지표':<40} {'Research':>12} {'Validation':>12} {'차이':>10}")
    print(f"{'-'*75}")
    
    # Trade Validation
    print(f"{'Trade Accept Rate':<40} {research.trade_accept_rate:>11.1%} {validation.trade_accept_rate:>11.1%} {validation.trade_accept_rate - research.trade_accept_rate:>+9.1%}")
    print(f"{'Trade Quarantine Rate':<40} {research.trade_quarantine_rate:>11.1%} {validation.trade_quarantine_rate:>11.1%} {validation.trade_quarantine_rate - research.trade_quarantine_rate:>+9.1%}")
    
    print(f"{'-'*75}")
    
    # Drop Rate
    print(f"{'Orderbook Drop Rate (stale)':<40} {research.orderbook_drop_rate:>11.2%} {validation.orderbook_drop_rate:>11.2%} {validation.orderbook_drop_rate - research.orderbook_drop_rate:>+9.2%}")
    print(f"{'Trade Drop Rate (stale)':<40} {research.trade_drop_rate:>11.2%} {validation.trade_drop_rate:>11.2%} {validation.trade_drop_rate - research.trade_drop_rate:>+9.2%}")
    
    print(f"{'-'*75}")
    
    # Lateness
    print(f"{'Orderbook Avg Lateness (ms)':<40} {research.get_avg_lateness('orderbook'):>11.2f} {validation.get_avg_lateness('orderbook'):>11.2f} {validation.get_avg_lateness('orderbook') - research.get_avg_lateness('orderbook'):>+9.2f}")
    print(f"{'Orderbook Max Lateness (ms)':<40} {research.get_max_lateness('orderbook'):>11.2f} {validation.get_max_lateness('orderbook'):>11.2f} {validation.get_max_lateness('orderbook') - research.get_max_lateness('orderbook'):>+9.2f}")
    print(f"{'Trade Avg Lateness (ms)':<40} {research.get_avg_lateness('trade'):>11.2f} {validation.get_avg_lateness('trade'):>11.2f} {validation.get_avg_lateness('trade') - research.get_avg_lateness('trade'):>+9.2f}")
    
    print(f"{'-'*75}")
    
    # Consistency Check 실패율
    for check_name in research.check_failures.keys():
        r_rate = research.get_check_fail_rate(check_name)
        v_rate = validation.get_check_fail_rate(check_name)
        diff = v_rate - r_rate
        print(f"{check_name + ' fail rate':<40} {r_rate:>11.2%} {v_rate:>11.2%} {diff:>+9.2%}")
    
    print(f"{'-'*75}")
    
    # State 분포
    print(f"{'TRUSTED %':<40} {research.trusted_rate:>11.1%} {validation.trusted_rate:>11.1%} {validation.trusted_rate - research.trusted_rate:>+9.1%}")
    print(f"{'DEGRADED %':<40} {research.degraded_rate:>11.1%} {validation.degraded_rate:>11.1%} {validation.degraded_rate - research.degraded_rate:>+9.1%}")
    print(f"{'UNTRUSTED %':<40} {research.untrusted_rate:>11.1%} {validation.untrusted_rate:>11.1%} {validation.untrusted_rate - research.untrusted_rate:>+9.1%}")
    
    print(f"\n{'='*75}")
    
    # 핵심 인사이트
    print(f"\n[핵심 인사이트]")
    
    # Drop rate 차이
    ob_drop_diff = validation.orderbook_drop_rate - research.orderbook_drop_rate
    if ob_drop_diff > 0.01:
        print(f"  ⚠️ Validation에서 Orderbook Drop Rate가 {ob_drop_diff:.2%}p 높음 → Stale 이벤트 많음")
    
    # Quarantine rate 차이
    q_diff = validation.trade_quarantine_rate - research.trade_quarantine_rate
    if q_diff > 0.01:
        print(f"  ⚠️ Validation에서 Trade Quarantine이 {q_diff:.1%}p 높음")
    
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
    
    # Drop rate이 높은데 Consistency가 좋으면
    if validation.orderbook_drop_rate > research.orderbook_drop_rate:
        if validation.trusted_rate >= research.trusted_rate * 0.9:
            print(f"  ✅ Stale 이벤트 필터링으로 Effective OB 품질 유지됨")