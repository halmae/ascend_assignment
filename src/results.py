"""
처리 결과 구조화 모듈
Research vs Validation 비교 분석용
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime
import json


@dataclass
class ProcessingResult:
    """처리 결과 요약"""
    
    # 메타 정보
    dataset_name: str = ""  # "research" or "validation"
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
    
    # Consistency Check 결과 (각 체크별 fail 횟수)
    check_failures: Dict[str, int] = field(default_factory=lambda: {
        'orderbook_exists': 0,
        'spread_valid': 0,
        'price_in_spread': 0,
        'depth_balanced': 0,
        'funding_imbalance_aligned': 0
    })
    
    # Consistency Check 결과 (각 체크별 pass 횟수)
    check_passes: Dict[str, int] = field(default_factory=lambda: {
        'orderbook_exists': 0,
        'spread_valid': 0,
        'price_in_spread': 0,
        'depth_balanced': 0,
        'funding_imbalance_aligned': 0
    })
    
    # State 분포
    state_counts: Dict[str, int] = field(default_factory=lambda: {
        'TRUSTED': 0,
        'DEGRADED': 0,
        'UNTRUSTED': 0
    })
    
    # State 전이 기록 (시계열 분석용)
    state_transitions: List[Dict] = field(default_factory=list)
    
    # ====== 계산 속성들 ======
    
    @property
    def trade_accept_rate(self) -> float:
        """Trade 수락률"""
        total = self.trade_accepts + self.trade_quarantines
        return self.trade_accepts / total if total > 0 else 0.0
    
    @property
    def trade_quarantine_rate(self) -> float:
        """Trade 격리률"""
        total = self.trade_accepts + self.trade_quarantines
        return self.trade_quarantines / total if total > 0 else 0.0
    
    @property
    def trusted_rate(self) -> float:
        """TRUSTED 상태 비율"""
        total = sum(self.state_counts.values())
        return self.state_counts['TRUSTED'] / total if total > 0 else 0.0
    
    @property
    def degraded_rate(self) -> float:
        """DEGRADED 상태 비율"""
        total = sum(self.state_counts.values())
        return self.state_counts['DEGRADED'] / total if total > 0 else 0.0
    
    @property
    def untrusted_rate(self) -> float:
        """UNTRUSTED 상태 비율"""
        total = sum(self.state_counts.values())
        return self.state_counts['UNTRUSTED'] / total if total > 0 else 0.0
    
    def get_check_fail_rate(self, check_name: str) -> float:
        """특정 체크의 실패율"""
        passes = self.check_passes.get(check_name, 0)
        fails = self.check_failures.get(check_name, 0)
        total = passes + fails
        return fails / total if total > 0 else 0.0
    
    # ====== 출력 메서드들 ======
    
    def to_dict(self) -> Dict:
        """딕셔너리로 변환"""
        return {
            'dataset_name': self.dataset_name,
            'processing_time_sec': self.processing_time_sec,
            'total_events': self.total_events,
            'total_trades': self.total_trades,
            'total_tickers': self.total_tickers,
            'total_orderbook_updates': self.total_orderbook_updates,
            'total_snapshots': self.total_snapshots,
            'trade_accepts': self.trade_accepts,
            'trade_quarantines': self.trade_quarantines,
            'trade_accept_rate': self.trade_accept_rate,
            'trade_quarantine_rate': self.trade_quarantine_rate,
            'check_failures': self.check_failures,
            'check_passes': self.check_passes,
            'state_counts': self.state_counts,
            'trusted_rate': self.trusted_rate,
            'degraded_rate': self.degraded_rate,
            'untrusted_rate': self.untrusted_rate,
        }
    
    def to_json(self, indent: int = 2) -> str:
        """JSON 문자열로 변환"""
        return json.dumps(self.to_dict(), indent=indent)
    
    def print_summary(self):
        """결과 요약 출력"""
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
        
        print(f"\n[State 분포]")
        print(f"  TRUSTED: {self.state_counts['TRUSTED']:,} ({self.trusted_rate:.1%})")
        print(f"  DEGRADED: {self.state_counts['DEGRADED']:,} ({self.degraded_rate:.1%})")
        print(f"  UNTRUSTED: {self.state_counts['UNTRUSTED']:,} ({self.untrusted_rate:.1%})")


def compare_results(research: ProcessingResult, validation: ProcessingResult):
    """
    Research와 Validation 결과 비교
    
    Args:
        research: Research 데이터 처리 결과
        validation: Validation 데이터 처리 결과
    """
    print(f"\n{'='*70}")
    print(f"📊 Research vs Validation 비교")
    print(f"{'='*70}")
    
    # 헤더
    print(f"\n{'지표':<30} {'Research':>15} {'Validation':>15} {'차이':>10}")
    print(f"{'-'*70}")
    
    # Trade Validation
    print(f"{'Trade Accept Rate':<30} {research.trade_accept_rate:>14.1%} {validation.trade_accept_rate:>14.1%} {validation.trade_accept_rate - research.trade_accept_rate:>+9.1%}")
    print(f"{'Trade Quarantine Rate':<30} {research.trade_quarantine_rate:>14.1%} {validation.trade_quarantine_rate:>14.1%} {validation.trade_quarantine_rate - research.trade_quarantine_rate:>+9.1%}")
    
    print(f"{'-'*70}")
    
    # Consistency Check 실패율
    for check_name in research.check_failures.keys():
        r_rate = research.get_check_fail_rate(check_name)
        v_rate = validation.get_check_fail_rate(check_name)
        diff = v_rate - r_rate
        print(f"{check_name + ' fail rate':<30} {r_rate:>14.2%} {v_rate:>14.2%} {diff:>+9.2%}")
    
    print(f"{'-'*70}")
    
    # State 분포
    print(f"{'TRUSTED %':<30} {research.trusted_rate:>14.1%} {validation.trusted_rate:>14.1%} {validation.trusted_rate - research.trusted_rate:>+9.1%}")
    print(f"{'DEGRADED %':<30} {research.degraded_rate:>14.1%} {validation.degraded_rate:>14.1%} {validation.degraded_rate - research.degraded_rate:>+9.1%}")
    print(f"{'UNTRUSTED %':<30} {research.untrusted_rate:>14.1%} {validation.untrusted_rate:>14.1%} {validation.untrusted_rate - research.untrusted_rate:>+9.1%}")
    
    print(f"\n{'='*70}")
    
    # 핵심 인사이트
    print(f"\n[핵심 인사이트]")
    
    # Quarantine rate 차이
    q_diff = validation.trade_quarantine_rate - research.trade_quarantine_rate
    if q_diff > 0.01:
        print(f"  ⚠️ Validation에서 Trade Quarantine이 {q_diff:.1%}p 높음 → Dirty data 영향 가능성")
    
    # UNTRUSTED 증가
    u_diff = validation.untrusted_rate - research.untrusted_rate
    if u_diff > 0.05:
        print(f"  ⚠️ Validation에서 UNTRUSTED 상태가 {u_diff:.1%}p 높음")
    
    # 가장 많이 실패한 체크
    max_fail_check = max(
        validation.check_failures.keys(),
        key=lambda k: validation.get_check_fail_rate(k)
    )
    max_fail_rate = validation.get_check_fail_rate(max_fail_check)
    if max_fail_rate > 0.01:
        print(f"  ⚠️ 가장 많이 실패한 체크: {max_fail_check} ({max_fail_rate:.2%})")