"""
처리 결과 및 비교 분석 모듈 (v2 - 3-State Architecture 지원)
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from pathlib import Path
import json


@dataclass
class ProcessingResult:
    """처리 결과"""
    
    dataset_name: str = ""
    processing_time_sec: float = 0.0
    
    # 기본 통계
    stats: Dict[str, int] = field(default_factory=dict)
    
    # Tradability 분포 (기존 호환성)
    tradability_counts: Dict[str, int] = field(default_factory=dict)
    
    # State 전이 기록
    state_transitions: List[Dict] = field(default_factory=list)
    
    # Uncertainty 로그
    uncertainty_log: List[Dict] = field(default_factory=list)
    
    # 최종 Uncertainty
    final_uncertainty: Dict = field(default_factory=dict)
    
    # === NEW: 3-State Architecture ===
    decision_counts: Dict[str, int] = field(default_factory=dict)
    decisions_log: List[Dict] = field(default_factory=list)
    state_evaluator_summary: Dict = field(default_factory=dict)
    
    # ====== 계산 속성 ======
    
    @property
    def total_tickers(self) -> int:
        return self.stats.get('ticker_checkpoints', 0)
    
    @property
    def tradable_rate(self) -> float:
        total = sum(self.tradability_counts.values())
        return self.tradability_counts.get('TRADABLE', 0) / total if total > 0 else 0.0
    
    @property
    def restricted_rate(self) -> float:
        total = sum(self.tradability_counts.values())
        return self.tradability_counts.get('RESTRICTED', 0) / total if total > 0 else 0.0
    
    @property
    def not_tradable_rate(self) -> float:
        total = sum(self.tradability_counts.values())
        return self.tradability_counts.get('NOT_TRADABLE', 0) / total if total > 0 else 0.0
    
    @property
    def trade_validity_rate(self) -> float:
        valid = self.stats.get('trades_valid', 0)
        invalid = self.stats.get('trades_invalid', 0)
        total = valid + invalid
        return valid / total if total > 0 else 0.0
    
    # === NEW: 3-State 속성 ===
    @property
    def allowed_rate(self) -> float:
        total = sum(self.decision_counts.values()) if self.decision_counts else 0
        return self.decision_counts.get('ALLOWED', 0) / total if total > 0 else 0.0
    
    @property
    def decision_restricted_rate(self) -> float:
        total = sum(self.decision_counts.values()) if self.decision_counts else 0
        return self.decision_counts.get('RESTRICTED', 0) / total if total > 0 else 0.0
    
    @property
    def halted_rate(self) -> float:
        total = sum(self.decision_counts.values()) if self.decision_counts else 0
        return self.decision_counts.get('HALTED', 0) / total if total > 0 else 0.0
    
    # ====== 출력 ======
    
    def print_summary(self):
        print(f"\n{'='*70}")
        print(f"📊 Effective Orderbook Analysis: {self.dataset_name}")
        print(f"{'='*70}")
        
        print(f"\n[기본 통계]")
        print(f"  처리 시간: {self.processing_time_sec:.2f}초")
        for key, value in self.stats.items():
            print(f"  {key}: {value:,}")
        
        print(f"\n[Trade Validity]")
        valid = self.stats.get('trades_valid', 0)
        invalid = self.stats.get('trades_invalid', 0)
        print(f"  Valid: {valid:,} ({self.trade_validity_rate:.1%})")
        print(f"  Invalid: {invalid:,} ({1-self.trade_validity_rate:.1%})")
        
        # === NEW: 3-State 분포 ===
        if self.decision_counts:
            print(f"\n[3-State Decision 분포]")
            total = sum(self.decision_counts.values())
            for state, count in self.decision_counts.items():
                pct = count / total * 100 if total > 0 else 0
                bar = '█' * int(pct / 2)
                print(f"  {state:15s}: {count:>6,} ({pct:>5.1f}%) {bar}")
        
        # 기존 Tradability (호환성)
        print(f"\n[Tradability 분포 (호환)]")
        total = sum(self.tradability_counts.values())
        for state, count in self.tradability_counts.items():
            pct = count / total * 100 if total > 0 else 0
            bar = '█' * int(pct / 2)
            print(f"  {state:15s}: {count:>6,} ({pct:>5.1f}%) {bar}")
        
        print(f"\n[State 전이 횟수]")
        print(f"  총 전이: {len(self.state_transitions)}회")
        
        # 판단 중단(HALT) 통계
        if self.decisions_log:
            halt_count = sum(1 for d in self.decisions_log if d.get('action') == 'HALT')
            restrict_count = sum(1 for d in self.decisions_log if d.get('action') == 'RESTRICT')
            print(f"  HALT 발생: {halt_count}회")
            print(f"  RESTRICT 발생: {restrict_count}회")
        
        if self.final_uncertainty:
            print(f"\n[최종 상태]")
            u = self.final_uncertainty
            print(f"  Data Trust: {u.get('data_trust', 'N/A')}")
            print(f"  Hypothesis: {u.get('hypothesis', 'N/A')}")
            print(f"  Decision: {u.get('decision', 'N/A')}")
            if 'freshness' in u:
                f = u['freshness']
                print(f"  Freshness: avg={f.get('avg_lateness_ms', 0):.1f}ms, "
                      f"stale_ratio={f.get('stale_ratio', 0):.2%}")
            if 'integrity' in u:
                i = u['integrity']
                print(f"  Integrity: spread_valid={i.get('spread_valid', 'N/A')}, "
                      f"failures={i.get('failure_count', 0)}")
            if 'stability' in u:
                s = u['stability']
                print(f"  Stability: spread_vol={s.get('spread_volatility', 0):.4f}")
    
    def to_json(self, filepath: str):
        """결과를 JSON으로 저장"""
        data = {
            'dataset_name': self.dataset_name,
            'processing_time_sec': self.processing_time_sec,
            'stats': self.stats,
            'tradability_counts': self.tradability_counts,
            'tradable_rate': self.tradable_rate,
            'restricted_rate': self.restricted_rate,
            'not_tradable_rate': self.not_tradable_rate,
            'trade_validity_rate': self.trade_validity_rate,
            # 3-State
            'decision_counts': self.decision_counts,
            'allowed_rate': self.allowed_rate,
            'halted_rate': self.halted_rate,
            # Legacy
            'state_transitions': self.state_transitions,
            'uncertainty_log': self.uncertainty_log,
            'final_uncertainty': self.final_uncertainty
        }
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
    
    def save_outputs(self, output_dir: str):
        """
        과제 요구사항에 맞는 출력 파일 생성
        
        /output/
        ├── state_transitions.jsonl    # 상태 전이 로그
        ├── decisions.jsonl            # 판단 허용/제한/중단 기록
        └── summary.json               # 실행 요약
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # 1. state_transitions.jsonl
        transitions_file = output_path / "state_transitions.jsonl"
        with open(transitions_file, 'w') as f:
            for transition in self.state_transitions:
                f.write(json.dumps(transition) + '\n')
        print(f"  ✅ {transitions_file} ({len(self.state_transitions)} records)")
        
        # 2. decisions.jsonl
        decisions_file = output_path / "decisions.jsonl"
        with open(decisions_file, 'w') as f:
            for decision in self.decisions_log:
                f.write(json.dumps(decision) + '\n')
        print(f"  ✅ {decisions_file} ({len(self.decisions_log)} records)")
        
        # 3. summary.json
        summary_file = output_path / "summary.json"
        summary = {
            'dataset_name': self.dataset_name,
            'processing_time_sec': self.processing_time_sec,
            'stats': self.stats,
            'decision_distribution': {
                'counts': self.decision_counts,
                'rates': {
                    'ALLOWED': self.allowed_rate,
                    'RESTRICTED': self.decision_restricted_rate,
                    'HALTED': self.halted_rate
                }
            },
            'tradability_distribution': {
                'counts': self.tradability_counts,
                'rates': {
                    'TRADABLE': self.tradable_rate,
                    'RESTRICTED': self.restricted_rate,
                    'NOT_TRADABLE': self.not_tradable_rate
                }
            },
            'trade_validity_rate': self.trade_validity_rate,
            'state_transitions_count': len(self.state_transitions),
            'decisions_count': {
                'HALT': sum(1 for d in self.decisions_log if d.get('action') == 'HALT'),
                'RESTRICT': sum(1 for d in self.decisions_log if d.get('action') == 'RESTRICT')
            },
            'final_state': self.final_uncertainty
        }
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"  ✅ {summary_file}")


def compare_results(research: ProcessingResult, validation: ProcessingResult):
    """Research와 Validation 비교"""
    print(f"\n{'='*75}")
    print(f"📊 Effective Orderbook: Research vs Validation")
    print(f"{'='*75}")
    
    print(f"\n{'지표':<40} {'Research':>12} {'Validation':>12} {'차이':>10}")
    print(f"{'-'*75}")
    
    # Trade Validity
    print(f"{'Trade Validity Rate':<40} {research.trade_validity_rate:>11.1%} {validation.trade_validity_rate:>11.1%} {validation.trade_validity_rate - research.trade_validity_rate:>+9.1%}")
    
    print(f"{'-'*75}")
    
    # Tradability
    print(f"{'TRADABLE %':<40} {research.tradable_rate:>11.1%} {validation.tradable_rate:>11.1%} {validation.tradable_rate - research.tradable_rate:>+9.1%}")
    print(f"{'RESTRICTED %':<40} {research.restricted_rate:>11.1%} {validation.restricted_rate:>11.1%} {validation.restricted_rate - research.restricted_rate:>+9.1%}")
    print(f"{'NOT_TRADABLE %':<40} {research.not_tradable_rate:>11.1%} {validation.not_tradable_rate:>11.1%} {validation.not_tradable_rate - research.not_tradable_rate:>+9.1%}")
    
    print(f"{'-'*75}")
    
    # State 전이
    print(f"{'State Transitions':<40} {len(research.state_transitions):>11,} {len(validation.state_transitions):>11,}")
    
    print(f"\n{'='*75}")
    
    # 인사이트
    print(f"\n[핵심 인사이트]")
    
    tv_diff = validation.trade_validity_rate - research.trade_validity_rate
    if tv_diff < -0.05:
        print(f"  ⚠️ Validation에서 Trade Validity가 {-tv_diff:.1%}p 낮음 → Dirty Data 영향")
    
    t_diff = validation.tradable_rate - research.tradable_rate
    if t_diff < -0.05:
        print(f"  ⚠️ Validation에서 TRADABLE이 {-t_diff:.1%}p 낮음 → Uncertainty 증가")
    
    nt_diff = validation.not_tradable_rate - research.not_tradable_rate
    if nt_diff > 0.05:
        print(f"  ⚠️ Validation에서 NOT_TRADABLE이 {nt_diff:.1%}p 높음 → 판단 중단 구간 증가")
    
    if validation.tradable_rate >= 0.8:
        print(f"  ✅ Validation에서도 80% 이상 TRADABLE 유지")