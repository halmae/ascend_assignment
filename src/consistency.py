"""
단순화된 Consistency Checker

원칙:
- Score 없음, Pass/Fail만
- Tolerance 최소화, 직관적인 체크만
- 각 체크가 명확한 Yes/No 질문
"""
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional


class CheckResult(Enum):
    """체크 결과"""
    PASS = "pass"
    FAIL = "fail"
    SKIP = "skip"  # 데이터 부족으로 체크 불가


@dataclass
class ConsistencyReport:
    """Consistency 체크 결과 리포트"""
    checks: Dict[str, CheckResult]
    
    @property
    def all_passed(self) -> bool:
        """모든 체크가 통과했는가? (SKIP 제외)"""
        return all(
            r == CheckResult.PASS 
            for r in self.checks.values() 
            if r != CheckResult.SKIP
        )
    
    @property
    def any_failed(self) -> bool:
        """하나라도 실패했는가?"""
        return any(r == CheckResult.FAIL for r in self.checks.values())
    
    @property
    def fail_count(self) -> int:
        """실패한 체크 수"""
        return sum(1 for r in self.checks.values() if r == CheckResult.FAIL)
    
    @property
    def pass_count(self) -> int:
        """통과한 체크 수"""
        return sum(1 for r in self.checks.values() if r == CheckResult.PASS)


class ConsistencyChecker:
    """
    단순화된 Consistency Checker
    
    5가지 Binary 체크:
    1. orderbook_exists: Orderbook이 존재하는가?
    2. spread_valid: Crossed market이 아닌가?
    3. price_in_spread: Last price가 합리적 범위에 있는가?
    4. depth_balanced: Bid/Ask 양쪽에 depth가 있는가?
    5. funding_imbalance_aligned: Funding과 Imbalance 부호가 일치하는가?
    """
    
    def check_all(self, 
                  ticker_data: Dict, 
                  orderbook: Optional['OrderbookState']) -> ConsistencyReport:
        """모든 consistency check 수행"""
        
        checks = {}
        
        # 1. Orderbook 존재 여부
        checks['orderbook_exists'] = self._check_orderbook_exists(orderbook)
        
        if checks['orderbook_exists'] == CheckResult.FAIL:
            # Orderbook 없으면 나머지는 SKIP
            checks['spread_valid'] = CheckResult.SKIP
            checks['price_in_spread'] = CheckResult.SKIP
            checks['depth_balanced'] = CheckResult.SKIP
            checks['funding_imbalance_aligned'] = CheckResult.SKIP
            return ConsistencyReport(checks=checks)
        
        # 2. Spread 유효성 (crossed market 체크)
        checks['spread_valid'] = self._check_spread_valid(orderbook)
        
        # 3. Last price 위치
        checks['price_in_spread'] = self._check_price_in_spread(ticker_data, orderbook)
        
        # 4. Depth 존재 여부
        checks['depth_balanced'] = self._check_depth_exists(orderbook)
        
        # 5. Funding-Imbalance 부호 일치
        checks['funding_imbalance_aligned'] = self._check_funding_imbalance_sign(
            ticker_data, orderbook
        )
        
        return ConsistencyReport(checks=checks)
    
    
    def _check_orderbook_exists(self, orderbook: Optional['OrderbookState']) -> CheckResult:
        """
        Orderbook이 존재하고 비어있지 않은가?
        
        FAIL 조건:
        - orderbook이 None
        - bid_levels가 비어있음
        - ask_levels가 비어있음
        """
        if orderbook is None:
            return CheckResult.FAIL
        if not orderbook.bid_levels or not orderbook.ask_levels:
            return CheckResult.FAIL
        return CheckResult.PASS
    
    
    def _check_spread_valid(self, orderbook: 'OrderbookState') -> CheckResult:
        """
        Spread가 유효한가? (Crossed market이 아닌가?)
        
        FAIL 조건:
        - best_bid >= best_ask (crossed market)
        """
        best_bid = max(orderbook.bid_levels.keys())
        best_ask = min(orderbook.ask_levels.keys())
        
        if best_bid >= best_ask:
            return CheckResult.FAIL
        
        return CheckResult.PASS
    
    
    def _check_price_in_spread(self, 
                                ticker_data: Dict, 
                                orderbook: 'OrderbookState') -> CheckResult:
        """
        Last price가 합리적인 범위에 있는가?
        
        로직:
        - best_bid <= last_price <= best_ask 이면 PASS
        - 그 외에는 spread 대비 얼마나 벗어났는지 체크
        
        FAIL 조건:
        - last_price가 spread의 10배 이상 벗어남
        """
        last_price = ticker_data.get('last_price')
        if last_price is None:
            return CheckResult.SKIP
        
        best_bid = max(orderbook.bid_levels.keys())
        best_ask = min(orderbook.ask_levels.keys())
        
        # 정상 범위 내
        if best_bid <= last_price <= best_ask:
            return CheckResult.PASS
        
        # 범위 밖 - 얼마나 벗어났는지 체크
        spread = best_ask - best_bid
        if spread <= 0:
            return CheckResult.FAIL
        
        mid_price = (best_bid + best_ask) / 2
        deviation = abs(last_price - mid_price)
        
        # Spread의 10배 이상 벗어나면 FAIL
        if deviation > spread * 10:
            return CheckResult.FAIL
        
        return CheckResult.PASS
    
    
    def _check_depth_exists(self, orderbook: 'OrderbookState') -> CheckResult:
        """
        Bid와 Ask 양쪽에 depth가 존재하는가?
        
        FAIL 조건:
        - bid_depth == 0
        - ask_depth == 0
        """
        bid_depth = sum(orderbook.bid_levels.values())
        ask_depth = sum(orderbook.ask_levels.values())
        
        if bid_depth <= 0 or ask_depth <= 0:
            return CheckResult.FAIL
        
        return CheckResult.PASS
    
    
    def _check_funding_imbalance_sign(self, 
                                       ticker_data: Dict, 
                                       orderbook: 'OrderbookState') -> CheckResult:
        """
        Funding rate과 Orderbook imbalance의 부호가 일치하는가?
        
        논리:
        - Funding > 0 → 롱 포지션 많음 → bid pressure 예상 (imbalance > 0)
        - Funding < 0 → 숏 포지션 많음 → ask pressure 예상 (imbalance < 0)
        
        FAIL 조건:
        - Funding과 Imbalance의 부호가 반대 (둘 다 강한 신호일 때)
        
        PASS 조건:
        - 부호 일치
        - 둘 중 하나가 중립 (약한 신호)
        """
        funding_rate = ticker_data.get('funding_rate')
        if funding_rate is None:
            return CheckResult.SKIP
        
        # Orderbook imbalance 계산
        bid_depth = sum(orderbook.bid_levels.values())
        ask_depth = sum(orderbook.ask_levels.values())
        total = bid_depth + ask_depth
        
        if total == 0:
            return CheckResult.SKIP
        
        imbalance = (bid_depth - ask_depth) / total  # 양수면 bid 우세
        
        # 중립 판정 기준
        funding_neutral = abs(funding_rate) < 0.0001  # 0.01% 미만
        imbalance_neutral = abs(imbalance) < 0.1  # 10% 미만
        
        # 둘 다 중립이면 PASS
        if funding_neutral and imbalance_neutral:
            return CheckResult.PASS
        
        # 하나만 중립이면 PASS (약한 신호는 무시)
        if funding_neutral or imbalance_neutral:
            return CheckResult.PASS
        
        # 둘 다 강한 신호 - 부호 비교
        funding_positive = funding_rate > 0
        imbalance_positive = imbalance > 0
        
        if funding_positive == imbalance_positive:
            return CheckResult.PASS
        
        return CheckResult.FAIL