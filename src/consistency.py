from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, List


class CheckResult(Enum):
    """단순한 체크 결과"""
    PASS = "pass"
    FAIL = "fail"
    SKIP = "skip"  # 데이터 부족으로 체크 불가


@dataclass
class ConsistencyReport:
    """Consistency 체크 결과 리포트"""
    checks: Dict[str, CheckResult]
    
    @property
    def all_passed(self) -> bool:
        """모든 체크가 통과했는가?"""
        return all(r == CheckResult.PASS for r in self.checks.values() 
                   if r != CheckResult.SKIP)
    
    @property
    def any_failed(self) -> bool:
        """하나라도 실패했는가?"""
        return any(r == CheckResult.FAIL for r in self.checks.values())
    
    @property
    def fail_count(self) -> int:
        """실패한 체크 수"""
        return sum(1 for r in self.checks.values() if r == CheckResult.FAIL)


class ConsistencyChecker:
    """
    단순한 Consistency Checker
    
    원칙:
    - Score 없음, Pass/Fail만
    - Tolerance 없음, Sign만 체크
    - 직관적인 질문들
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
        
        # 2. Spread가 유효한가? (crossed market 아닌가?)
        checks['spread_valid'] = self._check_spread_valid(orderbook)
        
        # 3. Last price가 bid-ask 사이에 있는가?
        checks['price_in_spread'] = self._check_price_in_spread(ticker_data, orderbook)
        
        # 4. Bid/Ask depth가 둘 다 존재하는가?
        checks['depth_balanced'] = self._check_depth_exists(orderbook)
        
        # 5. Funding rate과 Imbalance 방향이 일치하는가?
        checks['funding_imbalance_aligned'] = self._check_funding_imbalance_sign(
            ticker_data, orderbook
        )
        
        return ConsistencyReport(checks=checks)
    
    
    def _check_orderbook_exists(self, orderbook: Optional['OrderbookState']) -> CheckResult:
        """Orderbook이 존재하고 비어있지 않은가?"""
        if orderbook is None:
            return CheckResult.FAIL
        if not orderbook.bid_levels or not orderbook.ask_levels:
            return CheckResult.FAIL
        return CheckResult.PASS
    
    
    def _check_spread_valid(self, orderbook: 'OrderbookState') -> CheckResult:
        """
        Spread가 유효한가?
        - Best bid < Best ask (crossed market이 아닌가?)
        """
        best_bid = max(orderbook.bid_levels.keys())
        best_ask = min(orderbook.ask_levels.keys())
        
        # Crossed market check
        if best_bid >= best_ask:
            return CheckResult.FAIL
        
        return CheckResult.PASS
    
    
    def _check_price_in_spread(self, 
                                ticker_data: Dict, 
                                orderbook: 'OrderbookState') -> CheckResult:
        """
        Last price가 합리적인 범위에 있는가?
        - best_bid <= last_price <= best_ask 이면 이상적
        - 약간 벗어나도 괜찮지만, 너무 벗어나면 이상함
        """
        last_price = ticker_data.get('last_price')
        if last_price is None:
            return CheckResult.SKIP
        
        best_bid = max(orderbook.bid_levels.keys())
        best_ask = min(orderbook.ask_levels.keys())
        mid_price = (best_bid + best_ask) / 2
        spread = best_ask - best_bid
        
        # Last price가 spread의 N배 이상 벗어나면 FAIL
        # 예: spread가 10이면, mid ± 50 범위를 벗어나면 이상
        max_deviation = spread * 5
        
        if abs(last_price - mid_price) > max_deviation:
            return CheckResult.FAIL
        
        return CheckResult.PASS
    
    
    def _check_depth_exists(self, orderbook: 'OrderbookState') -> CheckResult:
        """
        Bid와 Ask 양쪽에 depth가 존재하는가?
        - 한쪽만 있으면 FAIL
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
        - Funding > 0 (롱이 숏에게 지불) → 롱 포지션 많음 → bid pressure 예상
        - Funding < 0 (숏이 롱에게 지불) → 숏 포지션 많음 → ask pressure 예상
        
        Sign만 체크, 크기는 무시
        """
        funding_rate = ticker_data.get('funding_rate')
        if funding_rate is None:
            return CheckResult.SKIP
        
        # Orderbook imbalance 계산 (bid - ask) / (bid + ask)
        bid_depth = sum(orderbook.bid_levels.values())
        ask_depth = sum(orderbook.ask_levels.values())
        total = bid_depth + ask_depth
        
        if total == 0:
            return CheckResult.SKIP
        
        imbalance = (bid_depth - ask_depth) / total  # 양수면 bid 우세
        
        # 둘 다 0에 가까우면 (중립) PASS
        if abs(funding_rate) < 0.0001 and abs(imbalance) < 0.1:
            return CheckResult.PASS
        
        # Sign 일치 여부만 체크
        # funding > 0 이면 imbalance > 0 이어야 함
        funding_sign = 1 if funding_rate > 0 else (-1 if funding_rate < 0 else 0)
        imbalance_sign = 1 if imbalance > 0.1 else (-1 if imbalance < -0.1 else 0)
        
        # 같은 부호이거나 둘 중 하나가 중립이면 PASS
        if funding_sign == 0 or imbalance_sign == 0:
            return CheckResult.PASS
        if funding_sign == imbalance_sign:
            return CheckResult.PASS
        
        return CheckResult.FAIL