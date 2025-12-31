"""
단순화된 Consistency Checker

체크 항목 (3가지):
1. spread_valid: Crossed market이 아닌가?
2. price_in_spread: Last price가 mid price 근처에 있는가?
3. funding_imbalance_aligned: Funding과 Imbalance 부호가 일치하는가?

제거된 항목:
- orderbook_exists: 항상 통과 (0% fail rate)
- depth_balanced: 항상 통과 (0% fail rate)
"""
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional


class CheckResult(Enum):
    """체크 결과"""
    PASS = "pass"
    FAIL = "fail"
    SKIP = "skip"


@dataclass
class ConsistencyReport:
    """Consistency 체크 결과 리포트"""
    checks: Dict[str, CheckResult]
    lateness_us: Optional[int] = None  # Ticker-Orderbook 시간 차이 (microseconds)
    
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
    def lateness_ms(self) -> Optional[float]:
        """Lateness in milliseconds"""
        if self.lateness_us is None:
            return None
        return self.lateness_us / 1000.0


class ConsistencyChecker:
    """
    단순화된 Consistency Checker
    
    3가지 체크:
    1. spread_valid: Crossed market 체크
    2. price_in_spread: Last price 위치 체크
    3. funding_imbalance_aligned: Funding-Imbalance 부호 체크
    """
    
    def check_all(self, 
                  ticker_data: Dict, 
                  orderbook: Optional['OrderbookState']) -> ConsistencyReport:
        """모든 consistency check 수행"""
        
        checks = {}
        lateness_us = None
        
        # Orderbook이 없으면 모든 체크 SKIP
        if orderbook is None or not orderbook.bid_levels or not orderbook.ask_levels:
            checks['spread_valid'] = CheckResult.SKIP
            checks['price_in_spread'] = CheckResult.SKIP
            checks['funding_imbalance_aligned'] = CheckResult.SKIP
            return ConsistencyReport(checks=checks, lateness_us=None)
        
        # Lateness 계산 (Ticker timestamp - Orderbook timestamp)
        ticker_ts = ticker_data.get('timestamp')
        if ticker_ts and orderbook.timestamp:
            lateness_us = ticker_ts - orderbook.timestamp
        
        # 1. Spread 유효성 (crossed market 체크)
        checks['spread_valid'] = self._check_spread_valid(orderbook)
        
        # 2. Last price 위치
        checks['price_in_spread'] = self._check_price_in_spread(ticker_data, orderbook)
        
        # 3. Funding-Imbalance 부호 일치
        checks['funding_imbalance_aligned'] = self._check_funding_imbalance_sign(
            ticker_data, orderbook
        )
        
        return ConsistencyReport(checks=checks, lateness_us=lateness_us)
    
    
    def _check_spread_valid(self, orderbook: 'OrderbookState') -> CheckResult:
        """
        Spread가 유효한가? (Crossed market이 아닌가?)
        
        FAIL: best_bid >= best_ask
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
        Last price가 mid price 근처에 있는가?
        
        수정된 로직:
        - 상대값(spread × 10)과 절대값(10bp) 중 큰 threshold 사용
        - Spread가 좁아도 최소 10bp 여유 보장
        """
        last_price = ticker_data.get('last_price')
        if last_price is None:
            return CheckResult.SKIP
        
        best_bid = max(orderbook.bid_levels.keys())
        best_ask = min(orderbook.ask_levels.keys())
        
        if best_bid >= best_ask:
            return CheckResult.FAIL
        
        mid_price = (best_bid + best_ask) / 2
        spread = best_ask - best_bid
        
        # 상대값과 절대값 중 큰 threshold 사용
        relative_threshold = spread * 10
        absolute_threshold = mid_price * 0.001  # 10bp (0.1%)
        threshold = max(relative_threshold, absolute_threshold)
        
        deviation = abs(last_price - mid_price)
        
        if deviation > threshold:
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
        
        FAIL: 둘 다 강한 신호인데 부호가 반대
        PASS: 부호 일치 또는 둘 중 하나가 중립
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
        
        imbalance = (bid_depth - ask_depth) / total
        
        # 중립 판정 기준
        funding_neutral = abs(funding_rate) < 0.0001  # 0.01% 미만
        imbalance_neutral = abs(imbalance) < 0.1  # 10% 미만
        
        # 둘 다 중립이거나 하나만 중립이면 PASS
        if funding_neutral or imbalance_neutral:
            return CheckResult.PASS
        
        # 둘 다 강한 신호 - 부호 비교
        funding_positive = funding_rate > 0
        imbalance_positive = imbalance > 0
        
        if funding_positive == imbalance_positive:
            return CheckResult.PASS
        
        return CheckResult.FAIL