# Q1. 가장 위험한 불확실성은 무엇이었는가?
Orderbook의 신선도(Freshness)와 일관성(Integrity)이 가장 위험한 불확실성이라고 판단했다.

이 두 요소는 시장 상태 이전에 Orderbook 자체에 대한 신뢰를 무너뜨릴 수 있으며, 신뢰할 수 없는 상태에서는 alpha판단도 어렵다 생각들었다.
구체적으로는 다음을 핵심 위험으로 보았다:

- 허용된 latency 하에서 effective orderbook 정의 가능 여부
- Orderbook의 staleness (stale ratio)
- Crossed market, price deviation 등 데이터 무결성 이슈

# Q2. Dirty Data로부터 어떤 판단 조건을 정의했는가?
Dirty Data를 명시적인 예외 처리로 분리하지 않고, Uncertainty Vector 내부의 크기와 상태로 표현했다.

Uncertainty Vector
├── Freshness: avg_latency, stale_ratio, out_of_order_count
├── Integrity: spread_valid, price_deviation, imbalance_funding_mismatch
└── Stability: price_volatility (rolling std of returns)

이 중 Freshness와 Integrity를 Dirty Data 판단의 핵심 축으로 사용했다.

# Q3. 그 조건이 의사결정 구조에 어떻게 반영되는가?
Dirty Data는 Sanitization Policy를 통해 Data Trust 상태로 반영된다.

- ACCEPT : 정상 데이터 -> DATA TRUST 유지
- REPAIR : 경미한 이상 -> DATA TRUST를 DEGRADED로 하향
- QUARANTINE : 심각한 이상 -> DATA TRUST를 UNTRUSTED로 하향

의사결정은 개별 지표가 아니라 Data Trust 상태를 통해 간접적으로 영향을 받도록 설계했다.

# Q4. 가설 변화가 시스템 동작을 어떻게 바꾸는가?
- Orderbook 상태를 정의하는 지표로 Price Volatility를 선택했다.
EDA 결과, 평상시에는 변동이 작고 Liquidation 전후로 크게 변하는 것을 확인했다. (Cohen's d = 0.537)

평상시 : mean = 0.276 bps, p90 = 0.498, p95 = 0.617
Liquidation : mean = 0.404 bps

이를 바탕으로 Hypothesis를 판단했다.
- volatility < 0.50 (p90) -> VALID
- volatility < 0.62 (p95) -> WEAKENING
- volatility > 0.62 -> INVALID

Hypothesis가 INVALID가 되면, Data Trust와 무관하게 HALTED 상태로 전환된다.

# Q5. 언제 판단을 중단하도록 설계했는가?
아래 조건 중 하나라도 만족하면 HALTED (판단 중단):
1. Data Trust = UNTRUSTED
- 평균 latency > 50 ms
- Stale ratio > 15%
- QUARANTINE 발생 (crossed market, 심각한 price deviation)

2. Hypothesis = INVALID
- Price volatility > 0.62 bps (p95 초과)

Decision matrix에서 UNTRUSTED 행과 INVALID 열은 모두 HALTED로 매핑된다.

# Q6. 지금 다시 설계한다면, 가장 먼저 제거하거나 단수화할 요소는 무엇인가?
EDA 단계에서 다양한 지표들을 테스트하며 가장 단순한 것(Price Volatility)으로 시작했기 때문에, 아키텍처에서 제거하거나 단순화할 요소는 없다고 판단된다.

다만, 아쉬운 점:
1. Research와 Validation의 측정 목표 명확화 부족
    - 무엇을 측정하고 싶은지 사전에 더 명확히 정의했어야 함
2. EDA용 시뮬레이션 데이터 분리
    - 주어진 데이터에서 calibration용과 validation용을 미리 분리했어야 함
    - Time-based split 등으로 overfitting 고려
3. 초기에 AR(N) 모델 시도
    - Spread 변동이 너무 작아 무의미했음
    - EDA를 먼저 했다면 시간 절약 가능했음