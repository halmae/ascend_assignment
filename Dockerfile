# Single Decision Engine
# Phase 1: Historical Validation
# Phase 2: Realtime Validation (Binance Futures WebSocket)

FROM python:3.11-slim

WORKDIR /app

# 시스템 패키지
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Python 패키지
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 소스 코드 복사
COPY src/ ./src/
COPY run.py .

# 출력 디렉토리 생성
RUN mkdir -p /output /data

# 환경 변수
ENV PYTHONUNBUFFERED=1

# 엔트리포인트
ENTRYPOINT ["python", "run.py"]

# ============================================================
# 사용법:
# 
# Phase 1: Historical Validation
#   docker run -v /path/to/data:/data your-image historical
#
# Phase 2: Realtime Validation  
#   docker run your-image realtime
#
# ============================================================

# 기본: Realtime 모드
CMD ["realtime"]
