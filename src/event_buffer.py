"""
Event Buffer - Time Alignment Policy 구현

================================================================================
과제 6.2 Time Alignment Policy 요구사항:
- event-time / processing-time 정의
- allowed lateness
- buffer / window / watermark

================================================================================
개념 정의:

1. event-time (timestamp)
   - 이벤트가 거래소에서 발생한 시간
   - 데이터의 "실제 시간"
   
2. processing-time (local_timestamp)
   - 이벤트가 우리 시스템에 도착한 시간
   - 네트워크 지연 등으로 event-time보다 늦음

3. buffer
   - 이벤트를 모아두는 공간
   - buffer_max_size: 최대 이벤트 수
   
4. window
   - event-time 기준 시간 범위
   - window_size_ms: 윈도우 크기 (ms)
   - 같은 window 내 이벤트들은 함께 처리
   
5. watermark
   - "이 시점까지의 이벤트는 모두 도착했다"는 표시
   - 가장 마지막으로 처리 완료된 event-time
   - watermark 이전의 이벤트가 도착하면 "late event"

6. allowed_lateness
   - watermark 이전 이벤트 중 얼마나 늦은 것까지 허용할지
   - event_time < watermark - allowed_lateness → drop

================================================================================
동작 방식:

1. 이벤트 도착 → 버퍼에 추가
2. 버퍼가 가득 차거나 window 경과 시:
   - 버퍼 내 이벤트들을 event-time 기준 정렬
   - 정렬된 이벤트들을 순서대로 emit
   - 마지막 emit된 이벤트의 event-time이 새 watermark
3. 새 이벤트 도착 시 watermark - allowed_lateness 이전이면 drop

================================================================================
"""
import heapq
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from collections import defaultdict

from src.data_types import Event
from src.enums import EventType
from src.config import THRESHOLDS


@dataclass
class BufferStats:
    """버퍼 통계"""
    total_received: int = 0
    total_emitted: int = 0
    dropped_late: int = 0           # watermark 이전이라 버려진 이벤트
    out_of_order_received: int = 0  # out-of-order로 도착한 이벤트
    max_buffer_size: int = 0        # 관측된 최대 버퍼 크기
    windows_emitted: int = 0        # emit된 윈도우 수


@dataclass 
class BufferConfig:
    """
    버퍼 설정
    
    buffer_max_size: 버퍼에 담을 최대 이벤트 수
    window_size_ms: event-time 기준 윈도우 크기 (ms)
    allowed_lateness_ms: watermark 이전 이벤트 허용 범위 (ms)
    """
    buffer_max_size: int = 10000            # 최대 이벤트 수
    window_size_ms: float = 1000.0          # 1초 윈도우
    allowed_lateness_ms: float = 100.0      # 100ms 지연 허용
    
    @classmethod
    def from_thresholds(cls) -> 'BufferConfig':
        """config.py의 THRESHOLDS에서 생성"""
        return cls(
            buffer_max_size=THRESHOLDS.buffer_max_size,
            window_size_ms=THRESHOLDS.window_size_ms,
            allowed_lateness_ms=THRESHOLDS.allowed_lateness_ms,
        )


class EventBuffer:
    """
    Time Alignment을 위한 이벤트 버퍼
    
    - 이벤트를 버퍼에 모아서 event-time 기준 정렬
    - window 단위로 이벤트 emit
    - watermark 기반 late event 처리
    
    사용법:
        buffer = EventBuffer(config)
        
        for raw_event in stream:
            ready_events = buffer.add(raw_event)
            for event in ready_events:
                processor.process(event)
        
        # 스트림 종료 시 남은 이벤트 처리
        remaining = buffer.flush()
    """
    
    def __init__(self, config: Optional[BufferConfig] = None):
        self.config = config or BufferConfig.from_thresholds()
        
        # 버퍼: (event_time, sequence, event) 형태의 heap
        # sequence는 같은 timestamp 이벤트의 순서 보장용
        self._buffer: List[tuple] = []
        self._sequence: int = 0
        
        # Watermark: 마지막으로 emit된 이벤트의 event-time
        self._watermark: int = 0
        
        # 현재 윈도우 시작 시간
        self._window_start: int = 0
        
        # 관측된 최대 event-time (윈도우 경계 판단용)
        self._max_event_time: int = 0
        
        # 스트림별 마지막 event-time (out-of-order 감지용)
        self._last_event_time_by_type: Dict[EventType, int] = defaultdict(int)
        
        # 통계
        self.stats = BufferStats()
    
    def add(self, event: Event) -> List[Event]:
        """
        이벤트 추가 및 처리 가능한 이벤트 반환
        
        Args:
            event: 추가할 이벤트
            
        Returns:
            처리 가능한 이벤트 리스트 (event-time 순으로 정렬됨)
        """
        self.stats.total_received += 1
        event_time = event.timestamp  # microseconds
        
        # Out-of-order 감지 (같은 스트림 내에서)
        last_time = self._last_event_time_by_type[event.event_type]
        if event_time < last_time:
            self.stats.out_of_order_received += 1
        self._last_event_time_by_type[event.event_type] = max(last_time, event_time)
        
        # Late event 체크: watermark - allowed_lateness 이전이면 drop
        allowed_lateness_us = int(self.config.allowed_lateness_ms * 1000)
        late_threshold = self._watermark - allowed_lateness_us
        
        if self._watermark > 0 and event_time < late_threshold:
            # 너무 늦은 이벤트: drop
            self.stats.dropped_late += 1
            return []
        
        # 버퍼에 추가 (heap으로 event-time 순서 유지)
        heapq.heappush(self._buffer, (event_time, self._sequence, event))
        self._sequence += 1
        
        # 최대 event-time 업데이트
        self._max_event_time = max(self._max_event_time, event_time)
        
        # 윈도우 시작 초기화
        if self._window_start == 0:
            self._window_start = event_time
        
        # 통계 업데이트
        self.stats.max_buffer_size = max(self.stats.max_buffer_size, len(self._buffer))
        
        # 윈도우 경과 또는 버퍼 가득 찼으면 emit
        ready_events = []
        window_size_us = int(self.config.window_size_ms * 1000)
        
        if (self._max_event_time - self._window_start >= window_size_us or 
            len(self._buffer) >= self.config.buffer_max_size):
            ready_events = self._emit_window()
        
        return ready_events
    
    def _emit_window(self) -> List[Event]:
        """
        현재 윈도우의 이벤트들을 정렬하여 반환
        
        Returns:
            처리 가능한 이벤트 리스트
        """
        if not self._buffer:
            return []
        
        ready_events = []
        window_size_us = int(self.config.window_size_ms * 1000)
        window_end = self._window_start + window_size_us
        
        # 윈도우 내 이벤트들 emit
        while self._buffer:
            event_time, seq, event = self._buffer[0]
            
            # 윈도우 끝 이전의 이벤트만 emit
            if event_time < window_end:
                heapq.heappop(self._buffer)
                ready_events.append(event)
                self.stats.total_emitted += 1
                
                # Watermark 업데이트: 마지막 emit된 이벤트의 event-time
                self._watermark = max(self._watermark, event_time)
            else:
                break
        
        # 다음 윈도우 시작
        self._window_start = window_end
        self.stats.windows_emitted += 1
        
        return ready_events
    
    def flush(self) -> List[Event]:
        """
        버퍼에 남은 모든 이벤트 반환 (스트림 종료 시)
        
        Returns:
            남은 모든 이벤트 (event-time 순)
        """
        remaining = []
        
        while self._buffer:
            event_time, seq, event = heapq.heappop(self._buffer)
            remaining.append(event)
            self.stats.total_emitted += 1
            self._watermark = max(self._watermark, event_time)
        
        return remaining
    
    def get_watermark(self) -> int:
        """현재 watermark 반환 (microseconds)"""
        return self._watermark
    
    def get_watermark_ms(self) -> float:
        """현재 watermark 반환 (milliseconds)"""
        return self._watermark / 1000.0
    
    def get_buffer_size(self) -> int:
        """현재 버퍼 크기"""
        return len(self._buffer)
    
    def get_stats(self) -> BufferStats:
        """버퍼 통계 반환"""
        return self.stats
    
    def get_stats_dict(self) -> Dict:
        """버퍼 통계를 딕셔너리로 반환"""
        return {
            'total_received': self.stats.total_received,
            'total_emitted': self.stats.total_emitted,
            'dropped_late': self.stats.dropped_late,
            'out_of_order_received': self.stats.out_of_order_received,
            'max_buffer_size': self.stats.max_buffer_size,
            'windows_emitted': self.stats.windows_emitted,
            'current_buffer_size': len(self._buffer),
            'current_watermark_ms': self.get_watermark_ms(),
        }
    
    def reset(self):
        """버퍼 초기화"""
        self._buffer.clear()
        self._sequence = 0
        self._watermark = 0
        self._window_start = 0
        self._max_event_time = 0
        self._last_event_time_by_type.clear()
        self.stats = BufferStats()