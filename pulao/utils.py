import time
import threading

class IDGenerator:
    """
    高性能 Snowflake ID 生成器，线程安全，生成 int64 ID，适合 Polars DataFrame
    64位结构：
        41位：时间戳(ms)
        10位：worker_id
        12位：sequence
    """
    def __init__(self, worker_id: int = 0):
        if worker_id < 0 or worker_id > 1023:
            raise ValueError("worker_id must be between 0 and 1023")
        self.worker_id = worker_id
        self.sequence = 0
        self.last_timestamp = -1
        self.lock = threading.Lock()

        # 位移
        self.worker_id_bits = 10
        self.sequence_bits = 12
        self.max_sequence = (1 << self.sequence_bits) - 1
        self.worker_id_shift = self.sequence_bits
        self.timestamp_shift = self.sequence_bits + self.worker_id_bits

        # 自定义纪元（毫秒）
        self.epoch = int(time.mktime(time.strptime('2025-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')) * 1000)

    def _current_timestamp(self):
        return int(time.time() * 1000)

    def get_id(self) -> int:
        with self.lock:
            timestamp = self._current_timestamp()
            if timestamp < self.last_timestamp:
                raise Exception("Clock moved backwards. Refusing to generate id")

            if timestamp == self.last_timestamp:
                self.sequence = (self.sequence + 1) & self.max_sequence
                if self.sequence == 0:
                    # sequence 超过最大值，等待下一毫秒
                    while timestamp <= self.last_timestamp:
                        timestamp = self._current_timestamp()
            else:
                self.sequence = 0

            self.last_timestamp = timestamp

            new_id = ((timestamp - self.epoch) << self.timestamp_shift) | \
                     (self.worker_id << self.worker_id_shift) | \
                     self.sequence
            return new_id

    # 批量生成 ID（适合 Polars 构造列）
    def get_ids(self, n: int):
        return [self.get_id() for _ in range(n)]
