from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable


UTC = timezone.utc


@dataclass(frozen=True)
class Reading:
    device: str
    event_time: datetime
    value: float
    sequence: int


class Buffer:
    def __init__(self, lateness):
        self.lateness = lateness
        self.max = {}
        self.pending = defaultdict(list)
        self.seen = defaultdict(set)
        self.emitted = defaultdict(set)

    @staticmethod
    def _utc(value):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timezone-aware datetime required")
        return value.astimezone(UTC)

    def _key(self, reading):
        return (reading.device, reading.sequence)

    def _drain(self, device, flush_all=False):
        if device not in self.max:
            return []
        watermark = None if flush_all else self.max[device] - self.lateness
        ready = []
        remaining = []
        for row in self.pending[device]:
            event_time = self._utc(row.event_time)
            if flush_all or event_time <= watermark:
                ready.append(row)
            else:
                remaining.append(row)

        ready.sort(key=lambda x: (self._utc(x.event_time), x.sequence))
        out = []
        for row in ready:
            key = self._key(row)
            if key in self.emitted[device]:
                continue
            out.append(row)
            self.emitted[device].add(key)

        self.pending[device] = remaining
        return out

    def add(self, reading):
        self._utc(reading.event_time)
        key = self._key(reading)
        if key in self.seen[reading.device] or key in self.emitted[reading.device]:
            return []
        self.seen[reading.device].add(key)

        t = self._utc(reading.event_time)
        current_max = self.max.get(reading.device)
        if current_max is None or t > current_max:
            self.max[reading.device] = t

        self.pending[reading.device].append(reading)
        return self._drain(reading.device)

    def flush(self, device):
        return self._drain(device, flush_all=True)

    def mark_emitted(self, device, sequences: Iterable[int]):
        """Drop rows that a replay proves were emitted by an earlier process run."""
        wanted = set(sequences)
        released = []
        remaining = []
        for row in self.pending[device]:
            key = self._key(row)
            if row.sequence in wanted and key not in self.emitted[device]:
                released.append(row)
                self.emitted[device].add(key)
            elif key not in self.emitted[device]:
                remaining.append(row)
        self.pending[device] = remaining
        released.sort(key=lambda x: (self._utc(x.event_time), x.sequence))
        return released
