from dataclasses import dataclass
from datetime import datetime,timezone
from threading import RLock
@dataclass(frozen=True)
class Correction:
    sequence:int; device:str; model_key:str; revision:int; isolated:tuple; reason:str; at:datetime
class CorrectionLog:
    """Append-only record of the clock-correction model in force per device.

    Watermarks, excursion detection and billing must all reference the
    current model key so every stage computes on the same event time.
    record() is idempotent on (device, model key): replaying a fit returns
    the existing entry instead of appending a duplicate, and superseded
    models stay in the log for audit.
    """
    def __init__(self):
        self._rows=[]; self._by_key={}; self._lock=RLock()
    def record(self,device,model,reason='fit',at=None):
        key=(device,model.key())
        with self._lock:
            if key in self._by_key: return self._by_key[key]
            row=Correction(len(self._rows)+1,device,model.key(),model.revision,tuple(p.device_time.astimezone(timezone.utc).isoformat() for p in model.isolated),reason,at or datetime.now(timezone.utc))
            self._rows.append(row); self._by_key[key]=row; return row
    def current(self,device):
        with self._lock:
            rows=[r for r in self._rows if r.device==device]
            return rows[-1] if rows else None
    def all(self,device=None):
        with self._lock:return tuple(r for r in self._rows if device is None or r.device==device)
