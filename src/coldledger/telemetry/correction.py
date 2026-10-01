from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import Any, Iterable

from ..audit.trail import AuditTrail
from ..finance.claims_ledger import ClaimsLedger
from ..storage.event_store import EventStore
from .clock_drift import Model, SyncPoint, correct, fit_robust
from .watermark import Buffer, Reading


UTC = timezone.utc
DEFAULT_STREAM = "clock-corrections"


@dataclass(frozen=True)
class Correction:
    device: str
    sequence: int
    value: float
    device_time: datetime
    event_time: datetime


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timezone-aware datetime required")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat()


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value)


class ClockCorrectionProcessor:
    """Event-sourced clock correction pipeline.

    The corrected (trusted) timestamp is the single event time used by the reading
    event, watermark and audit/billing records.  Synchronization outliers are
    quarantined in the calibration event instead of being allowed to move every
    downstream temperature event.
    """

    def __init__(
        self,
        store: EventStore,
        ledger: ClaimsLedger | None = None,
        audit: AuditTrail | None = None,
        *,
        lateness: timedelta = timedelta(0),
        residual_tolerance_seconds: float = 1.0,
        stream: str = DEFAULT_STREAM,
    ):
        self.store = store
        self.ledger = ledger
        self.audit = audit
        self.lateness = lateness
        self.residual_tolerance_seconds = residual_tolerance_seconds
        self.stream = stream
        self.buffer = Buffer(lateness)
        self.models: dict[str, Model] = {}
        self._lock = RLock()

    def calibrate(
        self,
        device: str,
        points: Iterable[SyncPoint],
        *,
        actor: str = "clock",
        policy: str = "isolate",
        expected: int | None = None,
    ):
        policy = {
            "reject": "reject",
            "reject_segment": "reject",
            "isolate": "isolate",
            "isolate_outlier": "isolate",
            "quarantine": "isolate",
        }.get(policy, policy)
        with self._lock:
            result = fit_robust(
                points,
                residual_tolerance_seconds=self.residual_tolerance_seconds,
                policy=policy,
            )
            model_event_time = result.model.anchor + timedelta(seconds=result.model.intercept)
            payload = {
                "device": device,
                "event_time": _iso(model_event_time),
                "anchor": _iso(result.model.anchor),
                "slope": result.model.slope,
                "intercept": result.model.intercept,
                "policy": policy,
                "points": [
                    {"device_time": _iso(p.device_time), "trusted_time": _iso(p.trusted_time)}
                    for p in result.points
                ],
                "quarantined": [
                    {"device_time": _iso(p.device_time), "trusted_time": _iso(p.trusted_time)}
                    for p in result.quarantined
                ],
            }
            event = self.store.append(
                self.stream,
                len(self.store.read(self.stream)) if expected is None else expected,
                "clock.model.calibrated",
                payload,
            )
            self.models[device] = result.model
            if self.audit is not None:
                self.audit.append(actor, "clock.model.calibrated", f"calibration:{device}", model_event_time)
                for point in result.quarantined:
                    self.audit.append(
                        actor,
                        "clock.sync.quarantined",
                        f"sync:{device}",
                        correct(result.model, point.device_time),
                    )
            return event, result

    def ingest(
        self,
        device: str,
        sequence: int,
        device_time: datetime,
        value: float,
        *,
        actor: str = "device",
        account: str | None = None,
        amount=None,
        reason: str = "temperature reading",
        expected: int | None = None,
    ):
        with self._lock:
            model = self.models.get(device)
            if model is None:
                raise ValueError("no calibrated drift model for device")
            if (device, sequence) in self.buffer.seen[device] or (
                device,
                sequence,
            ) in self.buffer.emitted[device]:
                raise ValueError("duplicate reading sequence")

            raw_time = _utc(device_time)
            event_time = _utc(correct(model, raw_time))
            event = self.store.append(
                self.stream,
                len(self.store.read(self.stream)) if expected is None else expected,
                "clock.reading.corrected",
                {
                    "device": device,
                    "sequence": sequence,
                    "value": value,
                    "device_time": _iso(raw_time),
                    "event_time": _iso(event_time),
                },
            )
            if self.audit is not None:
                self.audit.append(actor, "clock.reading.corrected", f"{device}:{sequence}", event_time)

            emitted = self.buffer.add(Reading(device, event_time, value, sequence))
            marker = None
            if emitted:
                marker = self._release(
                    device,
                    emitted,
                    actor=actor,
                    account=account,
                    amount=amount,
                    reason=reason,
                    flushed=False,
                )
            correction = Correction(device, sequence, value, raw_time, event_time)
            return event, correction, marker

    def flush(
        self,
        device: str,
        *,
        actor: str = "clock",
        account: str | None = None,
        amount=None,
        reason: str = "temperature reading",
    ):
        with self._lock:
            emitted = self.buffer.flush(device)
            if not emitted:
                return None
            return self._release(
                device,
                emitted,
                actor=actor,
                account=account,
                amount=amount,
                reason=reason,
                flushed=True,
            )

    def _release(self, device, rows, *, actor, account, amount, reason, flushed):
        entries = [
            {
                "sequence": row.sequence,
                "event_time": _iso(row.event_time),
                "value": row.value,
                "account": account or device,
                "amount": None if amount is None else str(amount),
                "reason": reason,
            }
            for row in rows
        ]
        event = self.store.append(
            self.stream,
            len(self.store.read(self.stream)),
            "clock.readings.released",
            {"device": device, "flushed": flushed, "entries": entries},
        )

        if self.ledger is not None and amount is not None:
            for row, entry in zip(rows, entries):
                reference = f"clock:{device}:{row.sequence}"
                self.ledger.post_once(entry["account"], reference, entry["amount"], reason)
                if self.audit is not None:
                    self.audit.append(actor, "clock.reading.billed", reference, row.event_time)
        return event

    def replay(self, *, rebuild_audit: bool = False) -> tuple[Correction, ...]:
        """Replay durable events without duplicating billing entries."""
        with self._lock:
            models: dict[str, Model] = {}
            buffer = Buffer(self.lateness)
            audit = AuditTrail() if rebuild_audit else None
            corrections: list[Correction] = []

            for event in self.store.read(self.stream):
                if event.kind == "clock.model.calibrated":
                    model = self._model_from_payload(event.payload)
                    models[event.payload["device"]] = model
                    model_event_time = _parse(
                        event.payload.get(
                            "event_time",
                            _iso(model.anchor + timedelta(seconds=model.intercept)),
                        )
                    )
                    if audit is not None:
                        audit.append("replay", event.kind, f"calibration:{event.payload['device']}", model_event_time)
                        for point in model.quarantined:
                            audit.append(
                                "replay",
                                "clock.sync.quarantined",
                                f"sync:{event.payload['device']}",
                                correct(model, point.device_time),
                            )

                elif event.kind == "clock.reading.corrected":
                    device = event.payload["device"]
                    sequence = event.payload["sequence"]
                    event_time = _parse(event.payload["event_time"])
                    raw_time = _parse(event.payload["device_time"])
                    correction = Correction(
                        device,
                        sequence,
                        event.payload["value"],
                        raw_time,
                        event_time,
                    )
                    corrections.append(correction)
                    if audit is not None:
                        audit.append("replay", event.kind, f"{device}:{sequence}", event_time)
                    buffer.add(Reading(device, event_time, event.payload["value"], sequence))

                elif event.kind == "clock.readings.released":
                    device = event.payload["device"]
                    entries = event.payload["entries"]
                    rows = buffer.mark_emitted(device, [entry["sequence"] for entry in entries])
                    by_sequence = {row.sequence: row for row in rows}
                    for entry in entries:
                        row = by_sequence.get(entry["sequence"])
                        if row is None:
                            # The reading was already released earlier in this replay.
                            row = Reading(
                                device,
                                _parse(entry["event_time"]),
                                entry["value"],
                                entry["sequence"],
                            )
                        if self.ledger is not None and entry["amount"] is not None:
                            reference = f"clock:{device}:{entry['sequence']}"
                            self.ledger.post_once(
                                entry["account"], reference, entry["amount"], entry["reason"]
                            )
                            if audit is not None:
                                audit.append("replay", "clock.reading.billed", reference, row.event_time)

            self.models = models
            self.buffer = buffer
            if rebuild_audit:
                self.audit = audit
            return tuple(corrections)

    @staticmethod
    def _model_from_payload(payload: dict[str, Any]) -> Model:
        quarantined = tuple(
            SyncPoint(_parse(p["device_time"]), _parse(p["trusted_time"]))
            for p in payload.get("quarantined", ())
        )
        return Model(_parse(payload["anchor"]), payload["slope"], payload["intercept"], quarantined)
