import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from coldledger.audit.trail import AuditTrail
from coldledger.finance.claims_ledger import ClaimsLedger
from coldledger.storage.event_store import EventStore
from coldledger.telemetry.clock_drift import SyncPoint, correct_series, fit_robust
from coldledger.telemetry.correction import ClockCorrectionProcessor
from coldledger.telemetry.watermark import Buffer, Reading


UTC = timezone.utc


def dt(seconds):
    return datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds)


def sync_points(bad_at=None, bad_to=None):
    points = [SyncPoint(dt(i), dt(i)) for i in (0, 10, 20, 30)]
    if bad_at is not None:
        index = [0, 10, 20, 30].index(bad_at)
        points[index] = SyncPoint(dt(bad_at), dt(bad_to))
    return points


class ClockDriftTests(unittest.TestCase):
    def test_bad_sync_point_is_quarantined_without_rejecting_segment(self):
        result = fit_robust(sync_points(20, 999))
        self.assertEqual(result.quarantined, (SyncPoint(dt(20), dt(999)),))
        self.assertEqual(result.model.quarantined[0].trusted_time, dt(999))
        self.assertEqual(correct_series(sync_points(20, 999), [dt(15)]), [dt(15)])

    def test_reject_policy_retains_all_or_nothing_behavior(self):
        with self.assertRaises(ValueError):
            fit_robust(sync_points(20, 999), policy="reject")

    def test_late_reading_preserves_arrival_order_while_using_event_time(self):
        corrected = correct_series(sync_points(), [dt(20), dt(10)])
        self.assertEqual(corrected, [dt(20), dt(10)])


class WatermarkTests(unittest.TestCase):
    def test_late_reading_uses_corrected_event_time_and_deduplicates_sequence(self):
        buffer = Buffer(timedelta(seconds=2))
        first = buffer.add(Reading("d", dt(10), 1.0, 1))
        self.assertEqual(first, [])

        released = buffer.add(Reading("d", dt(13), 2.0, 2))
        self.assertEqual([row.sequence for row in released], [1])

        late = buffer.add(Reading("d", dt(11), 3.0, 1))
        self.assertEqual(late, [])

    def test_flush_emits_every_pending_reading(self):
        buffer = Buffer(timedelta(seconds=60))
        buffer.add(Reading("d", dt(1), 1.0, 1))
        buffer.add(Reading("d", dt(2), 2.0, 2))
        self.assertEqual([row.sequence for row in buffer.flush("d")], [1, 2])


class CorrectionReplayTests(unittest.TestCase):
    def make_processor(self):
        return ClockCorrectionProcessor(
            EventStore(),
            ClaimsLedger(),
            AuditTrail(),
            lateness=timedelta(seconds=2),
        )

    def test_replay_uses_same_event_time_and_does_not_double_bill(self):
        processor = self.make_processor()
        processor.calibrate("d", sync_points(20, 999))
        _, first, marker = processor.ingest(
            "d", 1, dt(1), 5.0, account="claims", amount=Decimal("1.25")
        )
        self.assertIsNone(marker)
        self.assertEqual(first.event_time, dt(1))

        _, second, marker = processor.ingest(
            "d", 2, dt(4), 6.0, account="claims", amount=Decimal("1.25")
        )
        self.assertEqual([entry["sequence"] for entry in marker.payload["entries"]], [1])
        processor.flush("d", account="claims", amount=Decimal("1.25"))
        self.assertEqual(processor.ledger.balance("claims"), Decimal("2.50"))
        self.assertTrue(processor.audit.verify())

        replayed = processor.replay(rebuild_audit=True)
        self.assertEqual([item.event_time for item in replayed], [dt(1), dt(4)])
        self.assertEqual(processor.ledger.balance("claims"), Decimal("2.50"))
        self.assertEqual(len(processor.ledger.entries("claims")), 2)
        self.assertTrue(processor.audit.verify())
        self.assertTrue(any(r.action == "clock.sync.quarantined" for r in processor.audit.records))


if __name__ == "__main__":
    unittest.main()
