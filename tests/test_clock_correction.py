import unittest
from datetime import datetime,timedelta,timezone
from decimal import Decimal
from coldledger.telemetry.clock_drift import SyncPoint,Model,fit,correct,correct_series
from coldledger.telemetry.watermark import Buffer,Reading
from coldledger.telemetry.corrections import CorrectionLog
from coldledger.claims.excursions import Excursion
from coldledger.claims.billing import post
from coldledger.finance.claims_ledger import ClaimsLedger
UTC=timezone.utc
BASE=datetime(2026,1,1,tzinfo=UTC)
def clean_sync(n=6,offset=5.0,drift=1.0001,step=3600.0):
    return [SyncPoint(BASE+timedelta(seconds=i*step),BASE+timedelta(seconds=offset+i*step*drift)) for i in range(n)]
def poisoned(sync,at,delta):
    rows=list(sync);bad=rows[at];rows[at]=SyncPoint(bad.device_time,bad.trusted_time+delta);return rows
class ClockDriftTests(unittest.TestCase):
    def test_clean_fit(self):
        m=fit(clean_sync())
        self.assertAlmostEqual(m.slope,1.0001,places=6)
        self.assertEqual(m.isolated,())
        self.assertEqual(m.revision,0)
    def test_single_bad_sync_point_isolated(self):
        m=fit(poisoned(clean_sync(),-1,timedelta(seconds=300)))
        self.assertAlmostEqual(m.slope,1.0001,places=6)
        self.assertEqual(len(m.isolated),1)
        self.assertEqual(m.revision,1)
        clean=fit(clean_sync());at=BASE+timedelta(seconds=2.5*3600)
        self.assertAlmostEqual((correct(m,at)-correct(clean,at)).total_seconds(),0.0,places=3)
    def test_middle_bad_point_isolated(self):
        m=fit(poisoned(clean_sync(),2,timedelta(seconds=-120)))
        self.assertEqual(len(m.isolated),1)
        self.assertAlmostEqual(m.slope,1.0001,places=6)
    def test_three_points_cannot_isolate_rejects_segment(self):
        with self.assertRaises(ValueError): fit(poisoned(clean_sync(n=3),1,timedelta(seconds=60)))
    def test_two_outliers_isolated_when_redundancy_allows(self):
        rows=poisoned(clean_sync(),-1,timedelta(seconds=300))
        rows=poisoned(rows,-2,timedelta(seconds=-300))
        m=fit(rows)
        self.assertEqual(len(m.isolated),2)
        self.assertAlmostEqual(m.slope,1.0001,places=6)
    def test_extreme_point_without_redundancy_rejects_segment(self):
        with self.assertRaises(ValueError): fit(poisoned(clean_sync(n=4),-1,timedelta(hours=1)))
    def test_too_few_points(self):
        with self.assertRaises(ValueError): fit(clean_sync(n=2))
    def test_correct_series_monotone_guard(self):
        with self.assertRaises(ValueError):
            correct_series(clean_sync(),[BASE+timedelta(seconds=2),BASE+timedelta(seconds=1)])
    def test_model_key_identifies_correction(self):
        a=fit(clean_sync());b=fit(poisoned(clean_sync(),-1,timedelta(seconds=300)))
        self.assertNotEqual(a.key(),b.key())
        self.assertEqual(a.key(),fit(clean_sync()).key())
class WatermarkTests(unittest.TestCase):
    def test_sequences_scoped_per_device(self):
        b=Buffer(timedelta(minutes=5))
        b.add(Reading('A',BASE,1.0,1));b.add(Reading('B',BASE,2.0,1))
        out_a=b.add(Reading('A',BASE+timedelta(minutes=10),1.1,2))
        out_b=b.add(Reading('B',BASE+timedelta(minutes=10),2.1,2))
        self.assertEqual([r.sequence for r in out_a],[1])
        self.assertEqual([r.sequence for r in out_b],[1])
        self.assertEqual([r.sequence for r in b.flush('A')],[2])
        self.assertEqual([r.sequence for r in b.flush('B')],[2])
    def test_flush_emits_every_pending_reading(self):
        b=Buffer(timedelta(hours=1))
        b.add(Reading('A',BASE,1.0,1));b.add(Reading('A',BASE+timedelta(seconds=1),2.0,2))
        self.assertEqual(len(b.flush('A')),2)
    def test_replay_never_reemits(self):
        b=Buffer(timedelta(0));r=Reading('A',BASE,1.0,1)
        self.assertEqual(len(b.add(r)),1)
        self.assertEqual(len(b.add(r)),0)
class CorrectionLogTests(unittest.TestCase):
    def test_record_idempotent_per_model(self):
        log=CorrectionLog();m=fit(clean_sync())
        self.assertIs(log.record('devA',m),log.record('devA',m))
        self.assertEqual(len(log.all()),1)
    def test_revision_supersedes_and_keeps_history(self):
        log=CorrectionLog();log.record('devA',fit(clean_sync()))
        m2=fit(poisoned(clean_sync(),-1,timedelta(seconds=300)))
        rec=log.record('devA',m2,reason='isolated bad sync point')
        self.assertEqual(log.current('devA').model_key,m2.key())
        self.assertEqual(len(rec.isolated),1)
        self.assertEqual(len(log.all('devA')),2)
class BillingTests(unittest.TestCase):
    def setUp(self):
        self.ledger=ClaimsLedger()
        self.m1=fit(clean_sync())
    def excs(self,model,n=2):
        out=[]
        for i in range(n):
            start=correct(model,BASE+timedelta(seconds=1000+i*10000))
            out.append(Excursion(start,start+timedelta(seconds=600),8.5))
        return out
    def test_replay_same_model_posts_nothing(self):
        e=self.excs(self.m1)
        self.assertEqual(len(post(self.ledger,'claims','devA',e,self.m1.key())),2)
        self.assertEqual(post(self.ledger,'claims','devA',e,self.m1.key()),())
        self.assertEqual(self.ledger.balance('claims'),Decimal('1200'))
    def test_revision_reverses_then_reposts_once(self):
        post(self.ledger,'claims','devA',self.excs(self.m1),self.m1.key())
        m2=fit(poisoned(clean_sync(),-1,timedelta(seconds=300)))
        post(self.ledger,'claims','devA',self.excs(m2),m2.key())
        self.assertEqual(self.ledger.balance('claims'),Decimal('1200'))
        self.assertIn('clock-correction revised',[e.reason for e in self.ledger.entries('claims')])
    def test_revision_with_shifted_event_times(self):
        m_old=Model(BASE,1.0001,5.0)
        m_new=Model(BASE,1.0001,8.0,revision=1)
        post(self.ledger,'claims','devA',self.excs(m_old),m_old.key())
        post(self.ledger,'claims','devB',self.excs(m_old),m_old.key())
        post(self.ledger,'claims','devA',self.excs(m_new),m_new.key())
        self.assertEqual(self.ledger.balance('claims'),Decimal('2400'))
        self.assertEqual(len([e for e in self.ledger.entries('claims') if e.reason=='clock-correction revised']),2)
if __name__=='__main__':unittest.main()
