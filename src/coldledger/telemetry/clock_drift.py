from dataclasses import dataclass
from datetime import datetime,timedelta,timezone
from hashlib import sha256
from statistics import median
MIN_POINTS=3
MIN_SLOPE,MAX_SLOPE=.95,1.05
ISOLATION_SIGMA=3.0
ISOLATION_FLOOR_SECONDS=1.0
@dataclass(frozen=True)
class SyncPoint: device_time:datetime; trusted_time:datetime
@dataclass(frozen=True)
class Model:
    anchor:datetime; slope:float; intercept:float
    isolated:tuple=(); revision:int=0
    def key(self):
        raw='|'.join([self.anchor.astimezone(timezone.utc).isoformat(),f'{self.slope:.12f}',f'{self.intercept:.6f}',','.join(p.device_time.astimezone(timezone.utc).isoformat() for p in self.isolated),str(self.revision)])
        return sha256(raw.encode()).hexdigest()[:16]
def _fit_rows(rows):
    anchor=rows[0].device_time.astimezone(timezone.utc)
    xs=[(p.device_time.astimezone(timezone.utc)-anchor).total_seconds() for p in rows]
    ys=[(p.trusted_time.astimezone(timezone.utc)-anchor).total_seconds() for p in rows]
    slopes=[(ys[j]-ys[i])/(xs[j]-xs[i]) for i in range(len(rows)) for j in range(i+1,len(rows)) if xs[j]>xs[i]]
    if not slopes: raise ValueError('degenerate synchronization span')
    slope=median(slopes)
    intercept=median([y-slope*x for x,y in zip(xs,ys)])
    residuals=[y-intercept-slope*x for x,y in zip(xs,ys)]
    return anchor,slope,intercept,residuals
def fit(points):
    """Robust drift fit: Theil-Sen median slope + median intercept.

    One bad sync point no longer bends the whole segment. Points whose
    residual exceeds max(3 robust sigmas, ISOLATION_FLOOR_SECONDS) are
    isolated and the model is refit without them; the isolated points are
    recorded on the Model so audit and billing can reference the exact
    correction via Model.key(). Isolation is only attempted when at least
    MIN_POINTS clean points remain -- with fewer, the outlier is not
    attributable and the whole segment is rejected instead of silently
    fitting a contaminated model. Reliable attribution of a single bad
    point needs redundancy: with three points an anomaly is detectable
    but not attributable (segment is rejected); five or more points let
    the median-based fit isolate it and continue.
    """
    rows=sorted(points,key=lambda p:p.device_time)
    if len(rows)<MIN_POINTS: raise ValueError('three sync points required')
    anchor,slope,intercept,residuals=_fit_rows(rows)
    isolated=(); revision=0
    center=median(residuals)
    scale=1.4826*median([abs(r-center) for r in residuals])
    threshold=max(ISOLATION_SIGMA*scale,ISOLATION_FLOOR_SECONDS)
    bad=[i for i,r in enumerate(residuals) if abs(r-center)>threshold]
    if bad:
        drop=set(bad); keep=[rows[i] for i in range(len(rows)) if i not in drop]
        if len(keep)<MIN_POINTS: raise ValueError('sync outliers detected but not isolable: segment rejected')
        isolated=tuple(rows[i] for i in bad)
        anchor,slope,intercept,_=_fit_rows(keep)
        revision=1
    if not MIN_SLOPE<=slope<=MAX_SLOPE: raise ValueError('unsupported drift')
    return Model(anchor,slope,intercept,isolated,revision)
def correct(model,at):
    x=(at.astimezone(timezone.utc)-model.anchor).total_seconds();return model.anchor+timedelta(seconds=model.intercept+model.slope*x)
def correct_series(sync,readings):
    m=fit(sync); out=[correct(m,r) for r in readings]
    if any(b<=a for a,b in zip(out,out[1:])): raise ValueError('non-monotone corrected times')
    return out
