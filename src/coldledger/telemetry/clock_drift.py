from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Sequence


UTC = timezone.utc


@dataclass(frozen=True)
class SyncPoint:
    device_time: datetime
    trusted_time: datetime


@dataclass(frozen=True)
class Model:
    anchor: datetime
    slope: float
    intercept: float
    quarantined: tuple[SyncPoint, ...] = ()


@dataclass(frozen=True)
class FitResult:
    model: Model
    points: tuple[SyncPoint, ...]
    quarantined: tuple[SyncPoint, ...]
    residuals: tuple[float, ...]

    @property
    def excluded_points(self) -> tuple[SyncPoint, ...]:
        return self.quarantined

    @property
    def outliers(self) -> tuple[SyncPoint, ...]:
        return self.quarantined


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timezone-aware datetime required")
    return value.astimezone(UTC)


def _ordered(points: Iterable[SyncPoint]) -> list[SyncPoint]:
    return sorted(points, key=lambda p: _utc(p.device_time))


def _linear_fit(points: Sequence[SyncPoint]):
    if len(points) < 2:
        raise ValueError("two synchronization points required")
    anchor = _utc(points[0].device_time)
    xs = [(_utc(p.device_time) - anchor).total_seconds() for p in points]
    ys = [(_utc(p.trusted_time) - anchor).total_seconds() for p in points]
    dx = xs[-1] - xs[0]
    if dx == 0:
        raise ValueError("degenerate synchronization span")

    x_mean = sum(xs) / len(xs)
    y_mean = sum(ys) / len(ys)
    variance = sum((x - x_mean) ** 2 for x in xs)
    if variance == 0:
        raise ValueError("degenerate synchronization span")
    covariance = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    slope = covariance / variance
    intercept = y_mean - slope * x_mean
    residuals = tuple(y - (intercept + slope * x) for x, y in zip(xs, ys))
    model = Model(anchor, slope, intercept)
    return model, residuals


def fit_robust(
    points: Iterable[SyncPoint],
    *,
    residual_tolerance_seconds: float = 1.0,
    policy: str = "isolate",
) -> FitResult:
    """Fit a drift model, isolating at most one clearly bad synchronization point."""
    policy_aliases = {
        "reject": "reject",
        "reject_segment": "reject",
        "isolate": "isolate",
        "isolate_outlier": "isolate",
        "quarantine": "isolate",
    }
    policy = policy_aliases.get(policy)
    if policy is None:
        raise ValueError("policy must be 'reject' or 'isolate'")
    if residual_tolerance_seconds < 0:
        raise ValueError("residual tolerance cannot be negative")

    rows = _ordered(points)
    if len(rows) < 3:
        raise ValueError("three sync points required")

    try:
        model, residuals = _linear_fit(rows)
        if 0.95 <= model.slope <= 1.05 and all(
            abs(r) <= residual_tolerance_seconds for r in residuals
        ):
            return FitResult(model, tuple(rows), (), residuals)
    except ValueError:
        if policy != "isolate":
            raise
        model = None

    if policy == "reject":
        if model is None:
            raise ValueError("degenerate synchronization span")
        if not 0.95 <= model.slope <= 1.05:
            raise ValueError("unsupported drift")
        raise ValueError("synchronization outlier")

    candidates = []
    indexes = range(len(rows))
    for removed_index in indexes:
        kept = [p for i, p in enumerate(rows) if i != removed_index]
        try:
            candidate, kept_residuals = _linear_fit(kept)
        except ValueError:
            continue
        if not 0.95 <= candidate.slope <= 1.05:
            continue
        if any(abs(r) > residual_tolerance_seconds for r in kept_residuals):
            continue

        removed = rows[removed_index]
        removed_x = (_utc(removed.device_time) - candidate.anchor).total_seconds()
        removed_y = (_utc(removed.trusted_time) - candidate.anchor).total_seconds()
        removed_residual = removed_y - (candidate.intercept + candidate.slope * removed_x)
        if abs(removed_residual) <= residual_tolerance_seconds:
            continue
        candidates.append((abs(removed_residual), removed_index, candidate, kept, removed_residual))

    if not candidates:
        if model is not None and not 0.95 <= model.slope <= 1.05:
            raise ValueError("unsupported drift")
        raise ValueError("synchronization outlier")

    _, removed_index, candidate, kept, _ = max(candidates, key=lambda c: (c[0], -c[1]))
    quarantined = (rows[removed_index],)

    anchor = _utc(kept[0].device_time)
    xs = [(_utc(p.device_time) - anchor).total_seconds() for p in kept]
    ys = [(_utc(p.trusted_time) - anchor).total_seconds() for p in kept]
    x_mean = sum(xs) / len(xs)
    y_mean = sum(ys) / len(ys)
    variance = sum((x - x_mean) ** 2 for x in xs)
    slope = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys)) / variance
    intercept = y_mean - slope * x_mean
    residuals = tuple(y - (intercept + slope * x) for x, y in zip(xs, ys))
    robust_model = Model(anchor, slope, intercept, quarantined)
    return FitResult(robust_model, tuple(kept), quarantined, residuals)


def fit(
    points: Iterable[SyncPoint],
    *,
    residual_tolerance_seconds: float = 1.0,
    policy: str = "isolate",
) -> Model:
    return fit_robust(
        points,
        residual_tolerance_seconds=residual_tolerance_seconds,
        policy=policy,
    ).model


def correct(model: Model, at: datetime) -> datetime:
    x = (_utc(at) - model.anchor).total_seconds()
    return model.anchor + timedelta(seconds=model.intercept + model.slope * x)


def correct_series(
    sync: Iterable[SyncPoint],
    readings: Iterable[datetime],
    *,
    residual_tolerance_seconds: float = 1.0,
    policy: str = "isolate",
) -> list[datetime]:
    """Correct readings while preserving their arrival order.

    Readings may arrive late.  Ordering is therefore checked on event-time order,
    not on the order in which readings were received.
    """
    result = fit_robust(
        sync,
        residual_tolerance_seconds=residual_tolerance_seconds,
        policy=policy,
    )
    values = list(readings)
    ordered = sorted(values, key=_utc)
    corrected_ordered = [correct(result.model, value) for value in ordered]
    duplicates = any(b <= a for a, b in zip(corrected_ordered, corrected_ordered[1:]))
    if duplicates:
        raise ValueError("non-monotone corrected times")
    corrected_by_raw = {id(value): correct(result.model, value) for value in values}
    return [corrected_by_raw[id(value)] for value in values]
