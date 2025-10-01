"""Rotational efficiency metrics for CS2 rounds."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Literal

import polars as pl

LazyOrFrame = pl.LazyFrame | pl.DataFrame
ReturnMode = Literal["dataframe", "lazy"]


@dataclass(frozen=True)
class RotationColumns:
    """Column mappings used for computing rotation metrics."""

    round: str = "round_number"
    rotator: str = "rotator_id"
    path: str = "rotation_path"
    trigger: str = "trigger_time"
    arrival: str = "arrival_time"
    round_win: str = "round_won"
    engagement_success: str = "engagement_success"


def _ensure_lazy(frame: LazyOrFrame) -> pl.LazyFrame:
    if isinstance(frame, pl.LazyFrame):
        return frame
    if isinstance(frame, pl.DataFrame):
        return frame.lazy()
    raise TypeError("Expected Polars DataFrame or LazyFrame")


def _finalize(result: pl.LazyFrame, mode: ReturnMode) -> LazyOrFrame:
    return result if mode == "lazy" else result.collect()


def _coerce_group_keys(
    columns: RotationColumns, group_keys: Sequence[str] | None
) -> list[str]:
    if group_keys is None:
        return [columns.rotator, columns.path]
    return list(group_keys)


def _with_travel_time(frame: pl.LazyFrame, columns: RotationColumns) -> pl.LazyFrame:
    return frame.with_columns(
        (
            pl.col(columns.arrival) - pl.col(columns.trigger)
        ).alias("rotation_travel_time")
    )


def rotation_timing(
    rotation_events: LazyOrFrame,
    *,
    columns: RotationColumns | None = None,
    group_keys: Sequence[str] | None = None,
    mode: ReturnMode = "dataframe",
) -> LazyOrFrame:
    """Average travel time for rotations grouped by the provided keys."""

    cols = columns or RotationColumns()
    keys = _coerce_group_keys(cols, group_keys)
    lf = _with_travel_time(_ensure_lazy(rotation_events), cols)

    result = lf.group_by(keys).agg(
        pl.col("rotation_travel_time").mean().alias("avg_rotation_seconds")
    )

    if keys:
        result = result.sort(keys)

    return _finalize(result, mode)


def rotation_success_rate(
    rotation_events: LazyOrFrame,
    *,
    columns: RotationColumns | None = None,
    group_keys: Sequence[str] | None = None,
    mode: ReturnMode = "dataframe",
) -> LazyOrFrame:
    """Share of rotations that resulted in a round win."""

    cols = columns or RotationColumns()
    keys = _coerce_group_keys(cols, group_keys)
    lf = _ensure_lazy(rotation_events)

    result = lf.group_by(keys).agg(
        pl.col(cols.round_win)
        .cast(pl.Float64)
        .mean()
        .alias("rotation_success_rate")
    )

    if keys:
        result = result.sort(keys)

    return _finalize(result, mode)


def engagement_success_rate(
    rotation_events: LazyOrFrame,
    *,
    columns: RotationColumns | None = None,
    group_keys: Sequence[str] | None = None,
    mode: ReturnMode = "dataframe",
) -> LazyOrFrame:
    """Share of rotations where the rotator secured an impactful engagement."""

    cols = columns or RotationColumns()
    keys = _coerce_group_keys(cols, group_keys)
    lf = _ensure_lazy(rotation_events)

    result = lf.group_by(keys).agg(
        pl.col(cols.engagement_success)
        .cast(pl.Float64)
        .mean()
        .alias("engagement_success_rate")
    )

    if keys:
        result = result.sort(keys)

    return _finalize(result, mode)


@dataclass
class RotationAnalyzer:
    """Helper for computing rotational efficiency metrics with shared settings."""

    columns: RotationColumns = RotationColumns()
    mode: ReturnMode = "dataframe"
    travel_time_threshold: float | None = None
    group_keys: Sequence[str] | None = None

    def _keys(self) -> list[str]:
        return _coerce_group_keys(self.columns, self.group_keys)

    def timing(self, rotation_events: LazyOrFrame) -> LazyOrFrame:
        return rotation_timing(
            rotation_events,
            columns=self.columns,
            group_keys=self._keys(),
            mode=self.mode,
        )

    def success_rate(self, rotation_events: LazyOrFrame) -> LazyOrFrame:
        return rotation_success_rate(
            rotation_events,
            columns=self.columns,
            group_keys=self._keys(),
            mode=self.mode,
        )

    def engagement_rate(self, rotation_events: LazyOrFrame) -> LazyOrFrame:
        return engagement_success_rate(
            rotation_events,
            columns=self.columns,
            group_keys=self._keys(),
            mode=self.mode,
        )

    def summarize(self, rotation_events: LazyOrFrame) -> LazyOrFrame:
        """Return a consolidated table with timing, success, and engagement metrics."""

        cols = self.columns
        keys = self._keys()
        lf = _with_travel_time(_ensure_lazy(rotation_events), cols)

        aggregations: list[pl.Expr] = [
            pl.col("rotation_travel_time").mean().alias("avg_rotation_seconds"),
            pl.col(cols.round_win)
            .cast(pl.Float64)
            .mean()
            .alias("rotation_success_rate"),
            pl.col(cols.engagement_success)
            .cast(pl.Float64)
            .mean()
            .alias("engagement_success_rate"),
        ]

        if self.travel_time_threshold is not None:
            aggregations.append(
                (pl.col("rotation_travel_time") <= self.travel_time_threshold)
                .cast(pl.Float64)
                .mean()
                .alias("fast_rotation_rate"),
            )

        result = lf.group_by(keys).agg(aggregations)

        if keys:
            result = result.sort(keys)

        return _finalize(result, self.mode)

