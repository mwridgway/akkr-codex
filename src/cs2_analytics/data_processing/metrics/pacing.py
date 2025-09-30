"""Pacing metrics derived from CS2 demo event streams."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal

import polars as pl


LazyOrFrame = pl.LazyFrame | pl.DataFrame
ReturnMode = Literal["dataframe", "lazy"]


@dataclass(frozen=True)
class PacingColumns:
    """Column mappings for pacing metric computations."""

    round: str = "round_number"
    seconds: str = "seconds_into_round"


def _ensure_lazy(frame: LazyOrFrame) -> pl.LazyFrame:
    if isinstance(frame, pl.LazyFrame):
        return frame
    if isinstance(frame, pl.DataFrame):
        return frame.lazy()
    raise TypeError("Expected a Polars DataFrame or LazyFrame")


def _finalize(result: pl.LazyFrame, mode: ReturnMode) -> LazyOrFrame:
    if mode == "lazy":
        return result
    return result.collect()


def time_to_first_kill(
    kill_events: LazyOrFrame,
    *,
    columns: PacingColumns | None = None,
    mode: ReturnMode = "dataframe",
) -> LazyOrFrame:
    """Compute time to first kill per round.

    Parameters
    ----------
    kill_events:
        Polars frame containing one row per kill with round and seconds columns.
    columns:
        Optional column name overrides. Defaults to ``round_number`` and ``seconds_into_round``.
    mode:
        ``"dataframe"`` returns a collected ``pl.DataFrame``; ``"lazy"`` returns ``pl.LazyFrame``.
    """

    cols = columns or PacingColumns()
    lf = _ensure_lazy(kill_events)

    result = (
        lf.group_by(cols.round)
        .agg(pl.col(cols.seconds).min().alias("time_to_first_kill"))
        .sort(cols.round)
    )
    return _finalize(result, mode)


def time_to_bomb_plant(
    plant_events: LazyOrFrame,
    *,
    columns: PacingColumns | None = None,
    mode: ReturnMode = "dataframe",
) -> LazyOrFrame:
    """Compute time to bomb plant per round.

    ``plant_events`` should be pre-filtered to contain only plant actions.
    """

    cols = columns or PacingColumns()
    lf = _ensure_lazy(plant_events)

    result = (
        lf.group_by(cols.round)
        .agg(pl.col(cols.seconds).min().alias("time_to_bomb_plant"))
        .sort(cols.round)
    )
    return _finalize(result, mode)


def average_death_timestamp(
    death_events: LazyOrFrame,
    *,
    columns: PacingColumns | None = None,
    mode: ReturnMode = "dataframe",
) -> LazyOrFrame:
    """Compute average death timestamp per round."""

    cols = columns or PacingColumns()
    lf = _ensure_lazy(death_events)

    result = (
        lf.group_by(cols.round)
        .agg(pl.col(cols.seconds).mean().alias("average_death_seconds"))
        .sort(cols.round)
    )
    return _finalize(result, mode)


def summarize_pacing(
    *,
    ttfk: LazyOrFrame,
    ttbp: LazyOrFrame,
    avg_death: LazyOrFrame,
    mode: ReturnMode = "dataframe",
) -> LazyOrFrame:
    """Join pacing metrics on round number to create a unified table."""

    frames: tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame] = (
        _ensure_lazy(ttfk),
        _ensure_lazy(ttbp),
        _ensure_lazy(avg_death),
    )

    round_frames = [frame.select("round_number") for frame in frames]
    rounds = pl.concat(round_frames).unique()

    result = rounds
    for frame in frames:
        result = result.join(frame, on="round_number", how="left")

    return _finalize(result.sort("round_number"), mode)
