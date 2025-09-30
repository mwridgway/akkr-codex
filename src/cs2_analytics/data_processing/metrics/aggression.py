"""Aggression metrics for CS2 rounds."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import polars as pl

ReturnMode = Literal["dataframe", "lazy"]
LazyOrFrame = pl.LazyFrame | pl.DataFrame


@dataclass(frozen=True)
class AggressionColumns:
    round: str = "round_number"
    seconds: str = "seconds_into_round"
    team: str = "team"
    player: str = "player_id"
    x: str = "x"
    y: str = "y"
    z: str | None = None
    side: str = "side"
    zone: str = "zone"


def _ensure_lazy(frame: LazyOrFrame) -> pl.LazyFrame:
    if isinstance(frame, pl.LazyFrame):
        return frame
    if isinstance(frame, pl.DataFrame):
        return frame.lazy()
    raise TypeError("Expected Polars DataFrame or LazyFrame")


def _finalize(result: pl.LazyFrame, mode: ReturnMode) -> LazyOrFrame:
    return result if mode == "lazy" else result.collect()


def t_side_average_distance(
    player_positions: LazyOrFrame,
    bombsite_distances: LazyOrFrame,
    *,
    columns: AggressionColumns | None = None,
    cutoff_seconds: int = 30,
    mode: ReturnMode = "dataframe",
) -> LazyOrFrame:
    """Average distance of T players to nearest bombsite in the opening window."""

    cols = columns or AggressionColumns()
    positions = _ensure_lazy(player_positions)
    distances = _ensure_lazy(bombsite_distances)

    tsides = positions.filter(pl.col(cols.side) == "T")
    within_cutoff = tsides.filter(pl.col(cols.seconds) <= cutoff_seconds)

    joined = within_cutoff.join(
        distances,
        left_on=cols.zone,
        right_on=cols.zone,
        how="left",
    )

    result = (
        joined.group_by(cols.round)
        .agg(pl.col("distance_to_bombsite").mean().alias("avg_distance_to_bombsite"))
        .sort(cols.round)
    )

    return _finalize(result, mode)


def ct_forward_presence(
    player_positions: LazyOrFrame,
    chokepoint_crossings: LazyOrFrame,
    *,
    columns: AggressionColumns | None = None,
    cutoff_seconds: int = 30,
    mode: ReturnMode = "dataframe",
) -> LazyOrFrame:
    """Count CT players crossing chokepoints in the opening window."""

    cols = columns or AggressionColumns()
    positions = _ensure_lazy(player_positions)
    crossings = _ensure_lazy(chokepoint_crossings)

    cts = positions.filter(pl.col(cols.side) == "CT")
    within_cutoff = cts.filter(pl.col(cols.seconds) <= cutoff_seconds)

    joined = within_cutoff.join(
        crossings,
        left_on=[cols.player, cols.zone],
        right_on=["player_id", "chokepoint"],
        how="inner",
    )

    result = (
        joined.group_by(cols.round)
        .agg(pl.col(cols.player).n_unique().alias("ct_forward_presence"))
        .sort(cols.round)
    )
    return _finalize(result, mode)


def player_spacing(
    player_positions: LazyOrFrame,
    *,
    columns: AggressionColumns | None = None,
    cutoff_seconds: int | None = None,
    mode: ReturnMode = "dataframe",
) -> LazyOrFrame:
    """Average distance between teammates within a round."""

    cols = columns or AggressionColumns()
    positions = _ensure_lazy(player_positions)

    filtered = positions
    if cutoff_seconds is not None:
        filtered = filtered.filter(pl.col(cols.seconds) <= cutoff_seconds)

    joined = filtered.join(
        filtered,
        on=[cols.round, cols.seconds, cols.side],
        how="inner",
        suffix="_other",
    )

    pairs = joined.filter(pl.col(cols.player) < pl.col(f"{cols.player}_other"))

    coord_columns = [cols.x, cols.y]
    if cols.z is not None and cols.z in filtered.columns:
        coord_columns.append(cols.z)

    distance_expr = None
    for coord in coord_columns:
        term = (pl.col(coord) - pl.col(f"{coord}_other")) ** 2
        distance_expr = term if distance_expr is None else distance_expr + term

    if distance_expr is None:
        raise ValueError("No positional columns available to compute spacing")

    pairs = pairs.with_columns(distance_expr.sqrt().alias("pair_distance"))

    result = (
        pairs.group_by(cols.round, pl.col(cols.side).alias("side"))
        .agg(pl.col("pair_distance").mean().alias("avg_player_spacing"))
        .sort([cols.round, "side"])
    )

    return _finalize(result, mode)
