import polars as pl

from cs2_analytics.data_processing.metrics import pacing


def test_time_to_first_kill_returns_min_time_per_round() -> None:
    frame = pl.DataFrame(
        {
            "round_number": [1, 1, 2, 2],
            "seconds_into_round": [5.2, 12.0, 3.0, 7.5],
        }
    )

    result = pacing.time_to_first_kill(frame)

    assert result.sort("round_number").to_dicts() == [
        {"round_number": 1, "time_to_first_kill": 5.2},
        {"round_number": 2, "time_to_first_kill": 3.0},
    ]


def test_time_to_bomb_plant_accepts_lazy_mode() -> None:
    frame = pl.DataFrame(
        {
            "round_number": [1, 2, 3],
            "seconds_into_round": [24.0, 40.0, 18.0],
        }
    )

    result = pacing.time_to_bomb_plant(frame.lazy(), mode="lazy")

    assert isinstance(result, pl.LazyFrame)
    collected = result.collect()
    assert collected.select(pl.col("time_to_bomb_plant").max()).item() == 40.0


def test_average_death_timestamp_computes_mean() -> None:
    frame = pl.DataFrame(
        {
            "round_number": [1, 1, 1, 2, 2],
            "seconds_into_round": [10, 20, 30, 15, 35],
        }
    )

    result = pacing.average_death_timestamp(frame)

    assert result.filter(pl.col("round_number") == 1)["average_death_seconds"].item() == 20
    assert result.filter(pl.col("round_number") == 2)["average_death_seconds"].item() == 25


def test_summarize_pacing_outer_joins_all_metrics() -> None:
    ttfk = pl.DataFrame({"round_number": [1, 2], "time_to_first_kill": [5.0, 6.0]})
    ttbp = pl.DataFrame({"round_number": [2], "time_to_bomb_plant": [30.0]})
    avg_death = pl.DataFrame({"round_number": [1, 3], "average_death_seconds": [18.0, 22.0]})

    summary = pacing.summarize_pacing(
        ttfk=ttfk,
        ttbp=ttbp,
        avg_death=avg_death,
    )

    summary_dicts = summary.sort("round_number").to_dicts()

    assert summary.shape == (3, 4)
    assert summary_dicts[-1]["round_number"] == 3
    assert summary_dicts[-1]["time_to_first_kill"] is None


def test_custom_column_mapping_supported() -> None:
    frame = pl.DataFrame(
        {
            "round_id": [1, 1, 2],
            "seconds": [8.0, 12.0, 4.0],
        }
    )

    config = pacing.PacingColumns(round="round_id", seconds="seconds")
    result = pacing.time_to_first_kill(frame, columns=config)

    assert result.sort("round_id").to_dicts() == [
        {"round_id": 1, "time_to_first_kill": 8.0},
        {"round_id": 2, "time_to_first_kill": 4.0},
    ]
