import polars as pl
import pytest

from cs2_analytics.data_processing.metrics import rotations


ROTATION_SAMPLE = pl.DataFrame(
    {
        "round_number": [1, 2, 3, 4],
        "rotator_id": ["p1", "p1", "p2", "p1"],
        "rotation_path": ["A_to_B", "A_to_B", "B_to_A", "A_to_B"],
        "trigger_time": [20.0, 22.0, 30.0, 10.0],
        "arrival_time": [40.0, 43.0, 54.0, 35.0],
        "round_won": [True, False, True, True],
        "engagement_success": [True, False, False, True],
    }
)


def test_rotation_timing_computes_average_travel_time() -> None:
    result = rotations.rotation_timing(ROTATION_SAMPLE)

    records = result.sort(["rotator_id", "rotation_path"]).to_dicts()
    assert len(records) == 2
    assert pytest.approx(records[0]["avg_rotation_seconds"], rel=1e-6) == 22.0


def test_rotation_success_rate_returns_fraction_of_round_wins() -> None:
    result = rotations.rotation_success_rate(ROTATION_SAMPLE)

    records = result.sort(["rotator_id", "rotation_path"]).to_dicts()
    assert pytest.approx(records[0]["rotation_success_rate"], rel=1e-6) == pytest.approx(
        2 / 3, rel=1e-6
    )


def test_engagement_success_rate_tracks_trade_or_assist_outcomes() -> None:
    result = rotations.engagement_success_rate(ROTATION_SAMPLE)

    records = result.sort(["rotator_id", "rotation_path"]).to_dicts()
    assert pytest.approx(records[0]["engagement_success_rate"], rel=1e-6) == pytest.approx(
        2 / 3, rel=1e-6
    )


def test_rotation_analyzer_summarize_combines_metrics_and_threshold() -> None:
    analyzer = rotations.RotationAnalyzer(travel_time_threshold=22.5)
    summary = analyzer.summarize(ROTATION_SAMPLE).sort(["rotator_id", "rotation_path"])

    assert summary.shape == (2, 6)

    first = summary.row(0, named=True)
    assert pytest.approx(first["avg_rotation_seconds"], rel=1e-6) == 22.0
    assert pytest.approx(first["rotation_success_rate"], rel=1e-6) == pytest.approx(
        2 / 3, rel=1e-6
    )
    assert pytest.approx(first["engagement_success_rate"], rel=1e-6) == pytest.approx(
        2 / 3, rel=1e-6
    )
    assert pytest.approx(first["fast_rotation_rate"], rel=1e-6) == pytest.approx(
        2 / 3, rel=1e-6
    )


def test_rotation_analyzer_respects_lazy_mode() -> None:
    analyzer = rotations.RotationAnalyzer(mode="lazy")
    lazy_result = analyzer.timing(ROTATION_SAMPLE.lazy())

    assert isinstance(lazy_result, pl.LazyFrame)
    collected = lazy_result.collect()
    assert collected.shape == (2, 3)
