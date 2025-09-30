import math

import polars as pl
import pytest

from cs2_analytics.data_processing.metrics import aggression


def test_t_side_average_distance_filters_to_first_window() -> None:
    positions = pl.DataFrame(
        {
            "round_number": [1, 1, 1, 2],
            "seconds_into_round": [10, 25, 35, 15],
            "player_id": ["t1", "t2", "t3", "t4"],
            "side": ["T", "T", "T", "T"],
            "zone": ["banana", "banana", "apps", "mid"],
        }
    )

    distances = pl.DataFrame(
        {
            "zone": ["banana", "apps", "mid"],
            "distance_to_bombsite": [15.0, 20.0, 10.0],
        }
    )

    result = aggression.t_side_average_distance(positions, distances)

    assert result.sort("round_number").to_dicts() == [
        {"round_number": 1, "avg_distance_to_bombsite": 15.0},
        {"round_number": 2, "avg_distance_to_bombsite": 10.0},
    ]


def test_ct_forward_presence_counts_unique_players() -> None:
    positions = pl.DataFrame(
        {
            "round_number": [1, 1, 1, 1],
            "seconds_into_round": [5, 12, 18, 35],
            "player_id": ["ct1", "ct2", "ct3", "ct4"],
            "side": ["CT", "CT", "CT", "CT"],
            "zone": ["arch", "arch", "library", "arch"],
        }
    )

    crossings = pl.DataFrame(
        {
            "player_id": ["ct1", "ct2", "ct3"],
            "chokepoint": ["arch", "arch", "library"],
        }
    )

    result = aggression.ct_forward_presence(positions, crossings)

    assert result.to_dicts() == [
        {"round_number": 1, "ct_forward_presence": 3},
    ]


def test_player_spacing_computes_average_pair_distance() -> None:
    positions = pl.DataFrame(
        {
            "round_number": [1, 1, 1],
            "seconds_into_round": [5, 5, 5],
            "side": ["T", "T", "T"],
            "player_id": ["t1", "t2", "t3"],
            "x": [0.0, 3.0, 0.0],
            "y": [0.0, 4.0, 1.0],
        }
    )

    result = aggression.player_spacing(positions)

    distances = result.filter(pl.col("side") == "T")["avg_player_spacing"].to_list()
    expected = (5.0 + 1.0 + math.sqrt(18.0)) / 3

    assert pytest.approx(distances[0], rel=1e-6) == expected
