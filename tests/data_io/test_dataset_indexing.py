import json
from pathlib import Path

import polars as pl

from cs2_analytics.data_io import DatasetIndexer, IndexingStrategy
from cs2_analytics.data_io.demos import ProcessedLayout


def _create_processed_demo(root: Path) -> None:
    layout = ProcessedLayout(root)
    layout.ensure_root()

    demo_stem = "match_inferno"
    tables_dir = layout.tables_dir(demo_stem)

    rounds = pl.DataFrame(
        {
            "round_number": [1, 2, 3],
            "tick": [120, 420, 780],
            "winning_side": ["CT", "T", "CT"],
        }
    )
    rounds.write_parquet(tables_dir / "rounds.parquet")

    players = pl.DataFrame(
        {
            "player_id": ["p1", "p2", "p3"],
            "round_number": [1, 2, 3],
            "damage": [12.5, 30.0, 55.0],
        }
    )
    players.write_parquet(tables_dir / "players.parquet")

    metadata_path = layout.metadata_path(demo_stem)
    metadata_path.write_text(json.dumps({"map": "de_inferno"}), encoding="utf-8")


def test_build_manifest_collects_index_metadata(tmp_path: Path) -> None:
    processed_root = tmp_path / "processed"
    _create_processed_demo(processed_root)

    strategy = IndexingStrategy(
        offset_columns={"rounds": ["round_number"]},
        bloom_filter_columns={"rounds": ["winning_side"]},
    )
    indexer = DatasetIndexer(processed_root, strategy=strategy)

    manifest = indexer.build_manifest()

    assert manifest["dataset_count"] == 1
    dataset = manifest["datasets"][0]
    assert dataset["demo_stem"] == "match_inferno"
    assert dataset["metadata_file"].endswith("metadata.json")

    rounds_table = next(
        table for table in dataset["tables"] if table["table_name"] == "rounds"
    )
    assert rounds_table["row_count"] == 3
    assert rounds_table["schema"]["round_number"].endswith("Int64")

    offset_entries = rounds_table["offset_index"]["round_number"]
    assert offset_entries[0]["value"] == 1
    assert offset_entries[0]["positions"] == [0]

    bloom = rounds_table["bloom_filters"]["winning_side"]
    assert bloom["num_bits"] == 2048
    assert bloom["hash_count"] == 3
    assert len(bloom["set_bits"]) >= 2


def test_write_manifest_persists_file(tmp_path: Path) -> None:
    processed_root = tmp_path / "processed"
    _create_processed_demo(processed_root)

    indexer = DatasetIndexer(processed_root)
    manifest_path = indexer.write_manifest()

    assert manifest_path == processed_root / "_manifest.json"
    assert manifest_path.exists()

    stored = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert stored["dataset_count"] == 1
    assert stored["datasets"][0]["demo_stem"] == "match_inferno"
