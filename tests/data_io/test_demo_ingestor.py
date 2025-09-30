from pathlib import Path

import json

from cs2_analytics.data_io import DemoIngestionConfig, DemoIngestor


def touch_demo(path: Path, name: str) -> Path:
    demo_path = path / name
    demo_path.write_bytes(b"")
    return demo_path


def test_list_demo_files_returns_sorted_paths(tmp_path: Path) -> None:
    source_root = tmp_path / "raw"
    processed_root = tmp_path / "processed"
    source_root.mkdir()

    files = [
        touch_demo(source_root, "match2.dem"),
        touch_demo(source_root, "match1.dem"),
    ]

    config = DemoIngestionConfig(
        source_root=source_root,
        processed_root=processed_root,
    )
    ingestor = DemoIngestor(config)

    listed = ingestor.list_demo_files()

    assert listed == sorted(files)


def test_iter_demo_files_yields_each_path(tmp_path: Path) -> None:
    source_root = tmp_path / "raw"
    processed_root = tmp_path / "processed"
    source_root.mkdir()

    expected = {
        touch_demo(source_root, "a.dem"),
        touch_demo(source_root, "b.dem"),
    }

    config = DemoIngestionConfig(
        source_root=source_root,
        processed_root=processed_root,
    )
    ingestor = DemoIngestor(config)

    assert set(ingestor.iter_demo_files()) == expected


class _DummyTable:
    def __init__(self, data: dict):
        self._data = data

    def to_dict(self) -> dict:
        return self._data


def test_update_manifest_records_metadata(tmp_path: Path) -> None:
    source_root = tmp_path / "raw"
    processed_root = tmp_path / "processed"
    source_root.mkdir()

    demo_path = touch_demo(source_root, "match.dem")
    config = DemoIngestionConfig(
        source_root=source_root,
        processed_root=processed_root,
    )
    ingestor = DemoIngestor(config)

    parse_result = {
        "total_rounds": 24,
        "events": _DummyTable({"tick": [1, 2]}),
        "metadata": {"map": "inferno"},
    }

    table_names = ["events"]
    ingestor._update_manifest(demo_path, parse_result, table_names)  # type: ignore[attr-defined]

    manifest_path = processed_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest[-1]["demo_name"] == "match.dem"
    assert manifest[-1]["total_rounds"] == 24
    assert manifest[-1]["parser"] == "awpy"
    assert manifest[-1]["tables"] == ["events"]
    assert manifest[-1]["metadata_file"].endswith("metadata.json")

    parse_result["total_rounds"] = 30
    ingestor._update_manifest(demo_path, parse_result, table_names)  # type: ignore[attr-defined]

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert len([entry for entry in manifest if entry["demo_stem"] == "match"]) == 1
    assert manifest[-1]["total_rounds"] == 30


def test_write_metadata_dumps_non_table_entries(tmp_path: Path) -> None:
    source_root = tmp_path / "raw"
    processed_root = tmp_path / "processed"
    source_root.mkdir()

    demo_path = touch_demo(source_root, "match.dem")

    config = DemoIngestionConfig(
        source_root=source_root,
        processed_root=processed_root,
    )
    ingestor = DemoIngestor(config)

    parse_result = {
        "events": _DummyTable({"tick": [1]}),
        "total_rounds": 10,
        "score": {"t": 6, "ct": 4},
        "map": "nuke",
    }

    table_names = ["events"]
    ingestor._write_metadata(demo_path.stem, parse_result, table_names)  # type: ignore[attr-defined]

    metadata_path = processed_root / demo_path.stem / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    assert metadata["total_rounds"] == 10
    assert metadata["score"] == {"t": 6, "ct": 4}
    assert metadata["map"] == "nuke"


def test_layout_creates_tables_directory(tmp_path: Path) -> None:
    source_root = tmp_path / "raw"
    processed_root = tmp_path / "processed"
    source_root.mkdir()

    touch_demo(source_root, "game.dem")

    config = DemoIngestionConfig(
        source_root=source_root,
        processed_root=processed_root,
    )
    ingestor = DemoIngestor(config)

    table_path = ingestor._layout.table_path("game", "events")  # type: ignore[attr-defined]

    assert table_path.parent.name == "tables"
    assert table_path.parent.exists()
    assert table_path.parent.parent == processed_root / "game"
