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
    output_dir = processed_root / demo_path.stem
    output_dir.mkdir(parents=True)

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

    ingestor._update_manifest(demo_path, output_dir, parse_result)  # type: ignore[attr-defined]

    manifest_path = processed_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest[-1]["demo_name"] == "match.dem"
    assert manifest[-1]["total_rounds"] == 24
    assert manifest[-1]["parser"] == "awpy"

    parse_result["total_rounds"] = 30
    ingestor._update_manifest(demo_path, output_dir, parse_result)  # type: ignore[attr-defined]

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert len([entry for entry in manifest if entry["demo_stem"] == "match"]) == 1
    assert manifest[-1]["total_rounds"] == 30
