"""Indexing and manifest utilities for processed CS2 datasets."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Sequence

import polars as pl

from .demos import ProcessedLayout


def _coerce_json(value: Any) -> Any:
    """Convert Polars and Path values into JSON-serialisable types."""

    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (pl.Series, pl.Expr)):
        return value.to_list()
    if isinstance(value, pl.DataFrame):
        return value.to_dict(as_series=False)
    if isinstance(value, (pl.Enum,)):
        return str(value)
    return value


@dataclass(frozen=True)
class IndexingStrategy:
    """Configuration for building dataset indices and bloom filters."""

    offset_columns: dict[str, Sequence[str]] = field(default_factory=dict)
    bloom_filter_columns: dict[str, Sequence[str]] = field(default_factory=dict)
    numeric_statistics: bool = True

    def _merged(self, table_name: str, mapping: dict[str, Sequence[str]]) -> list[str]:
        columns: list[str] = []
        if "*" in mapping:
            columns.extend(mapping["*"])
        if table_name in mapping:
            columns.extend(mapping[table_name])
        return columns

    def offsets_for(self, table_name: str) -> list[str]:
        return self._merged(table_name, self.offset_columns)

    def blooms_for(self, table_name: str) -> list[str]:
        return self._merged(table_name, self.bloom_filter_columns)


class DatasetIndexer:
    """Builds dataset-level manifests with lightweight indexing metadata."""

    def __init__(self, processed_root: Path, *, strategy: IndexingStrategy | None = None) -> None:
        self._layout = ProcessedLayout(processed_root)
        self._strategy = strategy or IndexingStrategy()

    def build_manifest(self) -> dict[str, Any]:
        """Create an in-memory manifest describing processed demos and tables."""

        datasets: list[dict[str, Any]] = []

        for demo_dir in sorted(self._layout.root.iterdir()):
            if not demo_dir.is_dir():
                continue

            tables_dir = demo_dir / self._layout.tables_dirname
            if not tables_dir.exists():
                continue

            tables = [
                self._summarise_table(table_path)
                for table_path in sorted(tables_dir.glob("*.parquet"))
            ]

            if not tables:
                continue

            metadata_file = self._layout.metadata_path(demo_dir.name)
            datasets.append(
                {
                    "demo_stem": demo_dir.name,
                    "demo_dir": str(demo_dir.resolve()),
                    "metadata_file": str(metadata_file.resolve()) if metadata_file.exists() else None,
                    "tables": tables,
                }
            )

        manifest = {
            "generated_at": datetime.now(tz=timezone.utc).isoformat(),
            "dataset_count": len(datasets),
            "datasets": datasets,
        }
        return manifest

    def write_manifest(self) -> Path:
        """Persist the dataset manifest to ``_manifest.json`` under ``processed_root``."""

        manifest = self.build_manifest()
        manifest_path = self._layout.global_manifest_path()
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest, indent=2, default=_coerce_json), encoding="utf-8")
        return manifest_path

    def _summarise_table(self, table_path: Path) -> dict[str, Any]:
        table_name = table_path.stem
        frame = pl.scan_parquet(str(table_path)).collect()

        schema = {name: str(dtype) for name, dtype in frame.schema.items()}
        row_count = frame.height

        stats: dict[str, dict[str, Any]] = {}
        if self._strategy.numeric_statistics:
            numeric_columns = [
                name for name, dtype in frame.schema.items() if dtype.is_numeric()
            ]
            if numeric_columns:
                min_select = [pl.col(name).min().alias(name) for name in numeric_columns]
                max_select = [pl.col(name).max().alias(name) for name in numeric_columns]
                min_values = frame.select(min_select).row(0, named=True)
                max_values = frame.select(max_select).row(0, named=True)
                stats = {
                    "min": {name: _coerce_json(value) for name, value in min_values.items()},
                    "max": {name: _coerce_json(value) for name, value in max_values.items()},
                }

        offset_index = self._build_offset_index(frame, table_name)
        bloom_filters = self._build_bloom_filters(frame, table_name)

        return {
            "table_name": table_name,
            "path": str(table_path.resolve()),
            "file_size_bytes": table_path.stat().st_size,
            "row_count": row_count,
            "schema": schema,
            "stats": stats,
            "offset_index": offset_index,
            "bloom_filters": bloom_filters,
        }

    def _build_offset_index(self, frame: pl.DataFrame, table_name: str) -> dict[str, list[dict[str, Any]]]:
        columns = [col for col in self._strategy.offsets_for(table_name) if col in frame.columns]
        if not columns:
            return {}

        indexed = frame.with_row_index("row_number")
        result: dict[str, list[dict[str, Any]]] = {}
        for column in columns:
            grouped = indexed.group_by(column).agg(pl.col("row_number")).sort(column)
            entries: list[dict[str, Any]] = []
            for record in grouped.iter_rows(named=True):
                value = record[column]
                positions = [int(pos) for pos in record["row_number"]]
                entries.append({"value": _coerce_json(value), "positions": positions})
            result[column] = entries
        return result

    def _build_bloom_filters(self, frame: pl.DataFrame, table_name: str) -> dict[str, dict[str, Any]]:
        columns = [col for col in self._strategy.blooms_for(table_name) if col in frame.columns]
        if not columns:
            return {}

        bloom_filters: dict[str, dict[str, Any]] = {}
        for column in columns:
            series = frame[column].drop_nulls()
            bloom_filters[column] = self._create_bloom_filter(series.to_list())
        return bloom_filters

    def _create_bloom_filter(
        self,
        values: Iterable[Any],
        *,
        num_bits: int = 2048,
        hash_count: int = 3,
    ) -> dict[str, Any]:
        bitset = [0] * num_bits
        for value in values:
            encoded = str(value).encode("utf-8")
            for salt in range(hash_count):
                digest = hashlib.blake2b(encoded, digest_size=8, person=bytes([salt]))
                index = int.from_bytes(digest.digest(), "big") % num_bits
                bitset[index] = 1

        set_bits = [idx for idx, bit in enumerate(bitset) if bit]
        return {
            "num_bits": num_bits,
            "hash_count": hash_count,
            "set_bits": set_bits,
        }
