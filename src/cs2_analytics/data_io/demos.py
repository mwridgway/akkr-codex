"""Utilities for discovering and ingesting CS2 demo files."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from typing import Iterable, Iterator, Sequence

try:
    from awpy.parser import DemoParser
except ImportError:  # pragma: no cover - optional dependency at runtime
    DemoParser = None  # type: ignore[assignment]


@dataclass(frozen=True)
class DemoIngestionConfig:
    """Configuration values guiding demo discovery and ingestion.

    Attributes
    ----------
    source_root:
        Directory where raw `.dem` files are stored.
    processed_root:
        Base directory for serialized intermediate artifacts (Parquet, manifests).
    allowed_suffixes:
        File extensions considered valid demos. Defaults to (`.dem`,).
    """

    source_root: Path
    processed_root: Path
    allowed_suffixes: Sequence[str] = (".dem",)

    def validate(self) -> None:
        """Validate that configured directories exist."""

        if not self.source_root.exists():
            raise FileNotFoundError(f"Source root does not exist: {self.source_root}")
        if not self.source_root.is_dir():
            raise NotADirectoryError(f"Source root is not a directory: {self.source_root}")
        if not self.processed_root.exists():
            self.processed_root.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class ProcessedLayout:
    """Helpers for managing processed dataset structure."""

    root: Path
    tables_dirname: str = "tables"
    metadata_filename: str = "metadata.json"
    manifest_filename: str = "manifest.json"

    def ensure_root(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    def demo_dir(self, demo_stem: str) -> Path:
        path = self.root / demo_stem
        path.mkdir(parents=True, exist_ok=True)
        return path

    def tables_dir(self, demo_stem: str) -> Path:
        path = self.demo_dir(demo_stem) / self.tables_dirname
        path.mkdir(parents=True, exist_ok=True)
        return path

    def table_path(self, demo_stem: str, table_name: str) -> Path:
        return self.tables_dir(demo_stem) / f"{table_name}.parquet"

    def metadata_path(self, demo_stem: str) -> Path:
        return self.demo_dir(demo_stem) / self.metadata_filename

    def manifest_path(self) -> Path:
        return self.root / self.manifest_filename


class DemoIngestor:
    """High-level orchestrator for CS2 demo ingestion."""

    def __init__(self, config: DemoIngestionConfig) -> None:
        self._config = config
        self._config.validate()
        self._logger = logging.getLogger(self.__class__.__name__)
        self._layout = ProcessedLayout(config.processed_root)
        self._layout.ensure_root()

    def iter_demo_files(self) -> Iterator[Path]:
        """Yield paths to all tracked demo files under ``source_root``."""

        return (path for path in self._scan_sources())

    def list_demo_files(self) -> list[Path]:
        """Return a sorted list of available demo files."""

        return sorted(self._scan_sources())

    def ingest(self, paths: Iterable[Path] | None = None) -> None:
        """Ingest demo files into structured event tables.

        Parameters
        ----------
        paths:
            Specific demo paths to process. When ``None``, all known demos are processed.

        Notes
        -----
        The parsing pipeline will rely on `awpy`/`demoparser2` in subsequent iterations.
        This method currently acts as a placeholder to wire logging, manifests, and
        downstream hooks.
        """

        failures: list[tuple[Path, Exception]] = []

        for demo_path in paths or self.list_demo_files():
            try:
                self._process_single_demo(demo_path)
            except Exception as exc:  # pragma: no cover - runtime safety net
                self._logger.exception("Failed to ingest %s", demo_path.name)
                failures.append((demo_path, exc))

        if failures:
            names = ", ".join(path.name for path, _ in failures)
            raise RuntimeError(f"Failed to ingest demos: {names}") from failures[0][1]

    def _scan_sources(self) -> Iterator[Path]:
        """Internal helper that iterates over raw demo files."""

        for suffix in self._config.allowed_suffixes:
            yield from self._config.source_root.rglob(f"*{suffix}")

    def _process_single_demo(self, demo_path: Path) -> None:
        """Parse a single demo and emit structured artifacts."""

        if not demo_path.exists():
            raise FileNotFoundError(f"Demo file missing: {demo_path}")
        if DemoParser is None:
            raise ModuleNotFoundError(
                "awpy is required to parse demos. Install with `poetry add awpy`."
            )

        demo_dir = self._layout.demo_dir(demo_path.stem)

        self._logger.info("Parsing demo: %s", demo_path.name)
        try:
            parser = DemoParser(
                demofile=str(demo_path),
                demo_id=demo_path.stem,
                parse_rate=128,
                log=True,
            )
            parse_result = parser.parse()
        except Exception as exc:  # pragma: no cover - underlying parser failure
            raise RuntimeError(f"Parser failed for {demo_path.name}") from exc

        table_names = self._persist_parse_result(demo_path.stem, parse_result)
        self._write_metadata(demo_path.stem, parse_result, table_names)
        self._update_manifest(demo_path, parse_result, table_names)

    def _persist_parse_result(self, demo_stem: str, parse_result: dict) -> list[str]:
        """Persist parsed data frames as columnar files."""

        try:
            import polars as pl
        except ImportError as exc:  # pragma: no cover - optional dependency at runtime
            raise ModuleNotFoundError(
                "polars is required to persist parsed data. Install with `poetry add polars`."
            ) from exc

        tables: dict[str, "pl.DataFrame"] = {}
        for key, value in parse_result.items():
            if hasattr(value, "to_dict"):
                tables[key] = pl.DataFrame(value)

        written_tables: list[str] = []
        for name, frame in tables.items():
            path = self._layout.table_path(demo_stem, name)
            frame.write_parquet(path)
            written_tables.append(name)

        return written_tables

    def _write_metadata(
        self,
        demo_stem: str,
        parse_result: dict,
        table_names: list[str],
    ) -> None:
        metadata = {
            key: value
            for key, value in parse_result.items()
            if key not in table_names
        }

        metadata_path = self._layout.metadata_path(demo_stem)
        metadata_path.write_text(
            json.dumps(metadata, indent=2, default=str),
            encoding="utf-8",
        )

    def _update_manifest(
        self,
        demo_path: Path,
        parse_result: dict,
        table_names: list[str],
    ) -> None:
        """Append metadata about the parsed demo to a manifest file."""

        manifest_path = self._layout.manifest_path()
        manifest_entry = {
            "demo_name": demo_path.name,
            "demo_stem": demo_path.stem,
            "source": str(demo_path.resolve()),
            "output_dir": str(self._layout.demo_dir(demo_path.stem).resolve()),
            "tables_dir": str(self._layout.tables_dir(demo_path.stem).resolve()),
            "metadata_file": str(self._layout.metadata_path(demo_path.stem).resolve()),
            "total_rounds": parse_result.get("total_rounds"),
            "parser": "awpy",
            "tables": table_names,
            "processed_at": datetime.now(tz=timezone.utc).isoformat(),
        }

        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        current_entries = []
        if manifest_path.exists():
            try:
                current_entries = json.loads(manifest_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:  # pragma: no cover - defensive
                raise ValueError(f"Manifest file is corrupted: {manifest_path}") from exc

        existing = [entry for entry in current_entries if entry["demo_stem"] == demo_path.stem]
        current_entries = [entry for entry in current_entries if entry["demo_stem"] != demo_path.stem]
        if existing:
            self._logger.info("Updating existing manifest entry for %s", demo_path.stem)

        current_entries.append(manifest_entry)
        manifest_path.write_text(json.dumps(current_entries, indent=2), encoding="utf-8")
