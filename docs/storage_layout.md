# Processed Data Layout

Processed CS2 demo artifacts follow a consistent directory structure rooted at `data/processed/`:

```
data/processed/
└── <demo_stem>/
    ├── metadata.json      # Non-tabular attributes (scores, map, totals)
    └── tables/
        ├── events.parquet
        ├── players.parquet
        └── ...
```

- `tables/` contains one Parquet file per parsed awpy table.
- `metadata.json` stores scalar and dictionary values emitted by the parser that are not written as tables (e.g., map name, total rounds, score summaries).
- `manifest.json` in the root indexes all demos with absolute paths, parser metadata, and processing timestamps.

See `cs2_analytics.data_io.demos.ProcessedLayout` for helper methods that enforce this hierarchy during ingestion.
