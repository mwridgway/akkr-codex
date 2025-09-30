# Progress Tracking Workflow

This repository tracks execution progress with a structured plan file and a lightweight CLI helper.

## Plan File
- Location: `docs/plan.yml`
- Structure: Phases (e.g., Phase 1 – Data Foundations) containing task items with `status` (`todo`, `in_progress`, `done`) and optional notes.
- The file mirrors the milestones in `IMPLEMENTATION_PLAN.md` and should be treated as the authoritative backlog snapshot.

## CLI Helper
- Script: `scripts/plan_tracker.py`
- Dependencies: Python 3.11+ and `PyYAML` (`pip install pyyaml` if not already available).

### Commands
- List current progress:
  ```bash
  python scripts/plan_tracker.py list --verbose
  ```
- Update a task status and notes:
  ```bash
  python scripts/plan_tracker.py set "Phase 1 – Data Foundations" --task "Ingestion Pipeline Skeleton" --status in_progress --notes "Parsing hooked into awpy"
  ```
- Update an entire phase:
  ```bash
  python scripts/plan_tracker.py set "Phase 1 – Data Foundations" --status done
  ```
- Add new tasks inside an existing phase:
  ```bash
  python scripts/plan_tracker.py add "Phase 2 – Metric Computation Modules" "Calibration Metrics" --status todo
  ```

Run the list command before standups or reviews, update statuses after each meaningful PR merge, and commit the modified `docs/plan.yml` to record progress.
