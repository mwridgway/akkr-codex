# Repository Guidelines

## Project Structure & Module Organization
The repository follows a modular monolith rooted at `src/cs2_analytics/`, with domain-focused subpackages such as `agents/`, `data_processing/`, and `visualization/`. Keep raw match files in `data/raw/`, write cleaned artifacts to `data/processed/`, and store exploratory work in `notebooks/`. Tests live in `tests/` and should mirror source paths (for example, `tests/data_processing/test_analysis.py` should cover `src/cs2_analytics/data_processing/analysis.py`).

## Build, Test, and Development Commands
- `poetry install`: provision the virtual environment and sync locked dependencies.
- `poetry run streamlit run src/cs2_analytics/app.py`: launch the dashboard locally at `http://localhost:8501`.
- `poetry run pytest`: execute the full suite; combine with `-k` or `--maxfail=1` for focused runs.
- `./scripts/new_module.sh defense`: scaffold a new module skeleton (adds package, test, and prompt stub).
- `gemini run prompts/<task>.gemini`: replay curated Gemini workflows for repeatable automation tasks.

## Coding Style & Naming Conventions
Use 4-space indentation, exhaustive PEP 484 type hints, and snake_case for functions and modules; reserve PascalCase for classes and AutoGen agents. Keep `data_processing/` functions pure and let Streamlit-facing code delegate heavy lifting to service objects. Format with `poetry run black .` (line length 88) and lint via `poetry run ruff check .`.

## Testing Guidelines
Pytest is the baseline; co-locate shared fixtures in `tests/conftest.py` and prefer property-based checks for metric calculations. Name tests after the behavior under scrutiny (`test_rotation_timing_handles_overtime`). Maintain >=85% statement coverage on modules you touch by running `poetry run pytest --cov=src/cs2_analytics`. When validating Polars transforms, add regression datasets under `data/fixtures/` and assert on schema, dtypes, and sentinel values.

## Commit & Pull Request Guidelines
Write imperative, concise commit subjects (`Add rotation efficiency metrics`) and group changes by logical feature. Pull requests must summarize the problem, list key changes, include UI screenshots or metric diffs when applicable, and link to tracking issues. Verify CI-critical commands (`pytest`, lint, Streamlit smoke run) before requesting review and call out any Gemini prompt updates needing retraining.

## AI & Automation Practices
Treat the Gemini CLI as a first-class collaborator: version reusable prompts under `prompts/` (`data-cleanup.gemini`, `ui-wireframe.gemini`) and document invocation notes in README snippets. Keep scripts in `scripts/` idempotent with clear logging so human and agent operators can monitor progress.
