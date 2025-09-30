"""CLI utilities for managing project execution plan progress."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError as exc:  # pragma: no cover - optional dependency
    raise ModuleNotFoundError(
        "PyYAML is required for plan_tracker. Install with `pip install pyyaml` or add to your environment."
    ) from exc

PLAN_PATH = Path(__file__).resolve().parents[1] / "docs" / "plan.yml"
VALID_STATUSES = {"todo", "in_progress", "done"}


def load_plan() -> dict[str, Any]:
    if not PLAN_PATH.exists():
        raise FileNotFoundError(f"Plan file not found: {PLAN_PATH}")
    return yaml.safe_load(PLAN_PATH.read_text(encoding="utf-8"))


def save_plan(plan: dict[str, Any]) -> None:
    PLAN_PATH.write_text(yaml.safe_dump(plan, sort_keys=False), encoding="utf-8")


def list_plan(args: argparse.Namespace) -> None:
    plan = load_plan()
    phases = plan.get("phases", [])

    if args.json:
        print(json.dumps(phases, indent=2))
        return

    for phase in phases:
        phase_name = phase.get("name")
        phase_status = phase.get("status")
        if args.phase and args.phase.lower() != str(phase_name).lower():
            continue

        print(f"{phase_name} [{phase_status}]")
        for task in phase.get("tasks", []):
            task_line = f"  - {task.get('name')} [{task.get('status')}]"
            if args.verbose and task.get("notes"):
                task_line += f" — {task.get('notes')}"
            print(task_line)
        print()


def set_status(args: argparse.Namespace) -> None:
    if args.status not in VALID_STATUSES:
        raise ValueError(f"Invalid status '{args.status}'. Expected one of {sorted(VALID_STATUSES)}")

    plan = load_plan()
    updated = False

    for phase in plan.get("phases", []):
        if phase.get("name").lower() != args.phase.lower():
            continue

        if args.task:
            for task in phase.get("tasks", []):
                if task.get("name").lower() == args.task.lower():
                    task["status"] = args.status
                    if args.notes is not None:
                        task["notes"] = args.notes
                    updated = True
                    break
        else:
            phase["status"] = args.status
            if args.notes is not None:
                phase["notes"] = args.notes
            updated = True
        break

    if not updated:
        target = f"phase '{args.phase}'"
        if args.task:
            target += f" task '{args.task}'"
        raise ValueError(f"Unable to locate {target} in plan")

    save_plan(plan)


def add_task(args: argparse.Namespace) -> None:
    plan = load_plan()

    for phase in plan.get("phases", []):
        if phase.get("name").lower() == args.phase.lower():
            phase.setdefault("tasks", []).append(
                {
                    "name": args.task,
                    "status": args.status,
                    "notes": args.notes or "",
                }
            )
            save_plan(plan)
            return

    raise ValueError(f"Phase '{args.phase}' not found in plan")


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage project plan progress")
    subparsers = parser.add_subparsers(required=True)

    list_parser = subparsers.add_parser("list", help="List phases and tasks")
    list_parser.add_argument("--phase", help="Filter by phase name")
    list_parser.add_argument("--verbose", action="store_true", help="Show task notes")
    list_parser.add_argument("--json", action="store_true", help="Output raw JSON data")
    list_parser.set_defaults(func=list_plan)

    set_parser = subparsers.add_parser("set", help="Update status for a phase or task")
    set_parser.add_argument("phase", help="Phase name to update")
    set_parser.add_argument("--task", help="Task name within phase")
    set_parser.add_argument("--status", required=True, choices=sorted(VALID_STATUSES))
    set_parser.add_argument("--notes", help="Optional notes to record")
    set_parser.set_defaults(func=set_status)

    add_parser = subparsers.add_parser("add", help="Add a new task to a phase")
    add_parser.add_argument("phase", help="Phase name to append to")
    add_parser.add_argument("task", help="Task description")
    add_parser.add_argument("--status", default="todo", choices=sorted(VALID_STATUSES))
    add_parser.add_argument("--notes", help="Optional notes for context")
    add_parser.set_defaults(func=add_task)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
