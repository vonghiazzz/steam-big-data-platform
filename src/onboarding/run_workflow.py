"""Run resumable dynamic Steam game onboarding as one guarded workflow."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from src.common.jsonl import read_jsonl, write_jsonl
from src.discovery.policy import load_discovery_policy
from src.discovery.registry import load_registry, save_registry
from src.onboarding.activation import GameReadiness, build_activated_registry
from src.onboarding.hdfs_preflight import inspect_hdfs_collisions
from src.onboarding.manifest import build_manifest, load_manifest
from src.onboarding.publisher import publish_hdfs_batch
from src.onboarding.staging import batch_root, prepare_batch_layout
from src.onboarding.validation import save_validation_report, validate_staged_batch
from src.onboarding.run_onboarding import (
    DEFAULT_BASELINE_REVIEWS,
    DEFAULT_CANONICAL_GAMES,
    DEFAULT_METADATA_PROBE,
    DEFAULT_POLICY,
    DEFAULT_QUEUE,
    DEFAULT_REGISTRY,
    DEFAULT_STAGING_ROOT,
    _local_preflight,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PHASES = (
    "runtime",
    "prepare",
    "crawl",
    "validate",
    "publish_bronze",
    "verify_bronze",
    "silver",
    "gold",
    "analytics",
    "mongodb",
    "readiness",
    "activate",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_batch_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"onboarding-{stamp.lower()}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--queue", type=Path, default=DEFAULT_QUEUE)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--metadata-probe", type=Path, default=DEFAULT_METADATA_PROBE)
    parser.add_argument("--baseline-reviews", type=Path, default=DEFAULT_BASELINE_REVIEWS)
    parser.add_argument("--canonical-games", type=Path, default=DEFAULT_CANONICAL_GAMES)
    parser.add_argument("--staging-root", type=Path, default=DEFAULT_STAGING_ROOT)
    parser.add_argument(
        "--namenode-container",
        default=os.getenv("HADOOP_NAMENODE_CONTAINER", "bda501-namenode"),
    )
    parser.add_argument("--spark-submit", default=os.getenv("SPARK_SUBMIT"))
    parser.add_argument("--run-discovery", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--stop-after", choices=PHASES)
    return parser


class Workflow:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.batch_id = args.batch_id or _default_batch_id()
        self.root = batch_root(args.staging_root, self.batch_id)
        self.state_path = self.root / "workflow_state.json"
        self.state = self._load_state()
        self.spark_submit = args.spark_submit or shutil.which("spark-submit")

    def run(self) -> int:
        self.args.staging_root.mkdir(parents=True, exist_ok=True)
        lock_path = self.args.staging_root / ".workflow.lock"
        with lock_path.open("a+", encoding="utf-8") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("Another onboarding workflow is already running") from exc

            if self.args.run_discovery and not (self.root / "manifest.json").exists():
                self._run_command(
                    [sys.executable, "-m", "src.discovery.run_discovery"],
                    "discovery",
                )

            actions: dict[str, Callable[[], object]] = {
                "runtime": self._runtime,
                "prepare": self._prepare,
                "crawl": self._crawl,
                "validate": self._validate,
                "publish_bronze": self._publish,
                "verify_bronze": self._verify_bronze,
                "silver": lambda: self._spark("src/silver/silver_pipeline.py"),
                "gold": lambda: self._spark("src/gold/run_gold.py"),
                "analytics": lambda: self._spark(
                    "src/analytics/run_gold_analytics.py"
                ),
                "mongodb": self._mongodb,
                "readiness": self._readiness,
                "activate": self._activate,
            }
            self.state["status"] = "RUNNING"
            self._save_state()
            try:
                for phase in PHASES:
                    self._step(phase, actions[phase])
                    if self.args.stop_after == phase:
                        self.state["status"] = "PAUSED"
                        self._save_state()
                        return 0
            except Exception as exc:
                self.state["status"] = "FAILED"
                self.state["error"] = str(exc)
                self._save_state()
                raise

            self.state["status"] = "COMPLETED"
            self.state["error"] = None
            self.state["completed_at"] = _utc_now()
            self._save_state()
            print(json.dumps(self.state, indent=2, ensure_ascii=False))
            return 0

    def _runtime(self) -> dict:
        containers = (
            self.args.namenode_container,
            "bda501-datanode",
            "bda501-resourcemanager",
            "bda501-nodemanager",
        )
        started: list[str] = []
        for container in containers:
            inspect = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Running}}", container],
                capture_output=True,
                text=True,
                check=False,
            )
            if inspect.returncode != 0:
                raise RuntimeError(
                    f"Required existing container was not found: {container}"
                )
            if inspect.stdout.strip() == "true":
                continue
            start = subprocess.run(
                ["docker", "start", container],
                capture_output=True,
                text=True,
                check=False,
            )
            if start.returncode != 0:
                raise RuntimeError(
                    f"Could not start {container}: {start.stderr.strip()}"
                )
            started.append(container)

        for _ in range(36):
            safe_mode = subprocess.run(
                [
                    "docker",
                    "exec",
                    self.args.namenode_container,
                    "hdfs",
                    "dfsadmin",
                    "-safemode",
                    "get",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            if safe_mode.returncode == 0 and "OFF" in safe_mode.stdout:
                return {"started_containers": started, "hdfs_safe_mode": "OFF"}
            time.sleep(5)
        raise RuntimeError("HDFS did not leave safe mode within 180 seconds")

    def _step(self, name: str, action: Callable[[], object]) -> None:
        existing = self.state["steps"].get(name, {})
        if existing.get("status") == "COMPLETED":
            print(f"[SKIP] {name}: already completed")
            return
        self.state["current_phase"] = name
        self.state["steps"][name] = {
            "status": "RUNNING",
            "started_at": _utc_now(),
        }
        self._save_state()
        print(f"[RUN] {name}")
        try:
            result = action()
        except Exception as exc:
            self.state["steps"][name].update(
                {"status": "FAILED", "finished_at": _utc_now(), "error": str(exc)}
            )
            self._save_state()
            raise
        self.state["steps"][name].update(
            {"status": "COMPLETED", "finished_at": _utc_now()}
        )
        if isinstance(result, dict):
            self.state["steps"][name]["result"] = result
        self._save_state()

    def _prepare(self) -> dict:
        manifest_path = self.root / "manifest.json"
        if manifest_path.exists():
            manifest = load_manifest(manifest_path)
            if manifest.batch_id != self.batch_id:
                raise RuntimeError("Existing manifest batch_id mismatch")
            return manifest.to_dict()

        manifest = build_manifest(
            self.batch_id,
            read_jsonl(self.args.queue),
            load_registry(self.args.registry),
            load_discovery_policy(self.args.policy),
        )
        local = _local_preflight(
            manifest.appids,
            self.args.metadata_probe,
            self.args.baseline_reviews,
            self.args.canonical_games,
        )
        hdfs = inspect_hdfs_collisions(
            manifest,
            namenode_container=self.args.namenode_container,
        )
        if not local["passed"]:
            raise RuntimeError("Local preflight failed: " + "; ".join(local["errors"]))
        if not hdfs.passed:
            raise RuntimeError(
                "HDFS preflight failed: "
                + json.dumps(hdfs.to_dict(), ensure_ascii=False)
            )
        prepare_batch_layout(self.args.staging_root, manifest, self.args.metadata_probe)
        return manifest.to_dict()

    def _crawl(self) -> dict:
        manifest = self._manifest()
        current = validate_staged_batch(
            manifest,
            self.root,
            baseline_reviews_root=self.args.baseline_reviews,
        )
        if current.passed:
            return {"already_complete": True, "reviews": current.total_reviews}
        self._run_command(
            [
                sys.executable,
                "-m",
                "src.ingestion.review_crawler",
                "--games-path",
                str(self.args.queue),
                "--target-reviews",
                str(manifest.target_reviews_per_game),
                "--output-root",
                str(self.root),
            ],
            "review crawler",
        )
        report = validate_staged_batch(
            manifest,
            self.root,
            baseline_reviews_root=self.args.baseline_reviews,
        )
        if not report.passed:
            raise RuntimeError("Crawl finished without a valid batch: " + "; ".join(report.errors))
        return {"reviews": report.total_reviews}

    def _validate(self) -> dict:
        report = validate_staged_batch(
            self._manifest(),
            self.root,
            baseline_reviews_root=self.args.baseline_reviews,
        )
        save_validation_report(self.root / "validation_report.json", report)
        if not report.passed:
            raise RuntimeError("Batch validation failed: " + "; ".join(report.errors))
        return report.to_dict()

    def _publish(self) -> dict:
        report = publish_hdfs_batch(
            self._manifest(),
            self.root,
            namenode_container=self.args.namenode_container,
        )
        return report.to_dict()

    def _verify_bronze(self) -> None:
        active_games = sum(
            entry.status == "ACTIVE"
            for entry in load_registry(self.args.registry).values()
        )
        expected_games = active_games + self._manifest().expected_game_count
        self._run_command(
            [
                sys.executable,
                "src/hdfs/verify_bronze_integrity.py",
                "--source",
                "hdfs",
                "--container",
                self.args.namenode_container,
                "--expect-games",
                str(expected_games),
                "--expect-per-game",
                str(self._manifest().target_reviews_per_game),
                "--out",
                str(self.root / "bronze_verification.txt"),
            ],
            "Bronze integrity verification",
        )

    def _spark(self, script: str, *extra: str) -> None:
        if not self.spark_submit:
            raise RuntimeError("spark-submit is unavailable")
        # The desktop container IP can change after OrbStack/Docker restarts.
        # Resolve it at execution time instead of trusting a stale .env hostname.
        default_fs = self._namenode_uri()
        environment = self._environment()
        environment["HADOOP_USER_NAME"] = "hadoop"
        self._run_command(
            [
                self.spark_submit,
                "--master",
                "local[2]",
                "--conf",
                "spark.ui.enabled=false",
                "--conf",
                f"spark.hadoop.fs.defaultFS={default_fs}",
                "--conf",
                "spark.hadoop.dfs.client.use.datanode.hostname=false",
                script,
                *extra,
            ],
            f"Spark job {script}",
            environment=environment,
        )

    def _mongodb(self) -> None:
        environment = self._environment()
        environment["HADOOP_NAMENODE_CONTAINER"] = self.args.namenode_container
        environment["HDFS_DEFAULT_FS"] = self._namenode_uri()
        environment["HADOOP_USER_NAME"] = "hadoop"
        if self.spark_submit:
            environment["SPARK_SUBMIT"] = self.spark_submit
        environment["PYTHON_BIN"] = sys.executable
        self._run_command(
            ["bash", "scripts/run_mongodb_serving.sh"],
            "MongoDB serving",
            environment=environment,
        )

    def _readiness(self) -> dict:
        output = self.root / "readiness_report.json"
        self._spark(
            "src/onboarding/collect_readiness.py",
            "--manifest",
            str(self.root / "manifest.json"),
            "--out",
            str(output),
            "--mongo-uri",
            os.getenv("MONGO_URI", "mongodb://localhost:27017"),
            "--mongo-database",
            os.getenv("MONGO_DATABASE", "steam_analytics"),
        )
        payload = json.loads(output.read_text(encoding="utf-8"))
        if not payload.get("passed"):
            raise RuntimeError("Readiness report did not pass")
        return payload

    def _activate(self) -> dict:
        manifest = self._manifest()
        validation = validate_staged_batch(
            manifest,
            self.root,
            baseline_reviews_root=self.args.baseline_reviews,
        )
        payload = json.loads(
            (self.root / "readiness_report.json").read_text(encoding="utf-8")
        )
        readiness = {
            int(appid): GameReadiness(
                appid=int(row["appid"]),
                bronze_metadata=bool(row["bronze_metadata"]),
                bronze_reviews=int(row["bronze_reviews"]),
                silver_game=bool(row["silver_game"]),
                silver_reviews=int(row["silver_reviews"]),
                gold_reviews=int(row["gold_reviews"]),
                mongo_game_metrics=bool(row["mongo_game_metrics"]),
            )
            for appid, row in payload["games"].items()
        }
        registry = load_registry(self.args.registry)
        activated = build_activated_registry(
            registry,
            manifest,
            validation,
            readiness,
            activated_at=_utc_now(),
        )
        save_registry(self.args.registry, activated)
        activated_ids = set(manifest.appids)
        remaining = [
            row
            for row in read_jsonl(self.args.queue)
            if int(row["appid"]) not in activated_ids
        ]
        write_jsonl(self.args.queue, remaining)
        return {"activated_appids": list(manifest.appids)}

    def _manifest(self):
        return load_manifest(self.root / "manifest.json")

    def _namenode_uri(self) -> str:
        result = subprocess.run(
            [
                "docker",
                "inspect",
                "-f",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                self.args.namenode_container,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        address = result.stdout.strip()
        if result.returncode != 0 or not address:
            raise RuntimeError("Could not resolve the NameNode container IP")
        return f"hdfs://{address}:8020"

    def _run_command(
        self,
        command: list[str],
        label: str,
        *,
        environment: dict[str, str] | None = None,
    ) -> None:
        result = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            env=environment or self._environment(),
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"{label} failed with exit code {result.returncode}")

    def _environment(self) -> dict[str, str]:
        environment = dict(os.environ)
        paths = [str(PROJECT_ROOT), str(PROJECT_ROOT / "src")]
        if environment.get("PYTHONPATH"):
            paths.append(environment["PYTHONPATH"])
        environment["PYTHONPATH"] = os.pathsep.join(paths)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        environment["PYSPARK_PYTHON"] = sys.executable
        environment["PYSPARK_DRIVER_PYTHON"] = sys.executable
        return environment

    def _load_state(self) -> dict:
        if self.state_path.exists():
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if payload.get("batch_id") != self.batch_id:
                raise RuntimeError("Workflow state batch_id mismatch")
            return payload
        return {
            "schema_version": 1,
            "batch_id": self.batch_id,
            "status": "PENDING",
            "current_phase": None,
            "created_at": _utc_now(),
            "updated_at": _utc_now(),
            "completed_at": None,
            "error": None,
            "steps": {},
        }

    def _save_state(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.state["updated_at"] = _utc_now()
        temporary = self.state_path.parent / f"{self.state_path.name}.tmp"
        temporary.write_text(
            json.dumps(self.state, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        temporary.replace(self.state_path)


def main() -> int:
    args = build_parser().parse_args()
    workflow = Workflow(args)
    if args.status:
        print(json.dumps(workflow.state, indent=2, ensure_ascii=False))
        return 0
    try:
        return workflow.run()
    except Exception as exc:
        print(f"ONBOARDING WORKFLOW FAILED: {exc}", file=sys.stderr)
        print(f"Resume with --batch-id {workflow.batch_id}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
