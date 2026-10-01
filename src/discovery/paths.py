"""Repository-relative paths used by the discovery control plane."""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_ROOT = PROJECT_ROOT / "data" / "raw"
CATALOG_ROOT = RAW_ROOT / "catalog_probe"

CANDIDATES_PATH = CATALOG_ROOT / "candidates.jsonl"
METADATA_PROBE_PATH = CATALOG_ROOT / "metadata_probe_raw.jsonl"
ELIGIBLE_GAMES_PATH = CATALOG_ROOT / "eligible_games.jsonl"
REVIEW_PROBE_PATH = CATALOG_ROOT / "review_probe.jsonl"

INITIAL_SNAPSHOT_PATH = RAW_ROOT / "selected_50_games.jsonl"
REGISTRY_ROOT = RAW_ROOT / "registry"
REGISTRY_PATH = REGISTRY_ROOT / "game_registry.jsonl"
ONBOARDING_PLAN_PATH = REGISTRY_ROOT / "onboarding_plan.json"
ONBOARDING_QUEUE_PATH = REGISTRY_ROOT / "onboarding_queue.jsonl"
DISCOVERY_REPORT_PATH = REGISTRY_ROOT / "discovery_run_report.json"
