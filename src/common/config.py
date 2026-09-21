from pathlib import Path


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[3]
)


DATA_ROOT = (
    PROJECT_ROOT
    / "data"
)

STEAM_RAW_ROOT = (
    DATA_ROOT
    / "raw"
    / "steam"
)


CATALOG_PROBE_ROOT = (
    STEAM_RAW_ROOT
    / "catalog_probe"
)

CANDIDATES_PATH = (
    CATALOG_PROBE_ROOT
    / "candidates.jsonl"
)

METADATA_PROBE_RAW_PATH = (
    CATALOG_PROBE_ROOT
    / "metadata_probe_raw.jsonl"
)

ELIGIBLE_GAMES_PATH = (
    CATALOG_PROBE_ROOT
    / "eligible_games.jsonl"
)

REVIEW_PROBE_PATH = (
    CATALOG_PROBE_ROOT
    / "review_probe.jsonl"
)


SELECTED_GAMES_PATH = (
    STEAM_RAW_ROOT
    / "selected_50_games.jsonl"
)


LANDING_ROOT = (
    STEAM_RAW_ROOT
    / "landing"
)

LANDING_GAMES_ROOT = (
    LANDING_ROOT
    / "games"
)

LANDING_GAMES_RAW_PATH = (
    LANDING_GAMES_ROOT
    / "games_raw.jsonl"
)

LANDING_REVIEWS_ROOT = (
    LANDING_ROOT
    / "reviews_by_game"
)

LANDING_REVIEW_PAGES_ROOT = (
    LANDING_ROOT
    / "review_pages"
)

LANDING_STATE_PATH = (
    LANDING_ROOT
    / "crawl_state.json"
)

LANDING_VALIDATION_ROOT = (
    LANDING_ROOT
    / "validation"
)

LANDING_VALIDATION_REPORT_PATH = (
    LANDING_VALIDATION_ROOT
    / "landing_validation_report.json"
)


BRONZE_READY_ROOT = (
    STEAM_RAW_ROOT
    / "bronze_ready"
)

BRONZE_READY_REVIEWS_ROOT = (
    BRONZE_READY_ROOT
    / "reviews_by_game"
)

BRONZE_READY_FINALIZATION_REPORT_PATH = (
    BRONZE_READY_ROOT
    / "finalization_report.json"
)

BRONZE_READY_VALIDATION_REPORT_PATH = (
    BRONZE_READY_ROOT
    / "validation_report.json"
)


ARCHIVE_ROOT = (
    PROJECT_ROOT
    / "archive"
    / "steam_prototypes"
)


TARGET_GAME_COUNT = 50

TARGET_REVIEWS_PER_GAME = 500


HDFS_STEAM_ROOT = (
    "/user/hadoop/steam"
)

HDFS_BRONZE_ROOT = (
    f"{HDFS_STEAM_ROOT}/bronze"
)

HDFS_BRONZE_GAMES = (
    f"{HDFS_BRONZE_ROOT}/games"
)

HDFS_BRONZE_REVIEWS = (
    f"{HDFS_BRONZE_ROOT}/reviews"
)

HDFS_BRONZE_REVIEW_PAGES = (
    f"{HDFS_BRONZE_ROOT}/review_pages"
)

HDFS_SILVER_ROOT = (
    f"{HDFS_STEAM_ROOT}/silver"
)

HDFS_GOLD_ROOT = (
    f"{HDFS_STEAM_ROOT}/gold"
)

HADOOP_NAMENODE_CONTAINER = (
    "bda501-namenode"
)

HDFS_BRONZE_GAMES_RAW = (
    f"{HDFS_BRONZE_GAMES}/"
    "games_raw.jsonl"
)