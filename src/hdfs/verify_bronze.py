import subprocess

from steam.common.config import (
    HADOOP_NAMENODE_CONTAINER,
    HDFS_BRONZE_GAMES,
    HDFS_BRONZE_GAMES_RAW,
    HDFS_BRONZE_REVIEW_PAGES,
    HDFS_BRONZE_REVIEWS,
    TARGET_GAME_COUNT,
    TARGET_REVIEWS_PER_GAME,
)


EXPECTED_TOTAL_REVIEWS = (
    TARGET_GAME_COUNT
    * TARGET_REVIEWS_PER_GAME
)


def run_in_namenode(
    command: str,
) -> str:
    result = subprocess.run(
        [
            "docker",
            "exec",
            HADOOP_NAMENODE_CONTAINER,
            "sh",
            "-lc",
            command,
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    return result.stdout.strip()


def count_lines(
    hdfs_path: str,
) -> int:
    output = run_in_namenode(
        f'hdfs dfs -cat "{hdfs_path}" '
        "| wc -l"
    )

    return int(output)


def count_files(
    hdfs_path: str,
) -> int:
    output = run_in_namenode(
        f'hdfs dfs -ls "{hdfs_path}" '
        '| grep "^-" | wc -l'
    )

    return int(output)


def count_recursive_files(
    hdfs_path: str,
) -> int:
    output = run_in_namenode(
        f'hdfs dfs -ls -R "{hdfs_path}" '
        '| grep "^-" | wc -l'
    )

    return int(output)


def get_size(
    hdfs_path: str,
) -> str:
    return run_in_namenode(
        f'hdfs dfs -du -h -s "{hdfs_path}"'
    )


def main():
    print("=" * 80)
    print("STEAM HDFS BRONZE VERIFICATION")
    print("=" * 80)

    game_records = count_lines(
        HDFS_BRONZE_GAMES_RAW
    )

    review_files = count_files(
        HDFS_BRONZE_REVIEWS
    )

    review_records = count_lines(
        f"{HDFS_BRONZE_REVIEWS}/*.jsonl"
    )

    raw_review_pages = (
        count_recursive_files(
            HDFS_BRONZE_REVIEW_PAGES
        )
    )

    errors = []

    if (
        game_records
        != TARGET_GAME_COUNT
    ):
        errors.append(
            f"Expected "
            f"{TARGET_GAME_COUNT} "
            f"game records, found "
            f"{game_records}."
        )

    if (
        review_files
        != TARGET_GAME_COUNT
    ):
        errors.append(
            f"Expected "
            f"{TARGET_GAME_COUNT} "
            f"review files, found "
            f"{review_files}."
        )

    if (
        review_records
        != EXPECTED_TOTAL_REVIEWS
    ):
        errors.append(
            f"Expected "
            f"{EXPECTED_TOTAL_REVIEWS} "
            f"review records, found "
            f"{review_records}."
        )

    result = (
        "PASS"
        if not errors
        else "FAIL"
    )

    print(
        "Game records       :",
        game_records,
    )

    print(
        "Review files       :",
        review_files,
    )

    print(
        "Review records     :",
        review_records,
    )

    print(
        "Raw review pages   :",
        raw_review_pages,
    )

    print()
    print("=== HDFS SIZE ===")

    print(
        get_size(
            HDFS_BRONZE_GAMES
        )
    )

    print(
        get_size(
            HDFS_BRONZE_REVIEWS
        )
    )

    print(
        get_size(
            HDFS_BRONZE_REVIEW_PAGES
        )
    )

    print()
    print("=" * 80)

    print(
        "HDFS BRONZE RESULT :",
        result,
    )

    if errors:
        print()
        print("ERRORS:")

        for error in errors:
            print(
                "-",
                error,
            )


if __name__ == "__main__":
    main()
