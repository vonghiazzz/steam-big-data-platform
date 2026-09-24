#!/usr/bin/env python3
"""Verify the Steam Bronze handoff: file count, record count, integrity.

Standalone (no imports from the repo), run from the repository ROOT.

  # 1) baseline on the unzipped local files
  python src/hdfs/verify_bronze_integrity.py --source local

  # 2) after upload: verify what is really in HDFS, and compare every file
  #    byte-for-byte (md5) with the local copy
  python src/hdfs/verify_bronze_integrity.py --source hdfs --out evidence/hdfs/bronze_verification.txt

Exit code 0 = PASS, 1 = FAIL.
"""
import argparse
import hashlib
import json
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path


# ---------- I/O helpers: same interface for local disk and HDFS ----------
class Source:
    def list_reviews(self): raise NotImplementedError
    def games_path(self): raise NotImplementedError
    def open_lines(self, path): raise NotImplementedError   # yields raw bytes lines


class LocalSource(Source):
    def __init__(self, root):
        self.root = Path(root)

    def games_path(self):
        return str(self.root / "landing/games/games_raw.jsonl")

    def list_reviews(self):
        return sorted(str(p) for p in (self.root / "bronze_ready/reviews_by_game").glob("*.jsonl"))

    def open_lines(self, path):
        with open(path, "rb") as f:
            yield from f


class HdfsSource(Source):
    def __init__(self, container, games_dir, reviews_dir):
        self.c, self.games_dir, self.reviews_dir = container, games_dir, reviews_dir

    def _dx(self, *args):
        return ["docker", "exec", self.c, *args]

    def games_path(self):
        return f"{self.games_dir}/games_raw.jsonl"

    def list_reviews(self):
        out = subprocess.run(self._dx("hdfs", "dfs", "-ls", self.reviews_dir),
                             capture_output=True, text=True, check=True).stdout
        files = [ln.split()[-1] for ln in out.splitlines() if ln.startswith("-")]
        return sorted(files)

    def open_lines(self, path):
        p = subprocess.Popen(self._dx("hdfs", "dfs", "-cat", path),
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        yield from p.stdout
        err = p.stderr.read().decode(errors="replace")
        if p.wait() != 0:
            raise RuntimeError(f"hdfs -cat {path} failed: {err.strip()}")


def md5_of_local(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------- checks ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["local", "hdfs"], required=True)
    ap.add_argument("--local-root", default="data/raw/steam")
    ap.add_argument("--container", default="bda501-namenode")
    ap.add_argument("--hdfs-games", default="/steam/bronze/games")
    ap.add_argument("--hdfs-reviews", default="/steam/bronze/reviews")
    ap.add_argument("--expect-games", type=int, default=50)
    ap.add_argument("--expect-per-game", type=int, default=500)
    ap.add_argument("--out", help="also write the report to this file")
    a = ap.parse_args()

    src = (LocalSource(a.local_root) if a.source == "local"
           else HdfsSource(a.container, a.hdfs_games, a.hdfs_reviews))
    local = LocalSource(a.local_root)              # reference copy for md5 comparison
    lines, errors = [], []

    def say(msg=""):
        lines.append(msg); print(msg)

    def check(ok, msg):
        if not ok:
            errors.append(msg)
        say(f"  [{'OK' if ok else 'FAIL'}] {msg}")

    say(f"STEAM BRONZE VERIFICATION  source={a.source}")

    # ---- games ----
    say("\n== games ==")
    gpath = src.games_path()
    games, bad = [], 0
    for raw in src.open_lines(gpath):
        try:
            games.append(json.loads(raw))
        except ValueError:
            bad += 1
    game_ids = {str(g.get("appid")) for g in games}
    say(f"  file: {gpath}")
    check(len(games) == a.expect_games and bad == 0, f"game records = {len(games)} (expected {a.expect_games}), unparsable = {bad}")
    check(len(game_ids) == a.expect_games, f"unique game appid = {len(game_ids)}")
    check(all(g.get("success") is True for g in games), "all game records have success=true")

    # ---- reviews ----
    say("\n== reviews ==")
    files = src.list_reviews()
    check(len(files) == a.expect_games, f"review files = {len(files)} (expected {a.expect_games})")
    file_ids = {os.path.basename(f)[:-len(".jsonl")] for f in files}
    check(file_ids == game_ids, f"review file names == game appids (diff: {sorted(file_ids ^ game_ids) or 'none'})")

    total, unparsable, appid_mismatch, missing_id, empty_text = 0, 0, 0, 0, 0
    seen_ids, dup_ids, labels = set(), 0, Counter()
    per_game, md5_bad = {}, []
    local_files = {os.path.basename(p): p for p in local.list_reviews()}
    for f in files:
        name = os.path.basename(f)
        appid = name[:-len(".jsonl")]
        h, n = hashlib.md5(), 0
        for raw in src.open_lines(f):
            h.update(raw)
            try:
                r = json.loads(raw)
            except ValueError:
                unparsable += 1
                continue
            n += 1
            if str(r.get("appid")) != appid:
                appid_mismatch += 1
            rv = r.get("review") or {}
            rid = rv.get("recommendationid")
            if not rid:
                missing_id += 1
            elif rid in seen_ids:
                dup_ids += 1
            else:
                seen_ids.add(rid)
            labels[rv.get("voted_up")] += 1
            if not str(rv.get("review") or "").strip():
                empty_text += 1
        per_game[appid] = n
        total += n
        if a.source == "hdfs" and name in local_files and md5_of_local(local_files[name]) != h.hexdigest():
            md5_bad.append(name)

    check(total == a.expect_games * a.expect_per_game, f"review records = {total} (expected {a.expect_games * a.expect_per_game})")
    wrong = {k: v for k, v in per_game.items() if v != a.expect_per_game}
    check(not wrong, f"every game has {a.expect_per_game} reviews (exceptions: {wrong or 'none'})")
    check(unparsable == 0, f"unparsable JSON lines = {unparsable}")
    check(appid_mismatch == 0, f"records whose appid != file name = {appid_mismatch}")
    check(missing_id == 0 and dup_ids == 0 and len(seen_ids) == total,
          f"unique review.recommendationid = {len(seen_ids)} (missing={missing_id}, duplicates={dup_ids})")
    say(f"  info: voted_up true={labels[True]} false={labels[False]} other={sum(v for k, v in labels.items() if k not in (True, False))}; empty review text={empty_text}")

    # cross-check with the handoff's own validation_report.json
    vr = Path(a.local_root) / "bronze_ready/validation_report.json"
    if vr.exists():
        rep = json.loads(vr.read_text(encoding="utf-8"))
        check(rep["label_counts"]["positive"] == labels[True] and rep["label_counts"]["negative"] == labels[False],
              "positive/negative counts match validation_report.json")
        check(rep["empty_review_text"] == empty_text, "empty-text count matches validation_report.json")
        check({k: int(v) for k, v in rep["per_game_counts"].items()} == {k: int(v) for k, v in per_game.items()},
              "per-game counts match validation_report.json")

    if a.source == "hdfs":
        say("\n== byte-level integrity (HDFS vs local copy) ==")
        # games file too
        h = hashlib.md5()
        for raw in src.open_lines(gpath):
            h.update(raw)
        lg = local.games_path()
        if os.path.exists(lg) and md5_of_local(lg) != h.hexdigest():
            md5_bad.append("games_raw.jsonl")
        check(not md5_bad and os.path.exists(lg), f"md5 identical for games + {len(files)} review files (mismatch: {md5_bad or 'none'})")

    say("\nRESULT: " + ("PASS" if not errors else "FAIL"))
    for e in errors:
        say(f"  - {e}")
    if a.out:
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        Path(a.out).write_text("\n".join(lines) + "\n", encoding="utf-8")
    sys.exit(0 if not errors else 1)


if __name__ == "__main__":
    main()
