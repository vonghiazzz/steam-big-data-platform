#!/usr/bin/env bash
set -euo pipefail

echo "[BDA501] Client container starting..."
echo "[BDA501] Python: $(python3 --version 2>&1)"
echo "[BDA501] Hadoop: $(hadoop version | head -n 1)"

echo "[BDA501] Waiting for HDFS..."
for i in $(seq 1 90); do
  if hdfs dfs -ls / >/dev/null 2>&1; then
    break
  fi
  if [ "$i" -eq 90 ]; then
    echo "[BDA501] ERROR: HDFS did not become ready." >&2
    exit 1
  fi
  sleep 2
done

echo "[BDA501] Preparing HDFS directories for Session 2..."
hdfs dfs -mkdir -p /tmp /user/bda501/i1/input /mr-history/tmp /mr-history/done
hdfs dfs -chmod 1777 /tmp /mr-history/tmp /mr-history/done || true
hdfs dfs -put -f /workspace/i1/sample_access_log.csv /user/bda501/i1/input/sample_access_log.csv

echo "[BDA501] Waiting for YARN NodeManager registration..."
for i in $(seq 1 90); do
  if yarn node -list 2>/dev/null | grep -q "RUNNING"; then
    break
  fi
  if [ "$i" -eq 90 ]; then
    echo "[BDA501] WARNING: YARN is not fully ready yet. It may need another minute." >&2
    break
  fi
  sleep 2
done

cat <<'MSG'

============================================================
 BDA501 HADOOP LAB READY
============================================================
 HDFS NameNode UI : http://localhost:9870
 YARN RM UI       : http://localhost:8088
 DataNode UI      : http://localhost:9864
 NodeManager UI   : http://localhost:8042
 JobHistory UI    : http://localhost:19888

 Run health check:
   docker compose exec client bash /workspace/scripts/check_lab.sh

 Run the complete I1 worked example:
   docker compose exec client bash /workspace/scripts/run_i1_hadoop.sh
============================================================
MSG

# Keep the client available for interactive commands.
tail -f /dev/null
