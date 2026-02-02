#!/usr/bin/env bash
set -euo pipefail

MODE="${SPARK_MODE:-master}"

mkdir -p /opt/spark-events

if [ "$MODE" = "master" ]; then
  echo "[Spark] Starting MASTER..."
  exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.master.Master \
    --host 0.0.0.0 \
    --port 7077 \
    --webui-port 8080
fi

if [ "$MODE" = "worker" ]; then
  : "${SPARK_MASTER_URL:?SPARK_MASTER_URL is required for worker mode}"
  CORES="${SPARK_WORKER_CORES:-2}"
  MEM="${SPARK_WORKER_MEMORY:-2g}"
  WEBUI_PORT="${SPARK_WORKER_WEBUI_PORT:-8081}"

  echo "[Spark] Starting WORKER..."
  exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
    --webui-port "${WEBUI_PORT}" \
    --cores "${CORES}" \
    --memory "${MEM}" \
    "${SPARK_MASTER_URL}"
fi

echo "Unknown SPARK_MODE: $MODE (use master|worker)"
exit 1
