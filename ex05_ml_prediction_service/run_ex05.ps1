# ============================================
# EX05 – Training & Error Analysis (Docker)
# ============================================
# Purpose: Train model and generate artifacts
# Output: Model, metrics, error analysis reports
# Question: "Je produis un modèle et ses artefacts"
# ============================================

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  EX05 - TRAINING & ERROR ANALYSIS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "[INFO] Starting EX05 training pipeline..." -ForegroundColor Yellow
Write-Host "[INFO] This script will:" -ForegroundColor Gray
Write-Host "       - Train GBTRegressor model" -ForegroundColor Gray
Write-Host "       - Evaluate on test set (RMSE, MAE, R²)" -ForegroundColor Gray
Write-Host "       - Run error analysis" -ForegroundColor Gray
Write-Host "       - Save model and reports" -ForegroundColor Gray
Write-Host ""

# Start Spark cluster
docker compose up -d spark-master spark-worker-1 spark-worker-2 | Out-Null

# =========================
# TRAINING + ERROR ANALYSIS
# =========================
Write-Host "[STEP 1/1] Running training job on Spark cluster..." -ForegroundColor Yellow

docker exec spark-master bash -c @"
export SPARK_CONF_DIR=/opt/workdir/ex05_ml_prediction_service/conf && \
cd /opt/workdir/ex05_ml_prediction_service && \
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/opt/spark-events \
  --conf spark.ui.showConsoleProgress=true \
  src/train.py
"@

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "[SUCCESS] Training completed successfully" -ForegroundColor Green
    Write-Host "[INFO] Artifacts generated:" -ForegroundColor Gray
    Write-Host "       - models/ex05_spark_model/" -ForegroundColor Gray
    Write-Host "       - reports/train_metrics.json" -ForegroundColor Gray
    Write-Host "       - reports/error_summary.json" -ForegroundColor Gray
    Write-Host "       - reports/error_by_price_bucket.json" -ForegroundColor Gray
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "[FAILED] Training failed" -ForegroundColor Red
    exit 1
}
