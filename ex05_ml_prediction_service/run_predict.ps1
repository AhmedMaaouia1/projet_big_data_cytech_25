# ============================================
# EX05 – Prediction / Inference (Docker)
# ============================================
# Purpose: Run inference with trained model
# Input: Data from MinIO (nyc-interim)
# Output: Predictions report
# Question: "Je fais de la prédiction en conditions contrôlées"
# ============================================

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  EX05 - PREDICTION / INFERENCE" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "[INFO] Starting inference pipeline..." -ForegroundColor Yellow
Write-Host "[INFO] This script will:" -ForegroundColor Gray
Write-Host "       - Load trained model" -ForegroundColor Gray
Write-Host "       - Validate input schema" -ForegroundColor Gray
Write-Host "       - Run Spark inference" -ForegroundColor Gray
Write-Host "       - Generate predictions report" -ForegroundColor Gray
Write-Host ""

# Check if model exists
$modelPath = "models/ex05_spark_model"
if (-not (Test-Path $modelPath)) {
    Write-Host "[ERROR] Trained model not found at $modelPath" -ForegroundColor Red
    Write-Host "[INFO] Run training first: .\run_ex05.ps1" -ForegroundColor Yellow
    exit 1
}

# Ensure Spark cluster is running
docker compose up -d spark-master spark-worker-1 spark-worker-2 | Out-Null

# =========================
# INFERENCE
# =========================
Write-Host "[STEP 1/1] Running inference job on Spark cluster..." -ForegroundColor Yellow

docker exec spark-master bash -c @"
export SPARK_CONF_DIR=/opt/workdir/ex05_ml_prediction_service/conf && \
cd /opt/workdir/ex05_ml_prediction_service && \
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/opt/spark-events \
  --conf spark.ui.showConsoleProgress=true \
  src/predict.py
"@

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "[SUCCESS] Inference completed successfully" -ForegroundColor Green
    Write-Host "[INFO] Artifacts generated:" -ForegroundColor Gray
    Write-Host "       - reports/predict_report.json" -ForegroundColor Gray
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "[FAILED] Inference failed" -ForegroundColor Red
    exit 1
}
