# ============================================
# EX05 – Post-training quality tests (Docker)
# ============================================
# Purpose: Validate model quality AFTER training
# Tests: RMSE < 10, R² > 0, MAE reasonable, MAE ≤ RMSE
# Question: "Ce modèle a-t-il le droit d'exister ?"
# ============================================

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  EX05 - POST-TRAINING QUALITY TESTS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "[INFO] Running post-training quality tests..." -ForegroundColor Yellow
Write-Host "[INFO] These tests verify model meets quality thresholds:" -ForegroundColor Gray
Write-Host "       - RMSE < 10 (project requirement)" -ForegroundColor Gray
Write-Host "       - R² > 0 (better than mean baseline)" -ForegroundColor Gray
Write-Host "       - MAE < 15 (reasonable error)" -ForegroundColor Gray
Write-Host "       - MAE ≤ RMSE (mathematical consistency)" -ForegroundColor Gray
Write-Host ""

# Check if training metrics exist
$metricsPath = "reports/train_metrics.json"
if (-not (Test-Path $metricsPath)) {
    Write-Host "[ERROR] Training metrics not found at $metricsPath" -ForegroundColor Red
    Write-Host "[INFO] Run training first: .\run_ex05.ps1" -ForegroundColor Yellow
    exit 1
}

# Ensure Spark cluster is running
docker compose up -d spark-master spark-worker-1 spark-worker-2 | Out-Null

# Run post-training quality tests
$testResult = docker exec spark-master bash -c @"
cd /opt/workdir/ex05_ml_prediction_service && \
export PYTHONPATH=/opt/workdir/ex05_ml_prediction_service && \
pytest tests/test_ml_quality.py -v --tb=short
"@

Write-Host $testResult

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "[SUCCESS] Post-training quality tests PASSED" -ForegroundColor Green
    Write-Host "[INFO] Model meets all quality thresholds" -ForegroundColor Green
    Write-Host "[INFO] Model is approved for inference" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "[FAILED] Post-training quality tests FAILED" -ForegroundColor Red
    Write-Host "[ERROR] Model does not meet quality requirements" -ForegroundColor Red
    Write-Host "[INFO] Review error analysis and retrain if needed" -ForegroundColor Yellow
    Write-Host ""
    exit 1
}
