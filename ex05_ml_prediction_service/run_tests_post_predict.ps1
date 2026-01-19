# ============================================
# EX05 – Post-prediction plausibility tests (Docker)
# ============================================
# Purpose: Validate prediction results are business-plausible
# Tests: Non-negative, finite, reasonable bounds
# Question: "Les résultats sont acceptables pour un humain ?"
# ============================================

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  EX05 - POST-PREDICTION PLAUSIBILITY TESTS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "[INFO] Running post-prediction plausibility tests..." -ForegroundColor Yellow
Write-Host "[INFO] These tests verify predictions are business-valid:" -ForegroundColor Gray
Write-Host "       - Non-negative (fares >= 0)" -ForegroundColor Gray
Write-Host "       - Finite (no NaN/Inf)" -ForegroundColor Gray
Write-Host "       - Reasonable bounds (< max fare)" -ForegroundColor Gray
Write-Host "       - NYC taxi business constraints" -ForegroundColor Gray
Write-Host ""

# Ensure Spark cluster is running
docker compose up -d spark-master spark-worker-1 spark-worker-2 | Out-Null

# Run post-prediction plausibility tests
$testResult = docker exec spark-master bash -c @"
cd /opt/workdir/ex05_ml_prediction_service && \
export PYTHONPATH=/opt/workdir/ex05_ml_prediction_service && \
pytest tests/test_ml_plausibility.py -v --tb=short
"@

Write-Host $testResult

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "[SUCCESS] Post-prediction plausibility tests PASSED" -ForegroundColor Green
    Write-Host "[INFO] Predictions are business-valid" -ForegroundColor Green
    Write-Host "[INFO] Results are acceptable for production use" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "[FAILED] Post-prediction plausibility tests FAILED" -ForegroundColor Red
    Write-Host "[ERROR] Some predictions violate business constraints" -ForegroundColor Red
    Write-Host "[INFO] Review prediction results and model behavior" -ForegroundColor Yellow
    Write-Host ""
    exit 1
}
