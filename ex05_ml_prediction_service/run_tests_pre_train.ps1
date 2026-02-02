# ============================================
# EX05 – Pre-training tests (Docker)
# ============================================
# Purpose: Validate data and schema BEFORE training
# Tests: Schema validation, data quality, temporal ranges
# Question: "Est-ce que j'ai le droit d'entraîner un modèle ?"
# ============================================

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  EX05 - PRE-TRAINING TESTS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "[INFO] Running pre-training validation tests..." -ForegroundColor Yellow
Write-Host "[INFO] These tests verify data is ready for training:" -ForegroundColor Gray
Write-Host "       - Schema validation (required columns)" -ForegroundColor Gray
Write-Host "       - Data quality (non-negative values)" -ForegroundColor Gray
Write-Host "       - Temporal ranges (month iteration)" -ForegroundColor Gray
Write-Host ""

# Ensure Spark cluster is running
docker compose up -d spark-master spark-worker-1 spark-worker-2 | Out-Null

# Run pre-training tests
$testResult = docker exec spark-master bash -c @"
cd /opt/workdir/ex05_ml_prediction_service && \
export PYTHONPATH=/opt/workdir/ex05_ml_prediction_service && \
pytest tests/test_validation.py tests/test_month_range.py tests/test_ml_schema.py -v --tb=short
"@

Write-Host $testResult

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "[SUCCESS] Pre-training tests PASSED" -ForegroundColor Green
    Write-Host "[INFO] Data is valid - you may proceed with training" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "[FAILED] Pre-training tests FAILED" -ForegroundColor Red
    Write-Host "[ERROR] Fix data issues before training" -ForegroundColor Red
    Write-Host ""
    exit 1
}
