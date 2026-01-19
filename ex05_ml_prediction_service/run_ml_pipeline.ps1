<#
.SYNOPSIS
    Run ML Pipeline with sliding window strategy.

.DESCRIPTION
    Executes the ML pipeline for training, evaluation, and model promotion.
    Supports automatic sliding window computation from test month.

.PARAMETER TestMonth
    The test month in YYYY-MM format (e.g., 2023-06)

.PARAMETER TrainMonths
    Optional: Comma-separated training months (e.g., 2023-03,2023-04,2023-05)
    If not provided, computed automatically from TestMonth

.PARAMETER RegistryPath
    Path to model registry (default: models/registry)

.PARAMETER DryRun
    Validate inputs without executing training

.EXAMPLE
    .\run_ml_pipeline.ps1 -TestMonth "2023-06"

.EXAMPLE
    .\run_ml_pipeline.ps1 -TestMonth "2023-06" -TrainMonths "2023-03,2023-04,2023-05"

.EXAMPLE
    .\run_ml_pipeline.ps1 -TestMonth "2023-06" -DryRun
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$TestMonth,
    
    [Parameter(Mandatory=$false)]
    [string]$TrainMonths,
    
    [Parameter(Mandatory=$false)]
    [string]$RegistryPath = "models/registry",
    
    [Parameter(Mandatory=$false)]
    [string]$ReportsDir = "reports",
    
    [Parameter(Mandatory=$false)]
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  NYC Taxi ML Pipeline" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Build command
$pythonCmd = "python src/ml_pipeline.py"
$pythonCmd += " --test-month `"$TestMonth`""

if ($TrainMonths) {
    $pythonCmd += " --train-months `"$TrainMonths`""
}

$pythonCmd += " --model-registry-path `"$RegistryPath`""
$pythonCmd += " --reports-dir `"$ReportsDir`""

if ($DryRun) {
    $pythonCmd += " --dry-run"
}

Write-Host "Executing: $pythonCmd" -ForegroundColor Yellow
Write-Host ""

# Execute
try {
    Invoke-Expression $pythonCmd
    $exitCode = $LASTEXITCODE
    
    if ($exitCode -eq 0) {
        Write-Host ""
        Write-Host "========================================" -ForegroundColor Green
        Write-Host "  Pipeline completed successfully!" -ForegroundColor Green
        Write-Host "========================================" -ForegroundColor Green
    } else {
        Write-Host ""
        Write-Host "========================================" -ForegroundColor Red
        Write-Host "  Pipeline failed with exit code: $exitCode" -ForegroundColor Red
        Write-Host "========================================" -ForegroundColor Red
        exit $exitCode
    }
}
catch {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Red
    Write-Host "  Pipeline error: $_" -ForegroundColor Red
    Write-Host "========================================" -ForegroundColor Red
    exit 1
}
