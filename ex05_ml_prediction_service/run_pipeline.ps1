# ============================================
# EX05 – PIPELINE ORCHESTRATOR (Full ML Pipeline)
# ============================================
# Purpose: Execute the complete ML pipeline end-to-end
# 
# NEW: Supports sliding window strategy with model registry
#
# Modes:
#   Legacy:  Uses train.py with hardcoded months (2023/01, 2023/02)
#   Sliding: Uses ml_pipeline.py with auto-detected or explicit months
#
# Steps:
#   1. Pre-training tests (data validation)
#   2. Training & Error Analysis
#   3. Post-training quality tests
#   4. Inference / Prediction
#   5. Post-prediction plausibility tests
# ============================================

param(
    [switch]$SkipTests,
    [switch]$TrainOnly,
    [switch]$PredictOnly,
    [switch]$Force,
    [switch]$Help,
    [switch]$Legacy,
    [switch]$SkipMissing,
    [string]$TestMonth,
    [string]$TrainMonths,
    [string]$RegistryPath = "models/registry"
)

$ErrorActionPreference = "Stop"

# ============================================
# CONFIGURATION
# ============================================
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$MODEL_PATH = Join-Path $SCRIPT_DIR "models\ex05_spark_model"
$REGISTRY_PATH = Join-Path $SCRIPT_DIR $RegistryPath
$CURRENT_MODEL_PATH = Join-Path $REGISTRY_PATH "current"
$METRICS_PATH = Join-Path $SCRIPT_DIR "reports\train_metrics.json"
$PREDICT_REPORT_PATH = Join-Path $SCRIPT_DIR "reports\predict_report.json"

# Determine which model path to use
if ($Legacy) {
    $ACTIVE_MODEL_PATH = $MODEL_PATH
} else {
    $ACTIVE_MODEL_PATH = $CURRENT_MODEL_PATH
}

# Timing
$pipelineStartTime = Get-Date

# ============================================
# HELP
# ============================================
if ($Help) {
    Write-Host ""
    Write-Host "EX05 ML Pipeline Orchestrator" -ForegroundColor Cyan
    Write-Host "==============================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Usage: .\run_pipeline.ps1 [options]"
    Write-Host ""
    Write-Host "Options:"
    Write-Host "  -SkipTests     Skip all validation tests (not recommended)"
    Write-Host "  -TrainOnly     Run only training phase (steps 1-3)"
    Write-Host "  -PredictOnly   Run only prediction phase (steps 4-5)"
    Write-Host "  -Force         Force re-training even if model exists"
    Write-Host "  -Legacy        Use legacy train.py (hardcoded 2023/01-02)"
    Write-Host "  -TestMonth     Test month for sliding window (e.g., 2023-06)"
    Write-Host "                 If omitted, uses current month"
    Write-Host "  -TrainMonths   Training months (e.g., 2023-03,2023-04,2023-05)"
    Write-Host "                 If omitted, auto-computed from TestMonth"
    Write-Host "  -SkipMissing   Continue with available data if some months missing"
    Write-Host "  -RegistryPath  Model registry path (default: models/registry)"
    Write-Host "  -Help          Show this help message"
    Write-Host ""
    Write-Host "Examples:" -ForegroundColor Yellow
    Write-Host "  .\run_pipeline.ps1                      # Auto-detect month, full pipeline"
    Write-Host "  .\run_pipeline.ps1 -TestMonth 2023-05   # Explicit test month"
    Write-Host "  .\run_pipeline.ps1 -TestMonth 2023-05 -SkipMissing  # Skip missing data"
    Write-Host "  .\run_pipeline.ps1 -Legacy              # Use old train.py"
    Write-Host "  .\run_pipeline.ps1 -TrainOnly           # Train only"
    Write-Host "  .\run_pipeline.ps1 -PredictOnly         # Predict only"
    Write-Host "  .\run_pipeline.ps1 -Force               # Force re-train"
    Write-Host "  .\run_pipeline.ps1 -SkipTests           # Skip tests (risky)"
    Write-Host ""
    exit 0
}

# ============================================
# FUNCTIONS
# ============================================

function Write-Header {
    param([string]$Text)
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "  $Text" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
}

function Write-Step {
    param([int]$Number, [int]$Total, [string]$Text)
    Write-Host "[STEP $Number/$Total] $Text" -ForegroundColor Yellow
}

function Write-Success {
    param([string]$Text)
    Write-Host "[SUCCESS] $Text" -ForegroundColor Green
}

function Write-Failure {
    param([string]$Text)
    Write-Host "[FAILED] $Text" -ForegroundColor Red
}

function Write-Info {
    param([string]$Text)
    Write-Host "[INFO] $Text" -ForegroundColor Gray
}

function Write-Skip {
    param([string]$Text)
    Write-Host "[SKIP] $Text" -ForegroundColor DarkYellow
}

function Get-ElapsedTime {
    param([datetime]$StartTime)
    $elapsed = (Get-Date) - $StartTime
    if ($elapsed.TotalMinutes -ge 1) {
        return "{0:N1} min" -f $elapsed.TotalMinutes
    } else {
        return "{0:N1} sec" -f $elapsed.TotalSeconds
    }
}

function Ensure-SparkCluster {
    Write-Info "Ensuring Spark cluster is running..."
    $ErrorActionPreference = "Continue"
    docker compose up -d spark-master spark-worker-1 spark-worker-2 minio 2>&1 | Out-Null
    $ErrorActionPreference = "Stop"
    Start-Sleep -Seconds 2
}

function Run-SparkJob {
    param([string]$Script, [string]$Description, [string]$Args = "")
    
    Write-Info "Running: $Description"
    
    $cmd = "export SPARK_CONF_DIR=/opt/workdir/ex05_ml_prediction_service/conf && cd /opt/workdir/ex05_ml_prediction_service && spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/opt/spark-events --conf spark.ui.showConsoleProgress=true src/$Script $Args"
    
    $result = docker exec spark-master bash -c $cmd

    Write-Host $result
    return $LASTEXITCODE
}

function Run-PyTests {
    param([string]$TestFiles, [string]$Description)
    
    Write-Info "Running: $Description"
    
    $cmd = "cd /opt/workdir/ex05_ml_prediction_service && export PYTHONPATH=/opt/workdir/ex05_ml_prediction_service && pytest $TestFiles -v --tb=short"
    
    $result = docker exec spark-master bash -c $cmd

    Write-Host $result
    return $LASTEXITCODE
}

# ============================================
# MAIN PIPELINE
# ============================================

Write-Header "EX05 - ML PIPELINE ORCHESTRATOR"

# Determine mode
if ($Legacy) {
    $mode = "Legacy (train.py)"
    $currentMonth = "2023/01-02 (hardcoded)"
} else {
    $mode = "Sliding Window (ml_pipeline.py)"
    if ($TestMonth) {
        $currentMonth = $TestMonth
    } else {
        $currentMonth = (Get-Date).ToString("yyyy-MM") + " (auto-detected)"
    }
}

Write-Host "Pipeline Configuration:" -ForegroundColor White
Write-Host "  - Mode:         $mode" -ForegroundColor Cyan
Write-Host "  - Test Month:   $currentMonth" -ForegroundColor Cyan
Write-Host "  - Skip Tests:   $SkipTests" -ForegroundColor Gray
Write-Host "  - Train Only:   $TrainOnly" -ForegroundColor Gray
Write-Host "  - Predict Only: $PredictOnly" -ForegroundColor Gray
Write-Host "  - Force:        $Force" -ForegroundColor Gray
Write-Host ""

# Determine total steps
$totalSteps = 5
if ($SkipTests) { $totalSteps = 2 }
if ($TrainOnly -and -not $SkipTests) { $totalSteps = 3 }
if ($TrainOnly -and $SkipTests) { $totalSteps = 1 }
if ($PredictOnly -and -not $SkipTests) { $totalSteps = 2 }
if ($PredictOnly -and $SkipTests) { $totalSteps = 1 }

$currentStep = 0

# Ensure Spark cluster is running
Ensure-SparkCluster

# ============================================
# PHASE 1: PRE-TRAINING (if not PredictOnly)
# ============================================

if (-not $PredictOnly) {
    
    # Check if model exists and Force not set
    if ($Legacy) {
        $modelExistsPath = $MODEL_PATH
    } else {
        $modelExistsPath = $CURRENT_MODEL_PATH
    }
    
    if ((Test-Path $modelExistsPath) -and -not $Force) {
        Write-Info "Model already exists at $modelExistsPath"
        Write-Info "Use -Force to re-train, or -PredictOnly to skip training"
        $response = Read-Host "Continue with re-training? (y/N)"
        if ($response -ne "y" -and $response -ne "Y") {
            Write-Skip "Training skipped by user"
            $PredictOnly = $true
        }
    }
}

if (-not $PredictOnly) {
    
    # -----------------------------------------
    # STEP 1: Pre-training tests
    # -----------------------------------------
    if (-not $SkipTests) {
        $currentStep++
        Write-Header "STEP $currentStep - PRE-TRAINING VALIDATION"
        
        $stepStart = Get-Date
        Write-Step $currentStep $totalSteps "Validating data before training..."
        Write-Info "Tests: Schema validation, data quality, temporal ranges"
        
        $exitCode = Run-PyTests "tests/test_validation.py tests/test_month_range.py tests/test_ml_schema.py" "Pre-training tests"
        
        if ($exitCode -ne 0) {
            Write-Failure "Pre-training tests FAILED"
            Write-Host "[ERROR] Data validation failed. Fix issues before training." -ForegroundColor Red
            exit 1
        }
        
        Write-Success "Pre-training tests PASSED ($(Get-ElapsedTime $stepStart))"
    } else {
        Write-Skip "Pre-training tests (--SkipTests)"
    }
    
    # -----------------------------------------
    # STEP 2: Training & Error Analysis
    # -----------------------------------------
    $currentStep++
    Write-Header "STEP $currentStep - TRAINING & ERROR ANALYSIS"
    
    $stepStart = Get-Date
    Write-Step $currentStep $totalSteps "Training GBTRegressor model..."
    Write-Info "This may take several minutes depending on data volume"
    
    if ($Legacy) {
        # Legacy mode: use train.py
        $exitCode = Run-SparkJob "train.py" "Model training (legacy)"
    } else {
        # Sliding window mode: use ml_pipeline.py
        $mlArgs = ""
        if ($TestMonth) {
            $mlArgs += "--test-month $TestMonth "
        }
        if ($TrainMonths) {
            $mlArgs += "--train-months $TrainMonths "
        }
        if ($SkipMissing) {
            $mlArgs += "--skip-missing "
        }
        $mlArgs += "--model-registry-path $RegistryPath"
        
        $exitCode = Run-SparkJob "ml_pipeline.py" "Model training (sliding window)" $mlArgs
    }
    
    if ($exitCode -ne 0) {
        Write-Failure "Training FAILED"
        exit 1
    }
    
    Write-Success "Training completed ($(Get-ElapsedTime $stepStart))"
    Write-Info "Artifacts generated:"
    if ($Legacy) {
        Write-Info "  - models/ex05_spark_model/"
    } else {
        Write-Info "  - $RegistryPath/current/"
        Write-Info "  - $RegistryPath/model_registry.json"
    }
    Write-Info "  - reports/train_metrics.json"
    Write-Info "  - reports/error_summary.json"
    Write-Info "  - reports/error_by_price_bucket.json"
    
    # -----------------------------------------
    # STEP 3: Post-training quality tests
    # -----------------------------------------
    if (-not $SkipTests) {
        $currentStep++
        Write-Header "STEP $currentStep - POST-TRAINING QUALITY TESTS"
        
        $stepStart = Get-Date
        Write-Step $currentStep $totalSteps "Validating model quality..."
        Write-Info "Tests: RMSE < 10, R² > 0, MAE reasonable"
        
        $exitCode = Run-PyTests "tests/test_ml_quality.py" "Post-training quality tests"
        
        if ($exitCode -ne 0) {
            Write-Failure "Post-training quality tests FAILED"
            Write-Host "[ERROR] Model does not meet quality thresholds." -ForegroundColor Red
            Write-Host "[INFO] Review training metrics and consider adjustments." -ForegroundColor Yellow
            exit 1
        }
        
        Write-Success "Post-training quality tests PASSED ($(Get-ElapsedTime $stepStart))"
    } else {
        Write-Skip "Post-training quality tests (--SkipTests)"
    }
}

# ============================================
# PHASE 2: PREDICTION (if not TrainOnly)
# ============================================

if (-not $TrainOnly) {
    
    # Check if model exists
    if (-not (Test-Path $ACTIVE_MODEL_PATH)) {
        Write-Failure "Model not found at $ACTIVE_MODEL_PATH"
        Write-Host "[ERROR] Train a model first: .\run_pipeline.ps1 -TrainOnly" -ForegroundColor Red
        exit 1
    }
    
    # -----------------------------------------
    # STEP 4: Inference / Prediction
    # -----------------------------------------
    $currentStep++
    Write-Header "STEP $currentStep - INFERENCE / PREDICTION"
    
    $stepStart = Get-Date
    Write-Step $currentStep $totalSteps "Running model inference..."
    Write-Info "Loading model and scoring data from MinIO"
    
    $exitCode = Run-SparkJob "predict.py" "Model inference"
    
    if ($exitCode -ne 0) {
        Write-Failure "Inference FAILED"
        exit 1
    }
    
    Write-Success "Inference completed ($(Get-ElapsedTime $stepStart))"
    Write-Info "Artifacts generated:"
    Write-Info "  - reports/predict_report.json"
    
    # -----------------------------------------
    # STEP 5: Post-prediction plausibility tests
    # -----------------------------------------
    if (-not $SkipTests) {
        $currentStep++
        Write-Header "STEP $currentStep - POST-PREDICTION PLAUSIBILITY TESTS"
        
        $stepStart = Get-Date
        Write-Step $currentStep $totalSteps "Validating prediction results..."
        Write-Info "Tests: Non-negative, finite, business constraints"
        
        $exitCode = Run-PyTests "tests/test_ml_plausibility.py" "Post-prediction plausibility tests"
        
        if ($exitCode -ne 0) {
            Write-Failure "Post-prediction plausibility tests FAILED"
            Write-Host "[WARNING] Some predictions may violate business constraints." -ForegroundColor Yellow
            # Don't exit - predictions are still usable, just flagged
        } else {
            Write-Success "Post-prediction plausibility tests PASSED ($(Get-ElapsedTime $stepStart))"
        }
    } else {
        Write-Skip "Post-prediction plausibility tests (--SkipTests)"
    }
}

# ============================================
# SUMMARY
# ============================================

$totalTime = Get-ElapsedTime $pipelineStartTime

Write-Header "PIPELINE COMPLETE"

Write-Host "Summary:" -ForegroundColor White
Write-Host "  - Total time:    $totalTime" -ForegroundColor Gray
Write-Host "  - Steps run:     $currentStep" -ForegroundColor Gray
Write-Host ""

if (Test-Path $METRICS_PATH) {
    $metrics = Get-Content $METRICS_PATH | ConvertFrom-Json
    Write-Host "Model Metrics:" -ForegroundColor White
    Write-Host "  - RMSE:  $([math]::Round($metrics.metrics.rmse, 4))" -ForegroundColor Gray
    Write-Host "  - MAE:   $([math]::Round($metrics.metrics.mae, 4))" -ForegroundColor Gray
    Write-Host "  - R²:    $([math]::Round($metrics.metrics.r2, 4))" -ForegroundColor Gray
    Write-Host ""
}

Write-Host "Artifacts:" -ForegroundColor White
if ($Legacy) {
    if (Test-Path $MODEL_PATH) {
        Write-Host "  [OK] models/ex05_spark_model/" -ForegroundColor Green
    } else {
        Write-Host "  [--] models/ex05_spark_model/" -ForegroundColor DarkGray
    }
} else {
    if (Test-Path $CURRENT_MODEL_PATH) {
        Write-Host "  [OK] $RegistryPath/current/" -ForegroundColor Green
    } else {
        Write-Host "  [--] $RegistryPath/current/" -ForegroundColor DarkGray
    }
    $registryFile = Join-Path $REGISTRY_PATH "model_registry.json"
    if (Test-Path $registryFile) {
        Write-Host "  [OK] $RegistryPath/model_registry.json" -ForegroundColor Green
    } else {
        Write-Host "  [--] $RegistryPath/model_registry.json" -ForegroundColor DarkGray
    }
}
if (Test-Path $METRICS_PATH) {
    Write-Host "  [OK] reports/train_metrics.json" -ForegroundColor Green
} else {
    Write-Host "  [--] reports/train_metrics.json" -ForegroundColor DarkGray
}
if (Test-Path (Join-Path $SCRIPT_DIR "reports\error_summary.json")) {
    Write-Host "  [OK] reports/error_summary.json" -ForegroundColor Green
} else {
    Write-Host "  [--] reports/error_summary.json" -ForegroundColor DarkGray
}
if (Test-Path $PREDICT_REPORT_PATH) {
    Write-Host "  [OK] reports/predict_report.json" -ForegroundColor Green
} else {
    Write-Host "  [--] reports/predict_report.json" -ForegroundColor DarkGray
}

Write-Host ""
Write-Host "Next steps:" -ForegroundColor White
Write-Host "  - View Streamlit dashboard: http://localhost:8501" -ForegroundColor Gray
Write-Host "  - View Spark UI: http://localhost:8081" -ForegroundColor Gray
Write-Host "  - Run prediction only: .\run_pipeline.ps1 -PredictOnly" -ForegroundColor Gray
Write-Host ""

Write-Success "Pipeline completed successfully!"
