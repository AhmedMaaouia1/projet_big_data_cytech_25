# ============================================================================
# Script d'initialisation Airflow pour NYC Taxi Pipeline (PowerShell)
# ============================================================================
# Usage: .\init-airflow.ps1
# ============================================================================

$ErrorActionPreference = "Stop"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "  Initialisation Airflow - NYC Taxi" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# Vérifier que Docker est en cours d'exécution
try {
    docker info | Out-Null
}
catch {
    Write-Host "❌ Docker n'est pas en cours d'exécution" -ForegroundColor Red
    exit 1
}

# Vérifier que le réseau nyc-net existe
$networks = docker network ls --format "{{.Name}}"
if ($networks -notcontains "nyc-net") {
    Write-Host "⚠️ Le réseau nyc-net n'existe pas." -ForegroundColor Yellow
    Write-Host "   Lancez d'abord: docker-compose up -d (depuis la racine du projet)" -ForegroundColor Yellow
    exit 1
}

# Créer le fichier .env s'il n'existe pas
if (-not (Test-Path ".env")) {
    Write-Host "📝 Création du fichier .env..." -ForegroundColor Yellow
    Copy-Item ".env.example" ".env"
    
    # Ajouter AIRFLOW_UID (utiliser 50000 par défaut sur Windows)
    Add-Content -Path ".env" -Value ""
    Add-Content -Path ".env" -Value "# Airflow User ID (Windows default)"
    Add-Content -Path ".env" -Value "AIRFLOW_UID=50000"
    
    Write-Host "✅ Fichier .env créé" -ForegroundColor Green
}

# Créer les dossiers nécessaires
Write-Host "📁 Création des dossiers..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path "logs" | Out-Null
New-Item -ItemType Directory -Force -Path "plugins" | Out-Null
New-Item -ItemType Directory -Force -Path "scripts" | Out-Null

# Démarrer Airflow
Write-Host "🚀 Démarrage d'Airflow..." -ForegroundColor Yellow
docker-compose up -d

# Attendre l'initialisation
Write-Host "⏳ Attente de l'initialisation (30 secondes)..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# Vérifier que les services sont up
Write-Host "🔍 Vérification des services..." -ForegroundColor Yellow
docker-compose ps

Write-Host ""
Write-Host "==========================================" -ForegroundColor Green
Write-Host "✅ Airflow initialisé avec succès!" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
Write-Host ""
Write-Host "📌 Accès:" -ForegroundColor Cyan
Write-Host "   URL: http://localhost:8080"
Write-Host "   User: airflow"
Write-Host "   Password: airflow"
Write-Host ""
Write-Host "📌 Commandes utiles:" -ForegroundColor Cyan
Write-Host "   docker-compose logs -f airflow-webserver"
Write-Host "   docker-compose logs -f airflow-scheduler"
Write-Host "   docker-compose down"
Write-Host ""
