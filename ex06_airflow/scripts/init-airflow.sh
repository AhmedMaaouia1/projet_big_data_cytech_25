#!/bin/bash
# ============================================================================
# Script d'initialisation Airflow pour NYC Taxi Pipeline
# ============================================================================
# Usage: ./init-airflow.sh
# ============================================================================

set -e

echo "=========================================="
echo "  Initialisation Airflow - NYC Taxi"
echo "=========================================="

# Vérifier que Docker est en cours d'exécution
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker n'est pas en cours d'exécution"
    exit 1
fi

# Vérifier que le réseau nyc-net existe
if ! docker network ls | grep -q "nyc-net"; then
    echo "⚠️ Le réseau nyc-net n'existe pas."
    echo "   Lancez d'abord: docker-compose up -d (depuis la racine du projet)"
    exit 1
fi

# Créer le fichier .env s'il n'existe pas
if [ ! -f .env ]; then
    echo "📝 Création du fichier .env..."
    cp .env.example .env
    
    # Ajouter AIRFLOW_UID
    echo "" >> .env
    echo "# Airflow User ID" >> .env
    echo "AIRFLOW_UID=$(id -u)" >> .env
    
    echo "✅ Fichier .env créé"
fi

# Créer les dossiers nécessaires
echo "📁 Création des dossiers..."
mkdir -p logs plugins scripts

# Définir les permissions
echo "🔒 Configuration des permissions..."
chmod -R 777 logs

# Démarrer Airflow
echo "🚀 Démarrage d'Airflow..."
docker-compose up -d

# Attendre l'initialisation
echo "⏳ Attente de l'initialisation (30 secondes)..."
sleep 30

# Vérifier que les services sont up
echo "🔍 Vérification des services..."
docker-compose ps

echo ""
echo "=========================================="
echo "✅ Airflow initialisé avec succès!"
echo "=========================================="
echo ""
echo "📌 Accès:"
echo "   URL: http://localhost:8080"
echo "   User: airflow"
echo "   Password: airflow"
echo ""
echo "📌 Commandes utiles:"
echo "   docker-compose logs -f airflow-webserver"
echo "   docker-compose logs -f airflow-scheduler"
echo "   docker-compose down"
echo ""
