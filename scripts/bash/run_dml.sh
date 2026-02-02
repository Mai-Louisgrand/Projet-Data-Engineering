#!/bin/bash
# ============================================================
# Purpose: Populate dimension and fact tables in the data warehouse
#          from the staging table
#
# Features:
# - Verify PostgreSQL connectivity
# - Execute all DML scripts in dml/ folder (excluding dim_date)
#
# Usage:
#   bash scripts/bash/run_dml.sh
# ============================================================

set -e  # Exit immediately if any command fails

echo "Début du peuplement des tables dim/fact"

# ------------------------------------------------------------
# Variables
# ------------------------------------------------------------
PROJECT_ROOT=$(pwd)
POSTGRES_DML_PATH="$PROJECT_ROOT/src/storage/postgres/dml"

# PostgreSQL credentials
PGUSER="data"
PGPASSWORD="data"
PGDATABASE="covid_dw"
PGHOST="localhost"
PGPORT=5432
export PGPASSWORD=$PGPASSWORD

# ------------------------------------------------------------
# Verify PostgreSQL connectivity
# ------------------------------------------------------------
echo "Vérification de la connexion à PostgreSQL..."

MAX_TRIES=5
SLEEP_TIME=2
count=0

while ! pg_isready -h $PGHOST -p $PGPORT -U $PGUSER > /dev/null 2>&1; do
    count=$((count+1))
    if [ $count -ge $MAX_TRIES ]; then
        echo "PostgreSQL ne répond pas après $((MAX_TRIES*SLEEP_TIME)) secondes."
        echo "Veuillez vérifier que votre container PostgreSQL est bien démarré."
        exit 1
    fi
    echo "PostgreSQL pas encore prêt, tentative $count/$MAX_TRIES…"
    sleep $SLEEP_TIME
done

echo "PostgreSQL prêt et accessible"

# ------------------------------------------------------------
# Execute DML scripts
# ------------------------------------------------------------
echo "Exécution des scripts DML pour peupler dim et fact..."

for dml_file in "$POSTGRES_DML_PATH"/[0-9]*_*.sql; do
    
    # Exclure 010_insert_dim_date.sql
    if [[ "$(basename $dml_file)" == "010_insert_dim_date.sql" ]]; then
        echo "Ignoré (dim_date déjà exécutée) : $dml_file"
        continue
    fi

    if [ -f "$dml_file" ]; then
        echo "Exécution de $dml_file ..."
        psql -U $PGUSER -h $PGHOST -p $PGPORT -d $PGDATABASE -f "$dml_file"
    else
        echo "Fichier $dml_file introuvable, étape ignorée."
    fi
done

echo "Peuplement des tables dim/fact terminé."
