#!/bin/bash
# ============================================================
# Purpose: Initialize the storage layer for the data pipeline
#   - Check PostgreSQL connectivity
#   - Execute DDL scripts to initialize schemas and tables
#   - Execute DML scripts to populate date dimension
#
# Usage:
#   bash scripts/bash/init_storage.sh
# ============================================================

set -e  # Exit immediately if any command fails

echo "[init_storage] Initialisation de la couche de stockage..."

# ------------------------------------------------------------
# Variables
# ------------------------------------------------------------
PROJECT_ROOT=$(pwd)
POSTGRES_DDL_PATH="$PROJECT_ROOT/src/storage/postgres/ddl"
POSTGRES_DML_PATH="$PROJECT_ROOT/src/storage/postgres/dml"

# PostgreSQL credentials
PGHOST=${PGHOST:-localhost}
PGUSER=${PGUSER:-data}
PGPASSWORD=${PGPASSWORD:-data}
PGDATABASE=${PGDATABASE:-covid_dw}
PGPORT=${PGPORT:-5432}
export PGPASSWORD=$PGPASSWORD

# ------------------------------------------------------------
# Verify PostgreSQL connectivity
# ------------------------------------------------------------
echo "[init_storage] Vérification de la connexion à PostgreSQL..."

MAX_TRIES=5   # maximum number of connection attempts
SLEEP_TIME=2  # seconds between attempts
count=0

while ! pg_isready -h $PGHOST -p $PGPORT -U $PGUSER > /dev/null 2>&1; do
    count=$((count+1))
    if [ $count -ge $MAX_TRIES ]; then
        echo "[init_storage] PostgreSQL ne répond pas après $((MAX_TRIES*SLEEP_TIME)) secondes."
        echo "[init_storage] Veuillez vérifier que votre container PostgreSQL est bien démarré."
        exit 1
    fi
    echo "[init_storage] PostgreSQL pas encore prêt, tentative $count/$MAX_TRIES…"
    sleep $SLEEP_TIME
done

echo "[init_storage] PostgreSQL prêt et accessible"

# ------------------------------------------------------------
# Execute PostgreSQL DDL scripts
# ------------------------------------------------------------
echo "[init_storage] Initialisation des schémas et tables PostgreSQL..."

for sql_file in "$POSTGRES_DDL_PATH"/0*_*.sql; do
    if [ -f "$sql_file" ]; then
        echo "[init_storage] Exécution de $sql_file ..."
        psql -U $PGUSER -h $PGHOST -p $PGPORT -d $PGDATABASE -f "$sql_file"
    else
        echo "[init_storage] Fichier $sql_file introuvable, étape ignorée."
    fi
done

echo "[init_storage] DDL PostgreSQL exécutés."

# ------------------------------------------------------------
# Populate the date dimension
# ------------------------------------------------------------
echo "[init_storage] Peuplement de la dimension date..."

DIM_DATE_SQL="$POSTGRES_DML_PATH/010_insert_dim_date.sql"

if [ -f "$DIM_DATE_SQL" ]; then
    echo "[init_storage] Exécution du DML pour peupler dim_date : $DIM_DATE_SQL ..."
    psql -U $PGUSER -h $PGHOST -p $PGPORT -d $PGDATABASE -f "$DIM_DATE_SQL"
    echo "[init_storage] Population de dim_date terminée avec succès."
else
    echo "[init_storage] Fichier DML dim_date introuvable : $DIM_DATE_SQL"
    echo "[init_storage] Étape ignorée."
fi

echo "[init_storage] Initialisation de la couche de stockage terminée."