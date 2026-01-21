#!/bin/bash
# ============================================================
# Script : init_storage.sh
# Objectif : Initialiser la couche de stockage du pipeline data
#   - Création des dossiers et fichiers SQL si inexistants
#   - Vérification existence des dossiers data/processed
#   - Vérification accès PostgreSQL et initialisation DDL
#
# Usage :
#   bash scripts/bash/init_storage.sh
# ============================================================

set -e  # Stop le script si erreur

echo "Initialisation de la couche de stockage..."

# ------------------------------------------------------------
# Variables
# ------------------------------------------------------------
PROJECT_ROOT=$(pwd)
DATA_PROCESSED_PATH="$PROJECT_ROOT/data/processed/owid_covid"
POSTGRES_DDL_PATH="$PROJECT_ROOT/src/storage/postgres/ddl"
POSTGRES_DML_PATH="$PROJECT_ROOT/src/storage/postgres/dml"
POSTGRES_INIT_SCRIPT="$POSTGRES_DDL_PATH/001_create_schema.sql"

# Credentials PostgreSQL (docker-compose)
PGUSER="data"
PGPASSWORD="data"
PGDATABASE="datadb"
PGHOST="localhost"
PGPORT=5432
export PGPASSWORD=$PGPASSWORD

# ------------------------------------------------------------
# Création de la structure de fichiers si nécessaire
# ------------------------------------------------------------
echo "Vérification / création des dossiers et fichiers SQL..."

mkdir -p src/storage/postgres/{ddl,dml}

# Fichiers SQL : création si inexistants
[ -f "$POSTGRES_DDL_PATH/001_create_schema.sql" ] || touch "$POSTGRES_DDL_PATH/001_create_schema.sql"
[ -f "$POSTGRES_DDL_PATH/002_create_dimensions.sql" ] || touch "$POSTGRES_DDL_PATH/002_create_dimensions.sql"
[ -f "$POSTGRES_DDL_PATH/003_create_fact_tables.sql" ] || touch "$POSTGRES_DDL_PATH/003_create_fact_tables.sql"
[ -f "$POSTGRES_DML_PATH/010_insert_data.sql" ] || touch "$POSTGRES_DML_PATH/010_insert_data.sql"

# README : création si inexistants
[ -f src/storage/postgres/README.md ] || touch src/storage/postgres/README.md

echo "Structure de stockage prête."

# ------------------------------------------------------------
# Vérification des dossiers data/processed
# ------------------------------------------------------------
# Vérifier si data/processed existe et n'est pas vide
if [ ! -d "$DATA_PROCESSED_PATH" ] || [ -z "$(ls -A $DATA_PROCESSED_PATH)" ]; then
    echo "Dossier data/processed vide ou inexistant : pas de données à charger"
    echo "Veuillez d'abord exécuter la transformation PySpark"
    exit 1
fi

# ------------------------------------------------------------
# Vérification accès PostgreSQL
# ------------------------------------------------------------
echo "Vérification de la connexion à PostgreSQL..."

MAX_TRIES=5  # nombre de tentatives
SLEEP_TIME=2  # secondes entre chaque tentative
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
# Initialisation PostgreSQL
# ------------------------------------------------------------
echo "Initialisation de PostgreSQL..."

if [ -f "$POSTGRES_INIT_SCRIPT" ]; then
    psql -U $PGUSER -h $PGHOST -p $PGPORT -d $PGDATABASE -f "$POSTGRES_INIT_SCRIPT"
    echo "Schéma PostgreSQL initialisé."
else
    echo "Script PostgreSQL non trouvé : $POSTGRES_INIT_SCRIPT"
    echo "Étape PostgreSQL ignorée."
fi

# ------------------------------------------------------------
# To DO : Initialisation MongoDB
# ------------------------------------------------------------
# echo "Initialisation MongoDB..."
# mongo < init_mongodb.js

echo "Initialisation de la couche de stockage terminée."
