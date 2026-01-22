#!/bin/bash
# ============================================================
# Objectif : Initialiser la couche de stockage du pipeline data
#   - Création des dossiers ddl/ dml/ si inexistants
#   - Vérification existence des dossiers data/processed
#   - Vérification accès PostgreSQL
#   - Initialisation DDL
#   - Exécution DML peuplement dim date
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

# Credentials PostgreSQL (docker-compose)
PGUSER="data"
PGPASSWORD="data"
PGDATABASE="covid_dw"
PGHOST="localhost"
PGPORT=5432
export PGPASSWORD=$PGPASSWORD

# ------------------------------------------------------------
# Création de la structure de dossiers si nécessaire
# ------------------------------------------------------------
echo "Vérification / création des dossiers ddl et dml si inexistant..."

mkdir -p src/storage/postgres/{ddl,dml}

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
# Initialisation PostgreSQL : exécutions des scripts DDL
# ------------------------------------------------------------
echo "Initialisation des schémas et tables PostgreSQL..."

for sql_file in "$POSTGRES_DDL_PATH"/0*_*.sql; do
    if [ -f "$sql_file" ]; then
        echo "Exécution de $sql_file ..."
        psql -U $PGUSER -h $PGHOST -p $PGPORT -d $PGDATABASE -f "$sql_file"
    else
        echo "Fichier $sql_file introuvable, étape ignorée."
    fi
done

echo "DDL PostgreSQL exécutés."

# ------------------------------------------------------------
# Population de la dimension date
# ------------------------------------------------------------
echo "Peuplement de la dimension date..."

DIM_DATE_SQL="$POSTGRES_DML_PATH/010_insert_dim_date.sql"

if [ -f "$DIM_DATE_SQL" ]; then
    echo "Exécution du DML pour peupler dim_date : $DIM_DATE_SQL ..."
    psql -U $PGUSER -h $PGHOST -p $PGPORT -d $PGDATABASE -f "$DIM_DATE_SQL"
    echo "Population de dim_date terminée avec succès."
else
    echo "Fichier DML dim_date introuvable : $DIM_DATE_SQL"
    echo "Étape ignorée."
fi

# ------------------------------------------------------------
# To DO : Initialisation MongoDB
# ------------------------------------------------------------
# echo "Initialisation MongoDB..."
# mongo < init_mongodb.js

echo "Initialisation de la couche de stockage terminée."
