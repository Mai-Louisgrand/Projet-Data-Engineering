# Design de la couche de stockage – Pipeline Vaccination COVID (OWID)

## Contexte et objectif
La couche de stockage a été conçue pour **supporter à la fois le traitement batch et streaming des données** OWID COVID-19. Elle est basée sur **PostgreSQL** comme système de gestion de base de données relationnelle.

Objectifs principaux :  
- centraliser les données transformées dans un **data warehouse analytique** (`covid_dw`) ;  
- permettre une **analyse rapide et cohérente** via des tables en étoile, facilitant les **jointures rapides** et les analyses temporelles et géographiques ;
- garantir la reproductibilité et l'idempotence des chargements batch et streaming.

Le stockage est structuré en **3 schémas principaux** :  
1. `staging` – tables temporaires alimentées par Spark (batch ou streaming) ;  
2. `dim` – tables de dimensions (Date, Location) ;  
3. `fact` – tables de faits analytiques (Vaccination).

## Architecture de la base de données
### Schéma en étoile
- **Dimension `dim_date`** : chaque ligne correspond à une date du calendrier.  
- **Dimension `dim_location`** : contient l’information pays / iso_code pour relier les faits aux géographies.  
- **Table de faits `fact_vaccination`** : grain 1 pays / 1 date, table centrale pour les analyses de vaccination.  
- **Table de staging `stg_owid_covid`** : reçoit les données batch ou streaming, nettoyées et transformées.

**Diagramme :** [`schema_bdd.png`](../docs/diagrams/schema_bdd.png) 

## Détails des scripts SQL
### Scripts DDL
Les scripts DDL créent la structure en étoile :
- [`create_schemas.sql`](../src/storage/postgres/ddl/001_create_schema.sql) : création des **schémas** (`staging`, `dim`, `fact`) ;  
- [`create_dimensions.sql`](../src/storage/postgres/ddl/002_create_dimensions.sql) : création des **tables de dimensions** (`dim_date`, `dim_location`) ;  
- [`create_fact_tables.sql`](../src/storage/postgres/ddl/003_create_fact_tables.sql) : création de la **table de faits** (`fact_vaccination`) ;  
- [`create_staging.sql`](../src/storage/postgres/ddl/004_create_staging.sql) création de la **table de staging** (`stg_owid_covid`).  

Ces scripts initialisent une base en **étoile**, optimisée pour les requêtes analytiques.

### Scripts DML
Les scripts DML peuplent les tables (hormis la table de staging) :  
- [`insert_dim_date.sql`](../src/storage/postgres/dml/010_insert_dim_date.sql) : remplit `dim_date` via génération de dates entre 2019 et 2030 ;  
- [`insert_dim_location.sql`](../src/storage/postgres/dml/020_insert_dim_location.sql) : remplit `dim_location` à partir des données de `stg_owid_covid` ;  
- [`insert_fact_vaccination.sql`](../src/storage/postgres/dml/100_insert_fact_vaccination.sql) : remplit `fact_vaccination` à partir des données de `stg_owid_covid`.  

Le grain des faits est garanti : **1 pays / 1 date**.

## Scripts d’exécution et d'ingestion
### Scripts bash
- [`init_storage.sh`](../scripts/bash/init_storage.sh) : initialise la couche de stockage complète en exécutant les scripts DDL et le script DML pour `dim_date`. 
- [`run_dml.sh`](../scripts/bash/run_dml.sh) : peuple les tables `dim_location` et `fact_vaccination` depuis `stg_owid_covid`, en exécutant les scripts DML (hors celui pour `dim_date`).

### `load_owid_postgres.py`
[`load_owid_postgres.py`](../src/storage/postgres/load_owid_postgres.py) permet de charger les données issues du traitement batch dans la table de staging `stg_owid_covid`.
Les données sont lues à partir des fichiers de données transformées (fichiers Parquet). Elles sont enrichies avec des métadonnées techniques (date de chargement) et écrites dans la table `stg_owid_covid` via JDBC.

## Caractéristiques
- **Idempotence** : les tables sont conçues pour éviter les doublons grâce à des contraintes `UNIQUE` côté PostgreSQL et à la logique de chargement contrôlée.  
- **Séparation ingestion / transformation** : les tables finales ne sont jamais alimentées directement par Spark, garantissant la cohérence des données.  
- **Portabilité** : les scripts bash et Python peuvent s’exécuter localement, dans Docker ou via Airflow.

## Limites et axes d’amélioration
- La génération de `dim_date` couvre une plage fixe (2019-2030) et doit être mise à jour régulièrement pour rester pertinente.  
- La table de staging **n’historise pas les états précédents**, elle est écrasée à chaque chargement batch. Une stratégie d’archivage pourrait être envisagée pour conserver un historique complet.

## Synthèse
La couche de stockage OWID COVID :  
- suit un modèle en **étoile** adapté à l’analyse analytique ;  
- est **reproductible** grâce à des scripts DDL/DML/batch idempotents,
- permet une ingestion batch et streaming fiable via staging partagé.
