# Design de la couche de stockage – Pipeline Vaccination COVID (OWID)

## Contexte et objectif
La couche de stockage a été conçue pour supporter les pipelines **batch et streaming** dans une architecture **cloud moderne** basée sur :
- un **Data Lake** : Google Cloud Storage (GCS)  
- un **Data Warehouse** : BigQuery  

Objectifs principaux :  
- centraliser les données transformées dans un **environnement analytique scalable** ;
- permettre une **analyse performante** via une modélisation en étoile, facilitant les **jointures rapides** et les analyses temporelles et géographiques ;
- garantir la **reproductibilité et l'idempotence** des chargements batch et streaming ;
- séparer clairement les couches **Data Lake (stockage brut et transformé)** et **Data Warehouse (modélisation analytique)**.

## Architecture de stockage
### Data Lake – Google Cloud Storage (GCS)
Le Data Lake est structuré en deux zones :
- **`raw/`** :  
  - stockage des données brutes OWID au format CSV  
  - ingestion directe depuis la source sans stockage local intermédiaire  
- **`processed/`** :  
  - données transformées par Spark au format Parquet  
  - données nettoyées, prêtes pour ingestion dans BigQuery

### Data Warehouse – BigQuery
BigQuery est utilisé comme **entrepôt de données analytique**.
Les données sont organisées en **datasets** :
- `staging` : tables d’ingestion intermédiaires  
- `dim` – tables de dimensions (Date, Location) ;  
- `fact` – tables de faits analytiques (Vaccination).

## Modélisation analytique
### Schéma en étoile
- **Dimension `dim_date`** : chaque ligne correspond à une date du calendrier.  
- **Dimension `dim_location`** : contient l’information pays / iso_code pour relier les faits aux géographies.  
- **Table de faits `fact_vaccination`** : grain 1 pays / 1 date, table centrale pour les analyses de vaccination. 

**Diagramme :** [`dwh_schema.png`](../docs/diagrams/dwh_schema.png) 

### Tables de staging
Deux niveaux de staging sont utilisés :
1. **Table de staging temporaire**  
   - utilisée lors du chargement des données batch et streaming  
   - reçoit les données depuis GCS (batch) ou Spark (streaming)
2. **Table de staging principale (`stg_owid_covid`)**  
   - table consolidée après nettoyage  
   - utilisée comme source pour les tables analytiques 

## Initialisation et gestion des tables
- Création des datasets et tables via **client Python BigQuery**  
- Exécution des scripts **DDL/DML adaptés à BigQuery**  
- Gestion centralisée depuis Airflow   

## Processus de chargement (Batch)
### 1. Chargement GCS → BigQuery (staging temporaire)
- Load natif BigQuery depuis les fichiers Parquet stockés sur **GCS (`processed/`)**  
- Chargement dans une **table de staging temporaire**
### 2. Post-traitement dans BigQuery
- Ajout de métadonnées (`load_date`)  
- Déduplication des données
### 3. Upsert vers staging principale
- Insertion / mise à jour dans `stg_owid_covid`  
- Garantit l’idempotence des chargements  
### 4. Alimentation du modèle en étoile
- Exécution de requêtes SQL (DML) pour alimenter :
  - `dim_date`  
  - `dim_location`  
  - `fact_vaccination`

## Processus de chargement (Streaming)
### 1. Chargement Spark Structured Streaming → BigQuery (staging temporaire)
- Lecture des événements via **Spark Structured Streaming** depuis Kafka  
- Écriture directe dans **BigQuery (staging temporaire)** via connecteur Spark BigQuery  
### 2. Processus similaire à pipeline batch 
- Post-traitement dans BigQuery
- Upsert vers staging principale
- Alimentation du modèle en étoile

## Caractéristiques
- **Scalabilité** : BigQuery permet de traiter de grands volumes sans gestion d’infrastructure  
- **Idempotence** : garantie par une logique d’upsert contrôlée. 
- **Séparation des couches** : distinction claire entre GCS (stockage) et BigQuery (analyse).
- **Convergence batch & streaming** utilisation de tables de staging communes.  

## Limites et axes d’amélioration
- La table de staging **n’historise pas les états précédents**, elle est écrasée à chaque chargement batch. Une stratégie d’archivage pourrait être envisagée pour conserver un historique complet.

## Synthèse
La couche de stockage du pipeline OWID COVID :
- implémente une architecture **Data Lake (GCS) → Data Warehouse (BigQuery)**, **scalable** ;
- repose sur une modélisation en **étoile adaptée à l’analytique** ;  
- garantit des chargements **fiables, idempotents et reproductibles** ;  
- permet une ingestion unifiée **batch et streaming** via une staging commune.
