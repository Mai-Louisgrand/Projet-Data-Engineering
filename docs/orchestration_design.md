# Design de l’orchestration – Pipeline Vaccination COVID (OWID)

## Contexte et objectif
L’orchestration est assurée par **Apache Airflow** afin d’automatiser et superviser les pipelines batch et streaming.
Le pipeline est exécuté localement (via Docker) tout en orchestrant des traitements s’appuyant sur des services **cloud GCP** (Google Cloud Storage et BigQuery).

Objectifs :  
- fiabiliser les traitements batch et streaming ;  
- centraliser le monitoring et les alertes ;  
- permettre une **exécution reproductible et contrôlée** des DAGs.

## Diagrammes de référence
- **Flux de données global** : [`data_flow.png`](../docs/diagrams/data_flow.png)
  Représente le parcours des données depuis la source OWID vers le Data Lake (GCS) et la Data Warehouse (BigQuery).

- **Orchestration Airflow** : [`airflow_dags.png`](../docs/diagrams/airflow_dags.png)  
  Représente les DAGs batch et streaming, leurs tâches et dépendances.

## Pipelines automatisés
Deux DAGs principaux ont été définis : un pour le traitement batch et un pour le streaming.

### Pipeline batch – `owid_batch_pipeline`
- **Fichier DAG** : [`owid_batch_pipeline.py`](../airflow/dags/owid_batch_pipeline.py)  
- **Objectif** : ingestion, transformation PySpark et chargement dans BigQuery (staging + tables analytiques) 
- **Fréquence** : quotidienne (`@daily`)  
- **Tâches principales** :  
  - Ingestion vers Data Lake (GCS) :
    - Téléchargement du dataset OWID depuis la source publique  
    - Upload direct du fichier CSV dans **GCS (`raw/`)** via client Python
  - Transformation PySpark (GCS → GCS) :
    - Lecture du CSV depuis **GCS raw**  
    - Nettoyage et préparation des données (sélection des colonnes métiers, traitement des valeurs aberrantes, recalcul des indicateurs)
    - Écriture des données transformées en **Parquet sur GCS (`processed/`)**
  - Initialisation du Data Warehouse (BigQuery) :
    - Création des datasets BigQuery
    - Exécution des scripts **DDL/DML** via client Python BigQuery 
  - Chargement GCS → BigQuery (staging temporaire) : load natif des fichiers Parquet depuis **GCS processed** vers une **table de staging temporaire** BigQuery
  - Post-processing dans BigQuery :
    - Ajout de métadonnées (`load_date`)  
    - Déduplication des données  
    - Upsert vers la **table de staging principale**  
  - Alimentation du modèle analytique : exécution des requêtes DML pour alimenter les tables `dim_location` et `fact_vaccination`  

### Pipeline streaming – `owid_streaming_pipeline`
- **Fichier DAG** : [`owid_streaming_pipeline.py`](../airflow/dags/owid_streaming_pipeline.py)  
- **Objectif** : simulation d’un flux de données quasi temps réel via Kafka et Spark Structured Streaming, avec stockage dans BigQuery
- **Fréquence** : manuel / event-driven  
- **Tâches principales** :  
  - Préparation Kafka : création / validation du topic Kafka  
  - Production des événements : 
    - Lecture des données **Parquet depuis GCS (`processed/`)** à une date donnée 
    - Simulation d’événements de vaccination via Kafka Producer
  - Consommation et traitement streaming :
    - Lecture des messages Kafka via **Spark Structured Streaming**  
    - Validation du schéma et traitement en micro-batch  
    - Écriture vers BigQuery dans une **table de staging temporaire BigQuery** via connecteur Spark BigQuery
  - Post-processing dans BigQuery :
    - Déduplication des données  
    - Upsert vers la **table de staging principale**  
  - Alimentation du modèle analytique : exécution des requêtes DML pour alimenter les tables `dim_location` et `fact_vaccination`

## Monitoring et fiabilité
- Airflow gère les **retries**, les logs centralisés et les alertes par email en cas d’échec.  
- Le batch et le streaming convergent vers une **staging commune dans BigQuery**, garantissant la cohérence des données.

## Paramétrage et portabilité
- Pipeline exécuté localement via **Docker & Docker Compose** (Airflow, Spark, Kafka).  
- Intégration avec des services cloud **GCP** :  
  - **Google Cloud Storage (GCS)** pour le Data Lake  
  - **BigQuery** pour le Data Warehouse
- Gestion des accès via **credentials GCP** (service account). 

## Synthèse
L’orchestration via Airflow permet :  
- un **pilotage fiable** des pipelines batch et streaming ;  
- la **reproductibilité** et la traçabilité des exécutions ;  
- une **supervision centralisée** pour détection rapide des erreurs ;  
- la **cohérence des données** via staging partagé entre batch et streaming.
