# Design de l’orchestration – Pipeline Vaccination COVID (OWID)

## Contexte et objectif
L’orchestration a été mise en place via **Airflow** pour automatiser et superviser les pipelines batch et streaming OWID COVID-19.  

Objectifs :  
- fiabiliser les traitements batch et streaming ;  
- centraliser la supervision et les alertes ;  
- permettre une **exécution reproductible et contrôlée** des DAGs.

## Diagrammes de référence
- **Flux de données global** : [`flux_donnees.png`](../docs/diagrams/flux_donnees.png)
  Représente le parcours des données depuis la source OWID jusqu’aux tables finales (staging, dimensions, faits).  

- **Orchestration Airflow** : [`airflow_dags.png`](../docs/diagrams/airflow_dags.png)  
  Représente les DAGs batch et streaming, leurs tâches et dépendances.

## Pipelines automatisés
Deux DAGs principaux ont été définis : un pour le traitement batch et un pour le streaming.

### Pipeline batch – `owid_batch_pipeline`
- **Fichier DAG** : [`owid_batch_pipeline.py`](../airflow/dags/owid_batch_pipeline.py)  
- **Objectif** : ingestion, transformation PySpark et chargement dans PostgreSQL (staging + tables dim/fact)  
- **Fréquence** : quotidienne (`@daily`)  
- **Tâches principales** :  
  - Création du dossier RAW et téléchargement du CSV OWID  
  - Vérification de l’intégrité des fichiers  
  - Transformation PySpark vers Parquet  
  - Initialisation PostgreSQL via [`init_storage.sh`](../scripts/bash/init_storage.sh)  
  - Chargement des données dans la table de staging via [`load_owid_postgres.py`](../src/storage/postgres/load_owid_postgres.py)  
  - Vérification des données staging  
  - Population des tables `dim` et `fact` via [`run_dml.sh`](../scripts/bash/run_dml.sh)  

### Pipeline streaming – `owid_streaming_pipeline`
- **Fichier DAG** : [`owid_streaming_pipeline.py`](../airflow/dags/owid_streaming_pipeline.py)  
- **Objectif** : simulation d’un flux de données quasi temps réel via Kafka et Spark Structured Streaming  
- **Fréquence** : manuel / event-driven  
- **Tâches principales** :  
  - Création / validation du topic Kafka  
  - Lancement du producer Kafka (simulation micro-batchs à partir de Parquet) via [`owid_event_producer.py`](../src/streaming/producer/owid_event_producer.py)  
  - Lancement du consumer Spark Structured Streaming vers PostgreSQL staging via [`owid_stream_processing.py`](../src/streaming/consumer/owid_stream_processing.py)  

## Monitoring et fiabilité
- Airflow gère les **retries**, les logs centralisés et les alertes par email en cas d’échec.  
- Chaque DAG contient des **checks qualité** :  
  - batch : fichiers CSV téléchargés, Parquet générés, staging PostgreSQL rempli ;  
  - streaming : topic Kafka existant, consommation et écriture dans staging réussies.  
- Le batch et le streaming partagent une **table de staging** pour garantir la cohérence des données.

## Paramétrage et portabilité
- Les DAGs utilisent des **variables Airflow et connections** : PostgreSQL (batch) et Kafka (streaming).  
- Compatible **exécution locale**, **Docker**, ou **cluster Airflow**.  

## Synthèse
L’orchestration via Airflow permet :  
- un **pilotage fiable** des pipelines batch et streaming ;  
- la **reproductibilité** et la traçabilité des exécutions ;  
- une **supervision centralisée** pour détection rapide des erreurs ;  
- la **cohérence des données** via staging partagé entre batch et streaming.
