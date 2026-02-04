# Projet Data Engineering – Pipeline OWID COVID-19

## Présentation du projet
Ce projet est un **pipeline de data engineering de bout en bout**, développé dans un cadre personnel afin de :
- reproduire des **conditions proches de la production**,
- mettre en œuvre les **bonnes pratiques data engineering**,
- consolider mes compétences en **batch et streaming processing**.

Le pipeline exploite les données publiques COVID-19 de **Our World in Data (OWID)** et permet de suivre l’évolution de la vaccination à l’échelle mondiale.

## Objectif métier
> **Suivre l’évolution de la couverture vaccinale COVID-19 dans le monde et permettre la détection d’anomalies ou de retards dans la vaccination.**

Cette problématique guide :
- la modélisation analytique (dimensions / faits),
- le choix d’une architecture **batch + streaming**,
- la mise en place d’une **staging commune**.

## Source de données
- **Source** : Our World in Data – COVID-19
- **Format** : CSV
- **Accès** : dépôt GitHub public
- **URL** : https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv

Les données sont mises à jour régulièrement et contiennent des indicateurs journaliers par pays.

## Architecture globale
Le projet repose sur une architecture en couches clairement séparées :
- **Sources de données**
- **Ingestion (batch & streaming)**
- **Processing (Spark)**
- **Stockage analytique (PostgreSQL)**
- **Orchestration & monitoring (Airflow)**
- **Exécution conteneurisée (Docker)**

Diagramme : ([`architecture_pipeline.png`](docs/diagrams/architecture_pipeline.png))

## Flux de données
Le pipeline implémente **deux flux complémentaires** :
### Flux Batch
1. Téléchargement du fichier CSV OWID  
2. Stockage des données brutes avec métadonnées (`ingestion_date`)  
3. Traitement batch via Apache Spark :
   - nettoyage,
   - profiling,
   - transformations,
   - partitionnement  
4. Écriture des données transformées en Parquet  
5. Chargement en base PostgreSQL (staging)  
6. Alimentation des tables dimensionnelles et factuelles  

### Flux Streaming (simulation)
1. Lecture des données Parquet issues du batch  
2. Simulation d’événements via un **Kafka Producer**  
3. Publication des événements dans un **topic Kafka**  
4. Consommation via **Spark Structured Streaming**  
5. Écriture dans la même table de staging PostgreSQL  
6. Alimentation des tables analytiques  

Diagramme : ([`flux_donnees.png`](docs/diagrams/flux_donnees.png))

## Architecture logique du pipeline
### Couche d’ingestion
#### Ingestion Batch
- **Outil :** Python
- **Opérations clés :**
   - Téléchargement du CSV OWID,
   - Stockage des données brutes,
   - Ajout de métadonnées d’ingestion.

#### Ingestion Streaming
- **Outil :** Apache Kafka
- **Rôle :** Kafka Producer
- **Opérations clés :**
  - lecture des données traitées,
  - émission des événements de vaccination,
  - simulation d'un flux quasi temps réel.

### Couche de traitement
#### Traitement Batch
- **Outil :** Apache Spark (PySpark)
- **Opérations clés :**
  - nettoyage des données,
  - profiling,
  - transformations métier,
  - partitionnement par pays (`iso_code`).

#### Traitement Streaming
- **Outil :** Spark Structured Streaming
- **Opérations clés :**
   - Consommation des messages Kafka,
   - Validation de schéma,
   - Traitement en micro-batch.

### Couche de stockage
#### PostgreSQL – Data Warehouse
Le stockage suit une **modélisation analytique** classique :
- Schéma staging (`stg_owid_covid`) : point d’entrée commun batch & streaming
- Tables de dimensions (`dim_date`, `dim_location`)
- Table de faits (`fact_vaccination`)

Diagramme : ([`schema_bdd.png`](docs/diagrams/schema_bdd.png))

### Orchestration & monitoring
- **Outil :** Apache Airflow
- Deux DAGs principaux :
  - **DAG Batch** : ingestion → processing → stockage
  - **DAG Streaming** : producer Kafka → consumer Spark → stockage

Airflow gère :
- les dépendances,
- la planification,
- les retries,
- les logs d’exécution.

Diagramme : ([`airflow_dags.png`](docs/diagrams/airflow_dags.png))

## Déploiement & environnement
L’ensemble du projet est **conteneurisé avec Docker & Docker Compose** afin de garantir :
- la reproductibilité,
- l’isolation des services,
- une exécution locale simplifiée.

**Services principaux :**
- Apache Airflow
- Apache Spark
- Apache Kafka + ZooKeeper
- PostgreSQL

Diagramme : ([`deploiement_docker.png`](docs/diagrams/deploiement_docker.png))

## Exécution rapide
Pour tester le pipeline OWID COVID-19 en local avec Docker Compose :

```bash
# Lancer tous les services (Airflow, Spark, Kafka, Zookeeper, PostgreSQL)
docker-compose up --build -d

# Lancer le DAG batch via Airflow UI ou CLI
# Exemple CLI :
docker exec -it <airflow_container> airflow dags trigger owid_batch_pipeline
```

## Structure du projet
```bash
├── airflow/                # DAGs et logs pour orchestrer le pipeline
│   ├── dags/
│   │   ├── owid_batch_pipeline.py
│   │   └── owid_streaming_pipeline.py
│   └── logs/
│
├── data/                   # Stockage des données
│   ├── raw/
│   ├── processed/
│   ├── profiling/
│   └── checkpoints/
│
├── src/                    # Code source
│   ├── ingestion/
│   ├── profiling/
│   ├── transformation/
│   ├── storage/
│   │   └── postgres/
│   │       ├── ddl/
│   │       ├── dml/
│   │       └── load_owid_postgres.py
│   └── streaming/
│       ├── producer/
│       ├── consumer/
│       └── postgres_writer.py
│
├── scripts/                # Scripts utilitaires
│   └── bash/
│
├── docker/                 # Dockerfiles pour conteneurs
│   └── Dockerfile.airflow  
├── docker-compose.yml      # Orchestration multi-conteneurs
│
├── docs/                   # Documentation
│
├── logs/
├── .gitignore
└── README.md
```

## Améliorations futures
- Optimisations de performance
- Gestion de l’évolution des schémas
- Historisation du staging
- Alerting sur anomalies

## Conclusion
Ce projet vise à démontrer :
- une **compréhension des fondamentaux du data engineering**,
- la capacité à **concevoir des pipelines robustes et scalables**,
- la **maîtrise d’outils** modernes de l’écosystème data.

## Auteur
Maï Louisgrand - Data engineer Junior 
([`LinkekIn`](https://www.linkedin.com/in/mai-louisgrand/))

## License
Code open source – usage personnel / à des fins de démonstration uniquement