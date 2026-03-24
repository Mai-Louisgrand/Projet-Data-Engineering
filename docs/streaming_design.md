# Design du Pipeline de streaming – Pipeline Vaccination COVID (OWID)

## Contexte et objectif du streaming
Le pipeline batch constitue la base principale pour traiter les données OWID COVID-19.  
Cependant, **afin de compléter cette approche et de simuler un traitement orienté événements, un pipeline de streaming a été ajouté**.

L’objectif du streaming n’est pas de remplacer le batch, mais de :
- simuler un flux de données **quasi temps réel** à partir de fichiers historiques,  
- réaliser une architecture **orientée événements** (event-driven),
- tester l’intégration d’un pipeline streaming dans une architecture Data Lake / Data Warehouse.

Le streaming s’appuie sur les données transformées stockées dans **GCS** et écrit dans une **table de staging BigQuery commune**, assurant la cohérence avec le pipeline batch.

## Architecture du pipeline de streaming
Le pipeline suit le flux logique suivant :  

1. Lecture des données transformées (Parquet) depuis **GCS (`processed/`)**
2. Transformation de chaque ligne en **événement métier** (un état de vaccination par pays et par date)
3. Publication des événements dans un **topic Kafka**
4. Consommation via **Spark Structured Streaming** (micro-batch)   
5. Écriture dans une **table de staging temporaire BigQuery**  

**Diagrammes :** 
- [`pipeline_architecture_cloud.png`](../docs/diagrams/pipeline_architecture_cloud.png)
- [`data_flow.png`](../docs/diagrams/data_flow.png)

## Détails des modules
### Modélisation des événements
Chaque événement représente un état de vaccination pour **un pays donné à une date donnée**.  

Cela permet de suivre précisément l’évolution par pays. 

### Simulation du streaming (Producer Kafka)
Le producer Kafka ([`owid_event_producer.py`](../src/streaming/producer/owid_event_producer.py)) simule un flux temps réel à partir de données historiques. 

Choix techniques :  
- lecture des fichiers Parquet depuis **GCS processed**, plutôt que de la source brute, afin de se concentrer sur la simulation du streaming plutôt que sur les transformations initiales ;
- filtrage des données par date pour simuler une journée de streaming ;
- émission des événements ligne par ligne ;
- sérialisation des messages en JSON avec un schéma explicite ;
- ajout d’un délai artificiel (sleep) pour reproduire un comportement proche du temps réel.

### Consommation Spark Structured Streaming
Le consumer est implémenté via Spark Structured Streaming ([`owid_stream_processing.py`](../src/streaming/consumer/owid_stream_processing.py)). Il lit les messages Kafka et les transforme pour correspondre au format batch.  

Choix techniques :  
- **Spark Structured Streaming** : pour gérer ingestion, transformation et écriture ;  
- Mode **micro-batch** : traite les événements par petits groupes pour limiter les risques de perte et faciliter le suivi ; 
- Schéma JSON explicite : chaque message est lu de manière sûre, pas de dépendance aux formats implicites ;
- Transformation vers le modèle batch : le streaming et le batch produisent exactement le même format de données pour l'ingestion dans la table de staging.  

### Écriture dans BigQuery (staging)
Les données sont écrites directement dans BigQuery via le connecteur Spark BigQuery.

Choix techniques :
- écriture directe via Spark vers BigQuery ;  
- écriture dans une **table de staging temporaire** commune batch et streaming pour assurer la convergence des deux pipelines ;
- tables finales (dim/fact) jamais alimentées directement par le streaming pour garantir la cohérence globale des données.

## Caractéristiques du pipeline
### Idempotence et gestion des doublons
Pour garantir la fiabilité des traitements en cas de rejouage ou de retry :
- déduplication au niveau des micro-batchs Spark lors de la consommation Kafka ;
- nettoyage des doublons dans la table de staging temporaire ;  
- logique d’**upsert** lors de l’intégration dans la staging principale puis dans les tables analytiques.

### Configuration et portabilité
Les paramètres sont centralisés dans :  
- ([`settings.py`](../src/config/settings.py)),
- ([`kafka_settings.py`](../src/config/kafka_settings.py)),
- ([`spark.py`](../src/utils/spark.py)).

## Limites et axes d’amélioration
- Le pipeline **simule un flux temps réel** : ce n’est pas un vrai flux événementiel produit en continu.  
- Le traitement reste limité par une exécution locale (Spark + Kafka).

## Synthèse
Le pipeline de streaming OWID permet :  
- de simuler une ingestion **quasi temps réel** dans un environnement cloud ;  
- de valider une architecture **event-driven** basée sur Kafka et Spark ;  
- d’assurer une **cohérence complète avec le pipeline batch** via une staging commune ;  
- de démontrer l’intégration d’un pipeline streaming dans une architecture  
  **Data Lake (GCS) → Data Warehouse (BigQuery)**.
