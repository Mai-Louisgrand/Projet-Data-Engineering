# Design du Pipeline de streaming – Pipeline Vaccination COVID (OWID)

## Contexte et objectif du streaming
Le pipeline batch constitue la base principale pour traiter les données OWID COVID-19.  
Cependant, **afin de compléter cette approche et de simuler un traitement orienté événements, un pipeline de streaming a été ajouté**.

L’objectif du streaming n’est pas de remplacer le batch, mais de :
- simuler un flux de données **quasi temps réel** à partir de fichiers historiques,  
- réaliser une architecture **orientée événements** (event-driven).

Le streaming utilise les mêmes données transformées que le batch et écrit dans une **table de staging commune**, ce qui permet de gérer les deux types de traitements ensemble sans conflits.

## Architecture du pipeline de streaming
Le pipeline suit le flux logique suivant :  

1. Lire les données OWID déjà transformées et stockées au format Parquet, à une date choisie.  
2. Transformer chaque ligne en **événement métier** (un état de vaccination par pays et par date).  
3. Publier ces événements dans un **topic Kafka**,
4. Consommer les événements via **Spark Structured Streaming**, qui traite les données par petits micro-batchs.  
5. Écrire les résultats dans une **table de staging PostgreSQL** partagée avec le batch, de façon idempotente.

**Illustration :** `architecture_pipeline.png` et `flux_donnees.png`.

## Détails des modules
### Modélisation des événements
Chaque événement représente un état de vaccination pour **un pays donné à une date donnée**.  

Cela permet de suivre précisément l’évolution par pays. 

### Simulation du streaming (Producer Kafka)
Le producer Kafka ([`owid_event_producer.py`](../src/streaming/producer/owid_event_producer.py)) simule un flux temps réel à partir de données historiques. 

Choix techniques :  
- lecture des fichiers Parquet déjà transformés, plutôt que de la source brute, afin de se concentrer sur la simulation du streaming plutôt que sur les transformations initiales ;
- filtrage des données par date pour simuler une journée de streaming ;
- émission des événements ligne par ligne ;
- les messages Kafka sont sérialisés en JSON avec un schéma stable et explicite ;
- ajout d’un délai artificiel (sleep) pour reproduire un comportement proche du temps réel.

### Consommation Spark Structured Streaming
Le consumer est implémenté via Spark Structured Streaming ([`owid_stream_processing.py`](../src/streaming/consumer/owid_stream_processing.py)). Il lit les messages Kafka et les transforme pour correspondre au format batch.  

Choix techniques :  
- **Spark Structured Streaming** : facilite la lecture des messages Kafka, la transformation des données et l’écriture dans PostgreSQL ; 
- Mode **micro-batch** : traite les événements par petits groupes pour limiter les risques de perte et faciliter le suivi ; 
- Schéma JSON explicite : chaque message est lu de manière sûre, pas de dépendance aux formats implicites ;
- Transformation vers le modèle batch : le streaming et le batch produisent exactement le même format de données pour l'ingestion dans la table de staging.  

### Écriture PostgreSQL (staging partagé)
Le module ([`postgres_writer.py`](../src/streaming/postgres_writer.py)) gère l'écriture des données de streaming dans PostgreSQL.

Choix techniques :
- Ecriture via JDBC depuis Spark, pour une intégration simple et robuste ;
- Ecriture dans une **table de staging commune batch/streaming** ;
- Les tables finales (dim/fact) ne sont jamais alimentées directement par le streaming pour garantir la cohérence globale des données.

Ce dernier point est détaillé plus largement dans `storage_design.md`.

## Caractéristiques du pipeline
### Déduplication et idempotence
Pour éviter les doublons ou erreurs lors de rejouages ou de retries :  
- à la production / lecture des messages Kafka : déduplication au niveau des micro-batchs Spark ;
- lors du stockage : contrainte UNIQUE côté PostgreSQL ;
- à l'ingestion : logique d’UPSERT via une table temporaire.

### Configuration et portabilité
Les paramètres sont centralisés dans :  
- ([`kafka_config.py`](../src/streaming/config/kafka_config.py)),
- ([`spark_config.py`](../src/streaming/config/spark_config.py)),
- ([`postgres_config.py`](../src/streaming/config/postgres_config.py)).

Ils sont externalisés via **variables d’environnement**, ce qui rend le pipeline compatible avec une exécution locale, Docker ou Airflow.

## Limites et axes d’amélioration
- Le pipeline **simule un flux temps réel** : ce n’est pas un vrai flux événementiel produit en continu.  
- La configuration locale limite le nombre de messages Kafka pouvant être traités simultanément.

## Synthèse
Le pipeline de streaming OWID permet :  
- une ingestion **robuste** et contrôlée,  
- une cohérence totale entre batch et streaming,  
- une **reproductibilité** complète des traitements,  
- une orchestration propre et supervisée via Airflow.
