# Justification des transformations - Pipeline Vaccination COVID (OWID)

## Contexte et objectif du profiling
Le fichier source OWID contient un large spectre de données COVID
(cas, décès, tests, vaccination, agrégats régionaux).

Un profiling exploratoire a été mené en amont des traitements Spark
afin de :
- **identifier les anomalies bloquantes** pour un traitement batch et streaming,
- **réduire le schéma** aux données strictement nécessaires au cas d’usage vaccination,
- **garantir la cohérence des indicateurs calculés** dans Spark.

## Méthodologie de profiling
### 1. Suppression des lignes non identifiées
- Les lignes sans `iso_code`, `location` ou `date` sont supprimées :
ces champs constituent la clé logique du grain (pays / date).
- Lignes correspondant à des agrégats régionaux ou globaux (`OWID_*`) exclues pour ne conserver que les données pays.

### 2. Réduction du schéma
- Seules les colonnes pertinentes pour l’analyse de la vaccination sont conservées.
- Les données de cas COVID sont exclues afin de garder un schéma focalisé sur la problématique métier.

### 3. Normalisation et recalcul des indicateurs
- Les métriques normalisées (`*_per_hundred`) sont recalculées pour garantir la cohérence et réduire la dépendance aux transformations amont.

### 4. Gestion des valeurs nulles et aberrantes
- Les valeurs nulles sur les métriques de vaccination sont conservées, car elles signalent des retards de reporting ou des données manquantes.
- Les valeurs maximales élevées et ratios >100% sont conservés : elles reflètent des particularités démographiques.
- Aucune valeur aberrante bloquante n’a été détectée lors du profiling.

## Synthèse du profiling
Le profiling n’a révélé aucune anomalie bloquante pour un traitement
batch ou streaming.

Les transformations appliquées visent principalement :
- la fiabilité du grain pays / date,
- la maîtrise du schéma et des indicateurs métier,
- la reproductibilité des calculs dans Spark, indépendamment des sources amont.