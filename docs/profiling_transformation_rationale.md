# Justification des transformations - Pipeline Vaccination COVID (OWID)

## 1. Suppression des lignes non identifiées
- Lignes sans `iso_code`, `location` ou `date` supprimées.
- Lignes correspondant à des agrégats régionaux ou globaux (`OWID_*`) exclues pour ne conserver que les données pays.

## 2. Réduction du schéma
- Seules les colonnes pertinentes pour l’analyse de la vaccination sont conservées.
- Les données de cas COVID sont exclues afin de garder un schéma focalisé sur la problématique métier.

## 3. Normalisation et recalcul des indicateurs
- Les métriques normalisées (`*_per_hundred`) sont recalculées pour garantir la cohérence et réduire la dépendance aux transformations amont.

## 4. Gestion des valeurs nulles et aberrantes
- Les valeurs nulles sur les métriques de vaccination sont conservées, car elles signalent des retards de reporting ou des données manquantes.
- Les valeurs maximales élevées et ratios >100% sont conservés : elles reflètent des particularités démographiques.
- Aucune valeur aberrante bloquante n’a été détectée lors du profiling.
