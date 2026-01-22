-- ============================================================
-- Base de données : covid_dw
-- Objectif : Data warehouse analytique pour le suivi de la
--            vaccination COVID-19 à l’échelle mondiale
--
-- Schémas :
-- - staging : données temporaires issues des traitements Spark
-- - dim     : tables de dimensions (référentiels)
-- - fact    : tables de faits pour l’analyse
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS fact;

SET search_path TO staging, dim, fact;