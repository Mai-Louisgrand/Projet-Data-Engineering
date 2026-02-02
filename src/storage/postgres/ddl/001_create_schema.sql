-- ============================================================
-- Database: covid_dw
-- Purpose : Analytical data warehouse for global COVID-19 vaccination tracking
--
-- Schemas:
--   staging : temporary data loaded from Spark jobs
--   dim     : dimension/reference tables
--   fact    : fact tables for analytical queries
-- ============================================================

-- Create schemas if they do not exist
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS fact;

-- Set the default search path to include all relevant schemas
SET search_path TO staging, dim, fact;