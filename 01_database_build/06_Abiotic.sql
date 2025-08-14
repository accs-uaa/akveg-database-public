-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Build abiotic tables
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated: 2024-11-19
-- Usage: Script should be executed in a PostgreSQL 14+ database.
-- Description: "Build abiotic tables" creates the empty tables for the abiotic components of the AKVEG database. WARNING: THIS SCRIPT WILL ERASE ALL DATA IN EXISTING ABIOTIC TABLES.
-- ---------------------------------------------------------------------------

-- Initialize transaction
START TRANSACTION;

-- Drop project tables if they exist
DROP TABLE IF EXISTS
    abiotic_top_cover,
    ground_cover;

-- Create abiotic top cover table
CREATE TABLE abiotic_top_cover (
    abiotic_cover_id serial PRIMARY KEY,
    site_visit_code varchar(65) NOT NULL REFERENCES site_visit,
    abiotic_element_code varchar(2) NOT NULL REFERENCES ground_element,
    abiotic_top_cover_percent decimal(6,3) NOT NULL,
    UNIQUE(site_visit_code, abiotic_element_code)
);

-- Create ground cover table
CREATE TABLE ground_cover (
    ground_cover_id serial PRIMARY KEY,
    site_visit_code varchar(65) NOT NULL REFERENCES site_visit,
    ground_element_code varchar(2) NOT NULL REFERENCES ground_element,
    ground_cover_percent decimal(6,3) NOT NULL,
    UNIQUE(site_visit_code, ground_element_code)
);

-- Commit transaction
COMMIT TRANSACTION;