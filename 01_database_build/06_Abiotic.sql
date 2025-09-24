-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Build abiotic tables
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated: 2025-09-24
-- Usage: Script should be executed in a PostgreSQL 17+ database.
-- Description: "Build abiotic tables" creates the empty tables for the abiotic components of the AKVEG database. WARNING: THIS SCRIPT WILL ERASE ALL DATA IN EXISTING ABIOTIC TABLES.
-- ---------------------------------------------------------------------------

-- Initialize transaction
START TRANSACTION;

-- Drop project tables if they exist
DROP TABLE IF EXISTS
    abiotic_top_cover,
    ground_cover;

-- Create function for constraining abiotic element type
CREATE OR REPLACE FUNCTION check_abiotic_element(p_code varchar)
RETURNS boolean AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1 FROM ground_element
        WHERE ground_element_code = p_code
          AND element_type IN ('abiotic', 'both')
    );
END;
$$ LANGUAGE plpgsql;

-- Create abiotic top cover table
CREATE TABLE abiotic_top_cover (
    abiotic_cover_id serial PRIMARY KEY,
    site_visit_code varchar(65) NOT NULL REFERENCES site_visit,
    abiotic_element_code varchar(2) NOT NULL,
    abiotic_top_cover_percent decimal(6,3) NOT NULL CONSTRAINT abiotic_percent_range CHECK (abiotic_top_cover_percent BETWEEN 0 AND 100),
    UNIQUE(site_visit_code, abiotic_element_code),
    CONSTRAINT fk_abiotic_element_code FOREIGN KEY (abiotic_element_code) REFERENCES ground_element(ground_element_code),
    CONSTRAINT check_abiotic_element_type CHECK (check_abiotic_element(abiotic_element_code))
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