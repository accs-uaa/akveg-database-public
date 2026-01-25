-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Define check constraints
-- Author: Amanda Droghini, Alaska Center for Conservation Science
-- Last Updated: 2026-01-24
-- Usage: Script should be executed in a PostgreSQL 17+ database.
-- Description: "Define check constraints" alters existing tables by adding check constraints for technical validation. This script must be run after the INSERT scripts to function properly. The check_abiotic_element function is defined in the build script that creates the abiotic top cover table.
-- ---------------------------------------------------------------------------

-- Initialize transaction
START TRANSACTION;

-- Add constraint to abiotic top cover table and validate records
ALTER TABLE abiotic_top_cover
    ADD CONSTRAINT check_abiotic_element_type
    CHECK (check_abiotic_element(abiotic_element_code));

-- Remove constraint to avoid issues with export file
ALTER TABLE abiotic_top_cover
    DROP CONSTRAINT check_abiotic_element_type;

-- Commit transaction
COMMIT TRANSACTION;
