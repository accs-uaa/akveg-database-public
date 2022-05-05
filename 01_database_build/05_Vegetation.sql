-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Build vegetation tables
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated: 2021-01-27
-- Usage: Script should be executed in a PostgreSQL 12 database.
-- Description: "Build vegetation tables" creates the empty tables for the vegetation components of the AKVEG database. WARNING: THIS SCRIPT WILL ERASE ALL DATA IN EXISTING VEGETATION TABLES.
-- ---------------------------------------------------------------------------

-- Initialize transaction
START TRANSACTION;

-- Drop project tables if they exist
DROP TABLE IF EXISTS
    cover;

-- Create total cover table
CREATE TABLE cover (
    cover_id serial PRIMARY KEY,
    project_id smallint NOT NULL REFERENCES project,
    site_id integer NOT NULL REFERENCES site,
    veg_observe_date date NOT NULL,
    veg_observer_id smallint NOT NULL REFERENCES personnel,
    veg_recorder_id smallint REFERENCES personnel,
    cover_type_id smallint NOT NULL REFERENCES cover_type,
    name_original varchar(120) NOT NULL,
    code varchar(15) NOT NULL REFERENCES taxon_all,
    cover decimal(6,3) NOT NULL
);

-- Commit transaction
COMMIT TRANSACTION;