-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Build project tables
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated: 2022-10-18
-- Usage: Script should be executed in a PostgreSQL 14+ database.
-- Description: "Build project tables" creates the empty tables for the vegetation survey and monitoring projects components of the AKVEG database. WARNING: THIS SCRIPT WILL ERASE ALL DATA IN EXISTING PROJECT TABLES.
-- ---------------------------------------------------------------------------

-- Initialize transaction
START TRANSACTION;

-- Drop project tables if they exist
DROP TABLE IF EXISTS
    project,
    project_status
CASCADE;

-- Create projects table
CREATE TABLE project (
    project_code varchar(30) PRIMARY KEY,
    project_name varchar(250) UNIQUE NOT NULL,
    originator_id smallint NOT NULL REFERENCES organization,
    funder_id smallint NOT NULL REFERENCES organization,
    manager_id smallint NOT NULL REFERENCES personnel,
    completion_id smallint NOT NULL REFERENCES completion,
    year_start smallint NOT NULL,
    year_end smallint,
    project_description varchar(500) NOT NULL,
    private boolean NOT NULL
);

-- Create project status table
CREATE TABLE project_status (
    project_status_id smallint PRIMARY KEY,
    project_code varchar(30) NOT NULL REFERENCES project,
    project_modified date NOT NULL,
    site_modified date,
    vegetation_modified date,
    abiotic_modified date,
    environment_modified date,
    modified_by_id smallint NOT NULL
);

-- Commit transaction
COMMIT TRANSACTION;