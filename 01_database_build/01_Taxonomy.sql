-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Build taxonomy tables
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated: 2022-02-12
-- Usage: Script should be executed in a PostgreSQL 12 database.
-- Description: "Build taxonomy tables" creates the empty tables for the taxonomy components of the AKVEG database. WARNING: THIS SCRIPT WILL ERASE ALL DATA IN EXISTING TAXONOMY TABLES.
-- ---------------------------------------------------------------------------

-- Initialize transaction
START TRANSACTION;

-- Drop taxonomy tables if they exist
DROP TABLE IF EXISTS
    taxon_all,
    taxon_accepted,
    hierarchy,
    author,
    category,
    family,
    habit,
    taxon_status,
    taxon_level,
    taxon_source
CASCADE;

-- Create constraint tables
CREATE TABLE author (
    author_id integer PRIMARY KEY,
    author varchar(120) UNIQUE NOT NULL
);
CREATE TABLE category (
    category_id smallint PRIMARY KEY,
    category varchar(30) UNIQUE NOT NULL
);
CREATE TABLE family (
    family_id smallint PRIMARY KEY,
    family varchar(80) UNIQUE NOT NULL
);
CREATE TABLE habit (
    habit_id smallint PRIMARY KEY,
    habit varchar(80) UNIQUE NOT NULL
);
CREATE TABLE taxon_status (
    taxon_status_id smallint PRIMARY KEY,
    taxon_status varchar(30) UNIQUE NOT NULL
);
CREATE TABLE taxon_level (
    level_id smallint PRIMARY KEY,
    level varchar(30) UNIQUE NOT NULL
);
CREATE TABLE taxon_source (
    taxon_source_id smallint PRIMARY KEY,
    taxon_source varchar(50) UNIQUE NOT NULL,
    citation varchar(500) UNIQUE NOT NULL
);

-- Create hierarchy table
CREATE TABLE hierarchy (
    hierarchy_id smallint PRIMARY KEY,
    genus_accepted varchar(50) UNIQUE NOT NULL,
    family_id smallint NOT NULL REFERENCES family,
    category_id smallint NOT NULL REFERENCES category
);

-- Create accepted taxa table
CREATE TABLE taxon_accepted (
    code_accepted varchar(15) PRIMARY KEY,
    name_accepted varchar(120) UNIQUE NOT NULL,
    auth_accepted_id integer NOT NULL REFERENCES author,
    hierarchy_id smallint NOT NULL REFERENCES hierarchy,
    taxon_source_id smallint REFERENCES taxon_source,
    link_source varchar(255),
    level_id smallint NOT NULL REFERENCES taxon_level,
    habit_id smallint NOT NULL REFERENCES habit,
    native boolean NOT NULL,
    non_native boolean NOT NULL
);

-- Create adjudicated taxa table
CREATE TABLE taxon_all (
    code integer PRIMARY KEY,
    name varchar(120) NOT NULL,
    auth_id integer NOT NULL REFERENCES author,
    taxon_status_id smallint NOT NULL REFERENCES taxon_status,
    accepted_id integer NOT NULL REFERENCES taxon_accepted
);

-- Commit transaction
COMMIT TRANSACTION;
