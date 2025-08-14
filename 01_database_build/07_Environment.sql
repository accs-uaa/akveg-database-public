-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Build environment tables
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated: 2024-11-19
-- Usage: Script should be executed in a PostgreSQL 14+ database.
-- Description: "Build environment tables" creates the empty tables for the environment components of the AKVEG database. WARNING: THIS SCRIPT WILL ERASE ALL DATA IN EXISTING ENVIRONMENT TABLES.
-- ---------------------------------------------------------------------------

-- Initialize transaction
START TRANSACTION;

-- Drop environment tables if they exist
DROP TABLE IF EXISTS
    environment,
    soil_metrics,
    soil_horizons;

-- Create environment table
CREATE TABLE environment (
    environment_id serial PRIMARY KEY,
    site_visit_code varchar(65) NOT NULL REFERENCES site_visit,
    physiography_id smallint REFERENCES physiography,
    geomorphology_id smallint REFERENCES geomorphology,
    macrotopography_id smallint REFERENCES macrotopography,
    microtopography_id smallint REFERENCES microtopography,
    microrelief_cm decimal(4,1) NOT NULL DEFAULT -999,
    drainage_id smallint REFERENCES drainage,
    moisture_id smallint REFERENCES moisture,
    depth_water_cm decimal(4,1) NOT NULL DEFAULT -999,
    depth_moss_duff_cm decimal(4,1) NOT NULL DEFAULT -999,
    depth_restrictive_layer_cm decimal(4,1) NOT NULL DEFAULT -999,
    restrictive_type_id smallint REFERENCES restrictive_type,
    disturbance_id smallint REFERENCES disturbance,
    disturbance_severity_id smallint REFERENCES disturbance_severity,
    disturbance_time_y smallint NOT NULL DEFAULT -999,
    surface_water boolean,
    soil_class_id smallint REFERENCES soil_class,
    cryoturbation boolean,
    dominant_texture_40_cm_code varchar(4) REFERENCES soil_texture,
    depth_15_percent_coarse_fragments_cm decimal(4,1) NOT NULL DEFAULT -999,
    UNIQUE(site_visit_code)
);

-- Create soil metrics table
CREATE TABLE soil_metrics (
    soil_metric_id serial PRIMARY KEY,
    site_visit_code varchar(65) NOT NULL REFERENCES site_visit,
    water_measurement boolean NOT NULL,
    measure_depth_cm decimal(4,1) NOT NULL DEFAULT -999,
    ph decimal(4,1) NOT NULL DEFAULT -999,
    conductivity_mus decimal(7,2) NOT NULL DEFAULT -999,
    temperature_deg_c decimal(4,1) NOT NULL DEFAULT -999,
    UNIQUE(site_visit_code, water_measurement, measure_depth_cm)
);

-- Create soil horizons table
CREATE TABLE soil_horizons (
    soil_horizon_id serial PRIMARY KEY,
    site_visit_code varchar(65) NOT NULL REFERENCES site_visit,
    horizon_order smallint NOT NULL DEFAULT -999,
    thickness_cm decimal(4,1) NOT NULL DEFAULT -999,
    depth_upper_cm decimal(4,1) NOT NULL DEFAULT -999,
    depth_lower_cm decimal(4,1) NOT NULL DEFAULT -999,
    depth_extend boolean NOT NULL,
    horizon_primary_code varchar(1) REFERENCES soil_horizon_type,
    horizon_suffix_1_code varchar(2) REFERENCES soil_horizon_suffix,
    horizon_suffix_2_code varchar(2) REFERENCES soil_horizon_suffix,
    horizon_secondary_code varchar(1) REFERENCES soil_horizon_type,
    horizon_suffix_3_code varchar(2) REFERENCES soil_horizon_suffix,
    horizon_suffix_4_code varchar(2) REFERENCES soil_horizon_suffix,
    texture_code varchar(4) REFERENCES soil_texture,
    clay_percent decimal(4,1) NOT NULL DEFAULT -999,
    total_coarse_fragment_percent decimal(4,1) NOT NULL DEFAULT -999,
    gravel_percent decimal(4,1) NOT NULL DEFAULT -999,
    cobble_percent decimal(4,1) NOT NULL DEFAULT -999,
    stone_percent decimal(4,1) NOT NULL DEFAULT -999,
    boulder_percent decimal(4,1) NOT NULL DEFAULT -999,
    structure_code varchar(3) REFERENCES soil_structure,
    matrix_hue_code varchar(5) REFERENCES soil_hue,
    matrix_value decimal(4,1) NOT NULL DEFAULT -999,
    matrix_chroma smallint NOT NULL DEFAULT -999,
    nonmatrix_feature_code varchar(2) REFERENCES soil_nonmatrix_features,
    nonmatrix_hue_code varchar(5) REFERENCES soil_hue,
    nonmatrix_value decimal(4,1) NOT NULL DEFAULT -999,
    nonmatrix_chroma smallint NOT NULL DEFAULT -999,
    UNIQUE(site_visit_code, horizon_order)
);

-- Commit transaction
COMMIT TRANSACTION;