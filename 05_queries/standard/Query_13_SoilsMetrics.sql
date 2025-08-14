-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Query soil metrics
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated:  2023-04-04
-- Usage: Script should be executed in a PostgreSQL 14+ database.
-- Description: "Query soil metrics" queries the soil pH, conductivity, and temperature data.
-- ---------------------------------------------------------------------------

-- Compile soils data
SELECT soil_metrics.soil_metric_id as soil_metric_id
     , soil_metrics.site_visit_code as site_visit_code
     , soil_metrics.water_measurement as water_measurement
     , soil_metrics.measure_depth_cm as measure_depth_cm
     , soil_metrics.ph as ph
     , soil_metrics.conductivity_mus as conductivity_mus
     , soil_metrics.temperature_deg_c as temperature_deg_c
FROM soil_metrics
ORDER BY soil_metric_id;
