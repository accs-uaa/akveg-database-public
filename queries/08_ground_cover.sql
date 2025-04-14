-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Query ground cover
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated: 2025-03-12
-- Usage: Script should be executed in a PostgreSQL 14+ database.
-- Description: "Query ground cover" queries the ground cover data.
-- ---------------------------------------------------------------------------

-- Compile ground cover data
SELECT ground_cover.site_visit_code as st_vst
     , ground_element.ground_element as ground_element
     , ground_cover.ground_cover_percent as cvr_pct
FROM ground_cover
    LEFT JOIN site_visit ON ground_cover.site_visit_code = site_visit.site_visit_code
    LEFT JOIN ground_element ON ground_cover.ground_element_code = ground_element.ground_element_code;
