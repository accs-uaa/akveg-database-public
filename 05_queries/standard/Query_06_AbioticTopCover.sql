-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Query abiotic top cover
-- Authors: Timm Nawrocki, Amanda Droghini, Alaska Center for Conservation Science
-- Last Updated:  2025-05-03
-- Usage: Script should be executed in a PostgreSQL 16+ database.
-- Description: "Query abiotic top cover" queries the abiotic top cover data.
-- ---------------------------------------------------------------------------

-- Compile abiotic top cover data
SELECT abiotic_top_cover.abiotic_cover_id as abiotic_cover_id
     , abiotic_top_cover.site_visit_code as site_visit_code
     , ground_element.ground_element as abiotic_element
     , abiotic_top_cover.abiotic_top_cover_percent as abiotic_top_cover_percent
FROM abiotic_top_cover
    LEFT JOIN ground_element ON abiotic_top_cover.abiotic_element_code = ground_element.ground_element_code
ORDER BY abiotic_cover_id;
