-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Query vegetation cover
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated:  2022-10-18
-- Usage: Script should be executed in a PostgreSQL 14+ database.
-- Description: "Query vegetation cover" queries the vegetation cover data with standardized taxonomic concepts.
-- ---------------------------------------------------------------------------

-- Compile vegetation cover data
SELECT vegetation_cover.vegetation_cover_id as vegetation_cover_id
     , vegetation_cover.site_visit_code as site_visit_code
     , cover_type.cover_type as cover_type
     , taxon_accepted.taxon_name as name_accepted
     , vegetation_cover.dead_status as dead_status
     , vegetation_cover.cover_percent as cover_percent
FROM vegetation_cover
    LEFT JOIN cover_type ON vegetation_cover.cover_type_id = cover_type.cover_type_id
    LEFT JOIN taxon_all taxon_adjudicated ON vegetation_cover.code_adjudicated = taxon_adjudicated.taxon_code
    LEFT JOIN taxon_all taxon_accepted ON taxon_adjudicated.taxon_accepted_code = taxon_accepted.taxon_code
ORDER BY vegetation_cover_id;
