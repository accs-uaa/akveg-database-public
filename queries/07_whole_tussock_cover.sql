-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Query whole tussock cover
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated: 2025-03-12
-- Usage: Script should be executed in a PostgreSQL 14+ database.
-- Description: "Query whole tussock cover" queries all whole tussock cover data from the AKVEG Database.
-- ---------------------------------------------------------------------------

-- Compile whole tussock cover data
SELECT whole_tussock_cover.site_visit_code as st_vst
     , cover_type.cover_type as cvr_type
     , whole_tussock_cover.cover_percent as cvr_pct
FROM whole_tussock_cover
    LEFT JOIN site_visit ON whole_tussock_cover.site_visit_code = site_visit.site_visit_code
    LEFT JOIN cover_type ON whole_tussock_cover.cover_type_id = cover_type.cover_type_id;
