-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Query whole tussock cover
-- Authors: Timm Nawrocki, Amanda Droghini, Alaska Center for Conservation Science
-- Last Updated:  2025-05-03
-- Usage: Script should be executed in a PostgreSQL 16+ database.
-- Description: "Query whole tussock cover" queries the whole tussock cover data.
-- ---------------------------------------------------------------------------

-- Compile whole tussock cover data
SELECT whole_tussock_cover.tussock_id as tussock_id
     , whole_tussock_cover.site_visit_code as site_visit_code
     , cover_type.cover_type as cover_type
     , whole_tussock_cover.cover_percent as cover_percent
FROM whole_tussock_cover
    LEFT JOIN cover_type ON whole_tussock_cover.cover_type_id = cover_type.cover_type_id
ORDER BY tussock_id;
