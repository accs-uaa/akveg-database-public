-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Query structural group cover
-- Authors: Timm Nawrocki, Amanda Droghini, Alaska Center for Conservation Science
-- Last Updated:  2025-05-03
-- Usage: Script should be executed in a PostgreSQL 16+ database.
-- Description: "Query structural group cover" queries the structural group cover data.
-- ---------------------------------------------------------------------------

-- Compile structural group cover data
SELECT structural_group_cover.structural_cover_id as structural_cover_id
     , structural_group_cover.site_visit_code as site_visit_code
     , cover_type.cover_type as cover_type
     , structural_group.structural_group as structural_group
     , structural_group_cover.cover_percent as cover_percent
FROM structural_group_cover
    LEFT JOIN cover_type ON structural_group_cover.cover_type_id = cover_type.cover_type_id
    LEFT JOIN structural_group ON structural_group_cover.structural_group_id = structural_group.structural_group_id
ORDER BY structural_cover_id;
