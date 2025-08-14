-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Query Vascular Checklist
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated:  2022-09-30
-- Usage: Script should be executed in a PostgreSQL 14 database.
-- Description: "Query Vascular Checklist" queries the vascular plant taxa in an excel spreadsheet format.
-- ---------------------------------------------------------------------------

-- Compile vascular plant checklist
SELECT taxon_all.taxon_code as code
     , taxon_all.taxon_name as name
     , author_all.taxon_author as author
     , taxon_status.taxon_status as status
     , taxon_accepted_name.taxon_name as name_accepted
     , author_accepted.taxon_author as author_accepted
     , taxon_genus_name.taxon_name as genus
     , taxon_family.taxon_family as family
     , taxon_source.taxon_source as source
     , taxon_level.taxon_level as level
     , taxon_category.taxon_category as category
     , taxon_habit.taxon_habit as habit
     , taxon_accepted.taxon_native as native
     , taxon_accepted.taxon_non_native as non_native
FROM taxon_all
    LEFT JOIN taxon_author author_all ON taxon_all.taxon_author_id = author_all.taxon_author_id
    LEFT JOIN taxon_accepted ON taxon_all.taxon_accepted_code = taxon_accepted.taxon_accepted_code
    LEFT JOIN taxon_all taxon_accepted_name ON taxon_all.taxon_accepted_code = taxon_accepted_name.taxon_code
    LEFT JOIN taxon_author author_accepted ON taxon_accepted_name.taxon_author_id = author_accepted.taxon_author_id
    LEFT JOIN taxon_status ON taxon_all.taxon_status_id = taxon_status.taxon_status_id
    LEFT JOIN taxon_level ON taxon_accepted.taxon_level_id = taxon_level.taxon_level_id
    LEFT JOIN taxon_hierarchy ON taxon_accepted.taxon_genus_code = taxon_hierarchy.taxon_genus_code
    LEFT JOIN taxon_all taxon_genus_name ON taxon_accepted.taxon_genus_code = taxon_genus_name.taxon_code
    LEFT JOIN taxon_family ON taxon_hierarchy.taxon_family_id = taxon_family.taxon_family_id
    LEFT JOIN taxon_category ON taxon_hierarchy.taxon_category_id = taxon_category.taxon_category_id
    LEFT JOIN taxon_habit ON taxon_accepted.taxon_habit_id = taxon_habit.taxon_habit_id
    LEFT JOIN taxon_source on taxon_accepted.taxon_source_id = taxon_source.taxon_source_id
WHERE taxon_level.taxon_level IN ('genus', 'hybrid', 'species', 'subspecies', 'variety')
  AND taxon_category.taxon_category IN ('eudicot', 'fern', 'gymnosperm', 'horsetail', 'lycophyte', 'monocot')
ORDER BY taxon_family.taxon_family, taxon_accepted_name.taxon_name, taxon_status.taxon_status, taxon_all.taxon_name;
