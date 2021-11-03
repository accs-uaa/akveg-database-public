-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Query Lichen Checklist
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated:  2021-01-29
-- Usage: Script should be executed in a PostgreSQL 12 database.
-- Description: "Query Lichen Checklist" queries the lichen and lichenicolous fungi taxa in a web checklist format.
-- ---------------------------------------------------------------------------

-- Compile lichen checklist
SELECT taxon_adjudicated.adjudicated_id as name_id
     , CONCAT('<i>', taxon_adjudicated.name_adjudicated, '</i> ', author_adjudicated.author) as name
     , taxon_status.taxon_status as status
     , CONCAT('<i>', taxon_accepted.name_accepted, '</i> ', author_accepted.author) as name_accepted
     , family.family as family
     , taxon_accepted.link_source as source
     , taxon_level.level as level
     , category.category as category
     , habit.habit as habit
FROM taxon_adjudicated
         LEFT JOIN author author_adjudicated ON taxon_adjudicated.auth_adjudicated_id = author_adjudicated.author_id
         LEFT JOIN taxon_accepted ON taxon_adjudicated.accepted_id = taxon_accepted.accepted_id
         LEFT JOIN author author_accepted ON taxon_accepted.auth_accepted_id = author_accepted.author_id
         LEFT JOIN taxon_status ON taxon_adjudicated.status_adjudicated_id = taxon_status.taxon_status_id
         LEFT JOIN taxon_level ON taxon_accepted.level_id = taxon_level.level_id
         LEFT JOIN hierarchy ON taxon_accepted.hierarchy_id = hierarchy.hierarchy_id
         LEFT JOIN family ON hierarchy.family_id = family.family_id
         LEFT JOIN category ON hierarchy.category_id = category.category_id
         LEFT JOIN habit ON taxon_accepted.habit_id = habit.habit_id
WHERE taxon_accepted.level_id != 6
  AND category.category = 'Lichen'
ORDER BY family.family, taxon_accepted.name_accepted, taxon_Status.taxon_status, taxon_adjudicated.name_adjudicated;