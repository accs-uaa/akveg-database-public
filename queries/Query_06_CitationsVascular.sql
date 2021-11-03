-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Query Vascular Plant Citations
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated:  2021-01-29
-- Usage: Script should be executed in a PostgreSQL 12 database.
-- Description: "Query Vascular Plant Citations" queries the vascular plant citations except "None".
-- ---------------------------------------------------------------------------

-- Compile vascular plant checklist citations
SELECT DISTINCT taxon_accepted.taxon_source_id
    , taxon_source.taxon_source
    , taxon_source.citation
FROM taxon_accepted
    LEFT JOIN taxon_source ON taxon_accepted.taxon_source_id = taxon_source.taxon_source_id
    LEFT JOIN hierarchy ON taxon_accepted.hierarchy_id = hierarchy.hierarchy_id
    LEFT JOIN category ON hierarchy.category_id = category.category_id
WHERE taxon_source.taxon_source != 'None'
  AND category.category IN ('Eudicot', 'Fern', 'Gymnosperm', 'Horsetail', 'Lycophyte', 'Monocot')
ORDER BY citation;