-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Query vascular plant citations
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated:  2022-10-18
-- Usage: Script should be executed in a PostgreSQL 14+ database.
-- Description: "Query vascular plant citations" queries the vascular plant citations except "None".
-- ---------------------------------------------------------------------------

-- Compile vascular plant checklist citations
SELECT DISTINCT taxon_accepted.taxon_source_id
    , taxon_source.taxon_source
    , taxon_source.taxon_citation as citation
FROM taxon_accepted
    LEFT JOIN taxon_source ON taxon_accepted.taxon_source_id = taxon_source.taxon_source_id
    LEFT JOIN taxon_hierarchy ON taxon_accepted.taxon_genus_code = taxon_hierarchy.taxon_genus_code
    LEFT JOIN taxon_category ON taxon_hierarchy.taxon_category_id = taxon_category.taxon_category_id
WHERE taxon_source.taxon_source != 'none'
  AND taxon_category.taxon_category IN ('eudicot', 'fern', 'gymnosperm', 'horsetail', 'lycophyte', 'monocot')
ORDER BY taxon_citation;
