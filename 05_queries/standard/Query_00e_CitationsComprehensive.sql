-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Query comprehensive citations
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated:  2022-10-18
-- Usage: Script should be executed in a PostgreSQL 14+ database.
-- Description: "Query comprehensive citations" queries all of the taxonomic citations except "None".
-- ---------------------------------------------------------------------------

-- Compile comprehensive checklist citations
SELECT taxon_source.taxon_source_id
     , taxon_source.taxon_source
     , taxon_source.taxon_citation as citation
FROM taxon_source
WHERE taxon_source.taxon_source != 'none'
ORDER BY taxon_citation;
