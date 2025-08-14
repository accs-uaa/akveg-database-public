-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Query database dictionary
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated:  2022-10-18
-- Usage: Script should be executed in a PostgreSQL 14+ database.
-- Description: "Query database dictionary" queries the database dictionary.
-- ---------------------------------------------------------------------------

-- Compile database dictionary
SELECT database_dictionary.dictionary_id as dictionary_id
     , database_schema.field as field
     , database_dictionary.data_attribute_id as data_attribute_id
     , database_dictionary.data_attribute as data_attribute
     , database_dictionary.definition as definition
FROM database_dictionary
    LEFT JOIN database_schema ON database_dictionary.field_id = database_schema.field_id
ORDER BY dictionary_id;
