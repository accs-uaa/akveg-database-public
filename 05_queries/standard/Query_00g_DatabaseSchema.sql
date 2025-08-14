-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Query database schema
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated:  2022-10-18
-- Usage: Script should be executed in a PostgreSQL 14+ database.
-- Description: "Query database schema" queries the database schema.
-- ---------------------------------------------------------------------------

-- Compile database schema
SELECT database_schema.field_id as field_id
     , database_schema.standards_section as standards_section
     , schema_category.schema_category as schema_category
     , native_table.schema_table as schema_table
     , database_schema.field as field
     , data_type.data_type as data_type
     , database_schema.field_length as field_length
     , database_schema.is_unique as is_unique
     , database_schema.is_key as is_key
     , database_schema.required as required
     , link_table.schema_table as link_table
     , database_schema.field_description as field_description
FROM database_schema
    LEFT JOIN schema_category ON database_schema.schema_category_id = schema_category.schema_category_id
    LEFT JOIN schema_table native_table ON database_schema.schema_table_id = native_table.schema_table_id
    LEFT JOIN data_type ON database_schema.data_type_id = data_type.data_type_id
    LEFT JOIN schema_table link_table ON database_schema.link_table_id = link_table.schema_table_id
ORDER BY field_id;
