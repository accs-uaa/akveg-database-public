# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Prepare taxonomic data for upload
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2021-01-27
# Usage: Script should be executed in R 4.0.0+.
# Description: "Prepare taxonomic data for upload" merges taxonomy tables for vascular plants, bryophytes, and lichens; and parses data into a SQL query for upload into empty tables.
# ---------------------------------------------------------------------------

# Set root directory
drive = 'N:'
root_folder = 'ACCS_Work'

# Define input folders
data_folder = paste(drive,
                    root_folder,
                    'Projects/VegetationEcology/AKVEG_PlotsDatabase/Data/Tables_Taxonomy',
                    sep = '/')

# Designate output sql file
sql_taxonomy = 'C:/Users/timmn/Documents/Repositories/vegetation-plots-database/02_data_insertion/01_b_Insert_Taxonomy.sql'

# Identify taxonomy tables
vascular_file = paste(data_folder,
                      'vascularPlants.xlsx',
                      sep='/')
bryophyte_file = paste(data_folder,
                       'bryophytes.xlsx',
                       sep='/')
lichen_file = paste(data_folder,
                    'lichens.xlsx',
                    sep='/')
unknown_file = paste(data_folder,
                     'unknowns.xlsx',
                     sep = '/')
citation_file = paste(data_folder,
                      'citations.xlsx',
                      sep='/')

# Import required libraries
library(dplyr)
library(readr)
library(readxl)
library(stringr)
library(tibble)
library(tidyr)

# Read taxonomy tables into dataframes
vascular_data = read_excel(vascular_file, sheet = 'taxonomy')
bryophyte_data = read_excel(bryophyte_file, sheet = 'taxonomy')
lichen_data = read_excel(lichen_file, sheet = 'taxonomy')
unknown_data = read_excel(unknown_file, sheet = 'taxonomy')
citation_data = read_excel(citation_file, sheet = 'citations')

# Merge taxonomy tables to single dataframe
taxonomy_data = bind_rows(vascular_data, bryophyte_data, lichen_data, unknown_data)

# Export merged taxonomy table
csv_taxonomy = paste(data_folder, 'csv', 'taxonomy.csv', sep = '/')
write.csv(taxonomy_data, file = csv_taxonomy, fileEncoding = 'UTF-8', row.names = FALSE)

# Parse author names table
author_table = taxonomy_data %>%
  distinct(auth_adjudicated) %>%
  arrange(auth_adjudicated) %>%
  rename(author = auth_adjudicated) %>%
  rowid_to_column('author_id')

# Export author table
csv_author = paste(data_folder, 'csv', 'author.csv', sep = '/')
write.csv(author_table, file = csv_author, fileEncoding = 'UTF-8', row.names = FALSE)

# Parse category table
category_table = taxonomy_data %>%
  distinct(category) %>%
  arrange(category) %>%
  rowid_to_column('category_id')

# Export category table
csv_category = paste(data_folder, 'csv', 'category.csv', sep = '/')
write.csv(category_table, file = csv_category, fileEncoding = 'UTF-8', row.names = FALSE)

# Parse family table
family_table = taxonomy_data %>%
  distinct(family) %>%
  arrange(family) %>%
  rowid_to_column('family_id')

# Export family table
csv_family = paste(data_folder, 'csv', 'family.csv', sep = '/')
write.csv(family_table, file = csv_family, fileEncoding = 'UTF-8', row.names = FALSE)

# Parse growth habit table
habit_table = taxonomy_data %>%
  distinct(habit) %>%
  arrange(habit) %>%
  rowid_to_column('habit_id')

# Export growth habit table
csv_habit = paste(data_folder, 'csv', 'habit.csv', sep = '/')
write.csv(habit_table, file = csv_habit, fileEncoding = 'UTF-8', row.names = FALSE)

# Parse taxonomic status table
status_table = taxonomy_data %>%
  distinct(status_adjudicated) %>%
  arrange(status_adjudicated) %>%
  rename(taxon_status = status_adjudicated) %>%
  rowid_to_column('taxon_status_id')

# Export taxonomic status table
csv_status = paste(data_folder, 'csv', 'taxon_status.csv', sep = '/')
write.csv(status_table, file = csv_status, fileEncoding = 'UTF-8', row.names = FALSE)

# Parse taxonomic level table
level_table = taxonomy_data %>%
  distinct(level) %>%
  arrange(level) %>%
  rowid_to_column('level_id')

# Export taxonomic level table
csv_level = paste(data_folder, 'csv', 'taxon_level.csv', sep = '/')
write.csv(level_table, file = csv_level, fileEncoding = 'UTF-8', row.names = FALSE)

# Parse taxonomic source table
source_table = taxonomy_data %>%
  distinct(taxon_source) %>%
  arrange(taxon_source) %>%
  left_join(citation_data, by = 'taxon_source') %>%
  rowid_to_column('taxon_source_id')

# Export taxonomic source table
csv_source = paste(data_folder, 'csv', 'taxon_source.csv', sep = '/')
write.csv(source_table, file = csv_source, fileEncoding = 'UTF-8', row.names = FALSE)

# Parse hierarchy table
hierarchy_table = taxonomy_data %>%
  filter(level == 'genus' |
           level == 'unknown') %>%
  filter(status_adjudicated == 'accepted' |
           status_adjudicated == 'historic' |
           status_adjudicated == 'taxonomy unresolved' |
           status_adjudicated == 'location unresolved') %>%
  mutate(name_accepted = case_when(level == 'unknown' ~ 'Unknown',
                                 TRUE ~ name_accepted)) %>%
  distinct(name_accepted, .keep_all = TRUE) %>%
  left_join(family_table, by = 'family') %>%
  left_join(category_table, by = 'category') %>%
  rename(genus_accepted = name_accepted) %>%
  arrange(genus_accepted) %>%
  rowid_to_column('hierarchy_id') %>%
  select(hierarchy_id, genus_accepted, family_id, category_id)

# Export hierarchy table
csv_hierarchy = paste(data_folder, 'csv', 'hierarchy.csv', sep = '/')
write.csv(hierarchy_table, file = csv_hierarchy, fileEncoding = 'UTF-8', row.names = FALSE)

# Parse taxon accepted table
accepted_table = taxonomy_data %>%
  filter(status_adjudicated == 'accepted' |
           status_adjudicated == 'historic' |
           status_adjudicated == 'taxonomy unresolved' |
           status_adjudicated == 'location unresolved') %>%
  left_join(author_table, by = c('auth_accepted' = 'author')) %>%
  separate(name_accepted, c('genus_accepted'), extra = 'drop', sep = '([ ])', remove = FALSE) %>%
  mutate(genus_accepted = case_when(level == 'unknown' ~ 'Unknown',
                                   TRUE ~ genus_accepted)) %>%
  left_join(hierarchy_table, by = 'genus_accepted') %>%
  left_join(source_table, by = 'taxon_source') %>%
  left_join(level_table, by = 'level') %>%
  left_join(habit_table, by = 'habit') %>%
  arrange(name_accepted) %>%
  rowid_to_column('accepted_id') %>%
  rename(auth_accepted_id = author_id) %>%
  select(accepted_id, name_accepted, auth_accepted_id, hierarchy_id, taxon_source_id, link_source, level_id, habit_id, native, non_native)

# Export taxon accepted table
csv_accepted = paste(data_folder, 'csv', 'taxon_accepted.csv', sep = '/')
write.csv(accepted_table, file = csv_accepted, fileEncoding = 'UTF-8', row.names = FALSE)

# Parse taxon adjudicated table
adjudicated_table = taxonomy_data %>%
  rename(adjudicated_id = id) %>%
  left_join(author_table, by = c('auth_adjudicated' = 'author')) %>%
  left_join(status_table, by = c('status_adjudicated' = 'taxon_status')) %>%
  left_join(accepted_table, by = 'name_accepted') %>%
  rename(auth_adjudicated_id = author_id) %>%
  rename(status_adjudicated_id = taxon_status_id) %>%
  arrange(name_adjudicated) %>%
  select(adjudicated_id, name_adjudicated, auth_adjudicated_id, status_adjudicated_id, accepted_id)

# Export taxon adjudicated table
csv_adjudicated = paste(data_folder, 'csv', 'taxon_adjudicated.csv', sep = '/')
write.csv(adjudicated_table, file = csv_adjudicated, fileEncoding = 'UTF-8', row.names = FALSE)

#### WRITE DATA TO SQL FILE

# Write statement header
statement = c(
  '-- -*- coding: utf-8 -*-',
  '-- ---------------------------------------------------------------------------',
  '-- Insert taxonomy data',
  '-- Author: Timm Nawrocki, Alaska Center for Conservation Science',
  paste('-- Last Updated: ', Sys.Date()),
  '-- Usage: Script should be executed in a PostgreSQL 12 database.',
  '-- Description: "Insert taxonomy data" pushes data from the taxonomy editing tables into the database server. The script "Build taxonomy tables" should be run prior to this script to start with empty, properly formatted tables.',
  '-- ---------------------------------------------------------------------------',
  '',
  '-- Initialize transaction',
  'START TRANSACTION;',
  ''
  )

# Add author statement
statement = c(statement,
              '-- Insert data into author table',
              'INSERT INTO author (author_id, author) VALUES'
              )
author_sql = author_table %>%
  mutate_if(is.character,
            str_replace_all, pattern = '\'', replacement = '\'\'') %>%
  mutate(author = paste('\'', author, '\'', sep = '')) %>%
  unite(sql, sep = ', ', remove = TRUE) %>%
  mutate(sql = paste('(', sql, '),', sep =''))
author_sql[nrow(author_sql),] = paste(str_sub(author_sql[nrow(author_sql),],
                                              start = 1,
                                              end = -2),
                                      ';',
                                      sep = '')
for (line in author_sql) {
  statement = c(statement, line)
}

# Add category statement
statement = c(statement,
              '',
              '-- Insert data into category table',
              'INSERT INTO category (category_id, category) VALUES'
              )
category_sql = category_table %>%
  mutate(category = paste('\'', category, '\'', sep = '')) %>%
  unite(sql, sep = ', ', remove = TRUE) %>%
  mutate(sql = paste('(', sql, '),', sep = ''))
category_sql[nrow(category_sql),] = paste(str_sub(category_sql[nrow(category_sql),],
                                              start = 1,
                                              end = -2),
                                          ';',
                                          sep = '')
for (line in category_sql) {
  statement = c(statement, line)
}

# Add family statement
statement = c(statement,
              '',
              '-- Insert data into family table',
              'INSERT INTO family (family_id, family) VALUES'
              )
family_sql = family_table %>%
  mutate(family = paste('\'', family, '\'', sep = '')) %>%
  unite(sql, sep = ', ', remove = TRUE) %>%
  mutate(sql = paste('(', sql, '),', sep = ''))
family_sql[nrow(family_sql),] = paste(str_sub(family_sql[nrow(family_sql),],
                                                  start = 1,
                                                  end = -2),
                                      ';',
                                      sep = '')
for (line in family_sql) {
  statement = c(statement, line)
}

# Add habit statement
statement = c(statement,
              '',
              '-- Insert data into habit table',
              'INSERT INTO habit (habit_id, habit) VALUES'
)
habit_sql = habit_table %>%
  mutate(habit = paste('\'', habit, '\'', sep = '')) %>%
  unite(sql, sep = ', ', remove = TRUE) %>%
  mutate(sql = paste('(', sql, '),', sep = ''))
habit_sql[nrow(habit_sql),] = paste(str_sub(habit_sql[nrow(habit_sql),],
                                              start = 1,
                                              end = -2),
                                      ';',
                                      sep = '')
for (line in habit_sql) {
  statement = c(statement, line)
}

# Add taxon status statement
statement = c(statement,
              '',
              '-- Insert data into taxon status table',
              'INSERT INTO taxon_status (taxon_status_id, taxon_status) VALUES'
)
status_sql = status_table %>%
  mutate(taxon_status = paste('\'', taxon_status, '\'', sep = '')) %>%
  unite(sql, sep = ', ', remove = TRUE) %>%
  mutate(sql = paste('(', sql, '),', sep = ''))
status_sql[nrow(status_sql),] = paste(str_sub(status_sql[nrow(status_sql),],
                                            start = 1,
                                            end = -2),
                                    ';',
                                    sep = '')
for (line in status_sql) {
  statement = c(statement, line)
}

# Add taxon level statement
statement = c(statement,
              '',
              '-- Insert data into taxon level table',
              'INSERT INTO taxon_level (level_id, level) VALUES'
)
level_sql = level_table %>%
  mutate(level = paste('\'', level, '\'', sep = '')) %>%
  unite(sql, sep = ', ', remove = TRUE) %>%
  mutate(sql = paste('(', sql, '),', sep = ''))
level_sql[nrow(level_sql),] = paste(str_sub(level_sql[nrow(level_sql),],
                                              start = 1,
                                              end = -2),
                                      ';',
                                      sep = '')
for (line in level_sql) {
  statement = c(statement, line)
}

# Add taxon source statement
statement = c(statement,
              '',
              '-- Insert data into taxon source table',
              'INSERT INTO taxon_source (taxon_source_id, taxon_source, citation) VALUES'
)
source_sql = source_table %>%
  mutate_if(is.character,
            str_replace_all, pattern = '\'', replacement = '\'\'') %>%
  mutate(taxon_source = paste('\'', taxon_source, '\'', sep = '')) %>%
  mutate(citation = paste('\'', citation, '\'', sep = '')) %>%
  unite(sql, sep = ', ', remove = TRUE) %>%
  mutate(sql = paste('(', sql, '),', sep = ''))
source_sql[nrow(source_sql),] = paste(str_sub(source_sql[nrow(source_sql),],
                                            start = 1,
                                            end = -2),
                                    ';',
                                    sep = '')
for (line in source_sql) {
  statement = c(statement, line)
}

# Add hierarchy statement
statement = c(statement,
              '',
              '-- Insert data into hierarchy table',
              'INSERT INTO hierarchy (hierarchy_id, genus_accepted, family_id, category_id) VALUES'
)
hierarchy_sql = hierarchy_table %>%
  mutate(genus_accepted = paste('\'', genus_accepted, '\'', sep = '')) %>%
  unite(sql, sep = ', ', remove = TRUE) %>%
  mutate(sql = paste('(', sql, '),', sep = ''))
hierarchy_sql[nrow(hierarchy_sql),] = paste(str_sub(hierarchy_sql[nrow(hierarchy_sql),],
                                            start = 1,
                                            end = -2),
                                    ';',
                                    sep = '')
for (line in hierarchy_sql) {
  statement = c(statement, line)
}

# Add taxon accepted statement
statement = c(statement,
              '',
              '-- Insert data into accepted taxon table',
              'INSERT INTO taxon_accepted (accepted_id, name_accepted, auth_accepted_id, hierarchy_id, taxon_source_id, link_source, level_id, habit_id, native, non_native) VALUES'
)
accepted_sql = accepted_table %>%
  mutate_if(is.character,
            str_replace_all, pattern = '\'', replacement = '\'\'') %>%
  mutate(name_accepted = paste('\'', name_accepted, '\'', sep = '')) %>%
  mutate(link_source = paste('\'', link_source, '\'', sep = '')) %>%
  unite(sql, sep = ', ', remove = TRUE) %>%
  mutate(sql = paste('(', sql, '),', sep = ''))
accepted_sql[nrow(accepted_sql),] = paste(str_sub(accepted_sql[nrow(accepted_sql),],
                                                    start = 1,
                                                    end = -2),
                                            ';',
                                            sep = '')
for (line in accepted_sql) {
  statement = c(statement, line)
}

# Add taxon adjudicated statement
statement = c(statement,
              '',
              '-- Insert data into adjudicated taxon table',
              'INSERT INTO taxon_adjudicated (adjudicated_id, name_adjudicated, auth_adjudicated_id, status_adjudicated_id, accepted_id) VALUES'
)
adjudicated_sql = adjudicated_table %>%
  mutate(name_adjudicated = paste('\'', name_adjudicated, '\'', sep = '')) %>%
  unite(sql, sep = ', ', remove = TRUE) %>%
  mutate(sql = paste('(', sql, '),', sep = ''))
adjudicated_sql[nrow(adjudicated_sql),] = paste(str_sub(adjudicated_sql[nrow(adjudicated_sql),],
                                                  start = 1,
                                                  end = -2),
                                          ';',
                                          sep = '')
for (line in adjudicated_sql) {
  statement = c(statement, line)
}

# Finalize statement
statement = c(statement,
              '',
              '-- Commit transaction',
              'COMMIT TRANSACTION;')

# Write statement to SQL file
write_lines(statement, sql_taxonomy)