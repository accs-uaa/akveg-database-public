# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Prepare project status metadata for upload
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2021-01-29
# Usage: Script should be executed in R 4.0.0+.
# Description: "Prepare project status metadata for upload" parses project status metadata for upload into the project status table.
# ---------------------------------------------------------------------------

# Set root directory
drive = 'N:'
root_folder = 'ACCS_Work'

# Define input folders
data_folder = paste(drive,
                    root_folder,
                    'Projects/VegetationEcology/AKVEG_PlotsDatabase/Data/Tables_Metadata',
                    sep = '/')

# Set repository directory
repository = 'C:/Users/timmn/Documents/Repositories/vegetation-plots-database'

# Designate input file
input_file = paste(data_folder,
                   'project_status.xlsx',
                   sep = '/')

# Designate output sql file
output_sql = paste(repository,
                   '02_data_insertion',
                   '07_b_Insert_ProjectStatus.sql',
                   sep = '/')

# Import required libraries
library(dplyr)
library(readr)
library(readxl)
library(stringr)
library(tibble)
library(tidyr)

# Read project status data into data frame
input_table = read_excel(input_file, sheet = 'projectStatus')

#### WRITE DATA TO SQL FILE

# Write statement header
statement = c(
  '-- -*- coding: utf-8 -*-',
  '-- ---------------------------------------------------------------------------',
  '-- Insert project status',
  '-- Author: Timm Nawrocki, Alaska Center for Conservation Science',
  paste('-- Last Updated:', Sys.Date(), sep = ' '),
  '-- Usage: Script should be executed in a PostgreSQL 12 database.',
  '-- Description: "Insert project status" pushes the project status metadata for all projects into the project status table of the database.',
  '-- ---------------------------------------------------------------------------',
  '',
  '-- Initialize transaction',
  'START TRANSACTION;',
  ''
)

# Add data insert statement
statement = c(statement,
              '-- Insert project status metadata into project status table',
              'INSERT INTO project_status (project_status_id, project_id, project_modified, site_modified, cover_modified, environment_modified, modified_by_id) VALUES'
)
input_sql = input_table %>%
  mutate_if(is.character,
            str_replace_all, pattern = '\'', replacement = '\'\'') %>%
  mutate(project_modified = paste('\'', project_modified, '\'', sep = '')) %>%
  mutate(site_modified = paste('\'', site_modified, '\'', sep = '')) %>%
  mutate(cover_modified = paste('\'', cover_modified, '\'', sep = '')) %>%
  mutate(environment_modified = paste('\'', environment_modified, '\'', sep = '')) %>%
  unite(sql, sep = ', ', remove = TRUE) %>%
  mutate(sql = paste('(', sql, '),', sep =''))
input_sql[nrow(input_sql),] = paste(str_sub(input_sql[nrow(input_sql),],
                                            start = 1,
                                            end = -2),
                                    ';',
                                    sep = '')
for (line in input_sql) {
  statement = c(statement, line)
}

# Finalize statement
statement = c(
  statement,
  '',
  '-- Commit transaction',
  'COMMIT TRANSACTION;'
)

# Replace NA values in statement
statement = str_replace(statement, ', \'NA\',', ', NULL,')

# Write statement to SQL file
write_lines(statement, output_sql)