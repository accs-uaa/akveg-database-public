# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Export Data from Standard Queries
# Author: Timm Nawrocki
# Last Updated: 2021-10-31
# Usage: Must be executed in R 4.0.0+.
# Description: "Export Data from Standard Queries" exports data from a PostgreSQL database for a set of standard queries.
# ---------------------------------------------------------------------------

# Set root directory
drive = 'N:'
root_folder = 'ACCS_Work'

# Set repository directory
repository = 'C:/Users/timmn/Documents/Repositories/vegetation-plots-database'
query_folder = paste(repository,
                     'queries',
                     sep = '/')

# Define output folder
data_folder = paste(drive,
                    root_folder,
                    'Projects/VegetationEcology/AKVEG_PlotsDatabase/Data/Queries',
                    sep = '/')

# Define query files
file_checklist_comprehensive = paste(query_folder, 'Query_01_ChecklistComprehensive.sql', sep = '/')
file_checklist_vascular = paste(query_folder, 'Query_02_ChecklistVascular.sql', sep = '/')
file_checklist_bryophyte = paste(query_folder, 'Query_03_ChecklistBryophyte.sql', sep = '/')
file_checklist_lichen = paste(query_folder, 'Query_04_ChecklistLichen.sql', sep = '/')
file_citations_comprehensive = paste(query_folder, 'Query_05_CitationsComprehensive.sql', sep = '/')
file_citations_vascular = paste(query_folder, 'Query_06_CitationsVascular.sql', sep = '/')
file_schema = paste(query_folder, 'Query_07_DatabaseSchema.sql', sep = '/')
file_dictionary = paste(query_folder, 'Query_08_DatabaseDictionary.sql', sep = '/')
file_project = paste(query_folder, 'Query_09_Project.sql', sep = '/')
file_site = paste(query_folder, 'Query_10_Site.sql', sep = '/')
file_cover = paste(query_folder, 'Query_11_Cover.sql', sep = '/')
file_environment = paste(query_folder, 'Query_12_Environment.sql', sep = '/')
file_project_status = paste(query_folder, 'Query_13_ProjectStatus.sql', sep = '/')

# Define output files
output_checklist_comprehensive = paste(data_folder, 'checklist_comprehensive.csv', sep = '/')
output_checklist_vascular = paste(data_folder, 'checklist_vascular.csv', sep = '/')
output_checklist_bryophyte = paste(data_folder, 'checklist_bryophyte.csv', sep = '/')
output_checklist_lichen = paste(data_folder, 'checklist_lichen.csv', sep = '/')
output_citations_comprehensive = paste(data_folder, 'citations_comprehensive.csv', sep = '/')
output_citations_vascular = paste(data_folder, 'citations_vascular.csv', sep = '/')
output_schema = paste(data_folder, 'database_schema.csv', sep = '/')
output_dictionary = paste(data_folder, 'database_dictionary.csv', sep = '/')
output_project = paste(data_folder, 'projects.csv', sep = '/')
output_sites = paste(data_folder, 'sites.csv', sep = '/')
output_cover = paste(data_folder, 'cover.csv', sep = '/')
output_environment = paste(data_folder, 'environment.csv', sep = '/')
output_project_status = paste(data_folder, 'project_status.csv', sep = '/')

# Define input and output sets
input_set = c(file_checklist_comprehensive,
              file_checklist_vascular,
              file_checklist_bryophyte,
              file_checklist_lichen,
              file_citations_comprehensive,
              file_citations_vascular,
              file_schema,
              file_dictionary,
              file_project,
              file_site,
              file_cover,
              file_environment,
              file_project_status)
output_set = c(output_checklist_comprehensive,
               output_checklist_vascular,
               output_checklist_bryophyte,
               output_checklist_lichen,
               output_citations_comprehensive,
               output_citations_vascular,
               output_schema,
               output_dictionary,
               output_project,
               output_sites,
               output_cover,
               output_environment,
               output_project_status)

# Import required libraries
library(dplyr)
library(lubridate)
library(reader)
library(readr)
library(readxl)
library(RPostgres)
library(stringr)
library(tibble)
library(tidyr)

# Import database connection function
connection_script = paste(repository,
                          'package_DataProcessing',
                          'connectDatabasePostGreSQL.R',
                          sep = '/')
source(connection_script)

# Create a connection to the AKVEG PostgreSQL database
authentication = paste(drive,
                       root_folder,
                       'Administrative/Credentials/accs-postgresql/authentication_akveg.csv',
                       sep = '/')
akveg_connection = connect_database_postgresql(authentication)

# Loop through queries and export data
count = 1
for (query in input_set) {
  # Define output file
  output_file = output_set[count]
  
  # Read query into data
  query_data = n.readLines(query, 100, comment = '--', skip = 10, header = FALSE)
  query_data = paste(query_data, collapse = '\n')
  
  # Read query from PostgreSQL database
  query_result = as_tibble(dbGetQuery(akveg_connection, query_data))
  
  # Export query result as csv
  write.csv(query_result, file = output_file, fileEncoding = 'UTF-8', row.names = FALSE)
  
  # Increase counter
  count = count + 1
}