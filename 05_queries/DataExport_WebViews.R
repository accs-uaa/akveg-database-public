# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Export data to web views
# Author: Timm Nawrocki
# Last Updated: 2022-10-18
# Usage: Must be executed in R 4.0.0+.
# Description: "Export data to web views" exports data from the AKVEG database for web views.
# ---------------------------------------------------------------------------

# Set root directory
drive = 'N:'
root_folder = 'ACCS_Work'

# Define input folders
data_folder = paste(drive,
                    root_folder,
                    'Projects/VegetationEcology/AKVEG_Database/Data/Web_Views',
                    sep = '/')
authentication_folder = paste(drive,
                              root_folder,
                              'Administrative/Credentials/akveg_private_read',
                              sep = '/')

# Set repository directory
repository = 'C:/Users/timmn/Documents/Repositories/akveg-database'

# Define query files
file_checklist_comprehensive = paste(repository, '05_queries', 'web_views',
                                     'Query_00a_ChecklistComprehensive.sql',
                                     sep = '/')
file_checklist_vascular = paste(repository, '05_queries', 'web_views',
                                'Query_00b_ChecklistVascular.sql',
                                sep = '/')
file_checklist_bryophyte = paste(repository, '05_queries', 'web_views',
                                 'Query_00c_ChecklistBryophyte.sql',
                                 sep = '/')
file_checklist_lichen = paste(repository, '05_queries', 'web_views',
                              'Query_00d_ChecklistLichen.sql',
                              sep = '/')
file_citations_comprehensive = paste(repository, '05_queries', 'standard',
                                     'Query_00e_CitationsComprehensive.sql',
                                     sep = '/')
file_citations_vascular = paste(repository, '05_queries', 'standard',
                                'Query_00f_CitationsVascular.sql',
                                sep = '/')
file_schema = paste(repository, '05_queries', 'standard',
                    'Query_00g_DatabaseSchema.sql',
                    sep = '/')
file_dictionary = paste(repository, '05_queries', 'standard',
                        'Query_00h_DatabaseDictionary.sql',
                        sep = '/')
file_project = paste(repository, '05_queries', 'standard',
                     'Query_01_Project.sql',
                     sep = '/')
file_site = paste(repository, '05_queries', 'standard',
                  'Query_02_Site.sql',
                  sep = '/')
file_site_visit = paste(repository, '05_queries', 'standard',
                        'Query_03_SiteVisit.sql',
                        sep = '/')
file_vegetation_cover = paste(repository, '05_queries', 'standard',
                              'Query_05_VegetationCover.sql',
                              sep = '/')
file_abiotic_cover = paste(repository, '05_queries', 'standard',
                           'Query_06_AbioticTopCover.sql',
                           sep = '/')
file_tussock_cover = paste(repository, '05_queries', 'standard',
                           'Query_07_WholeTussockCover.sql',
                           sep = '/')
file_ground_cover = paste(repository, '05_queries', 'standard',
                          'Query_08_GroundCover.sql',
                          sep = '/')
file_structural_cover = paste(repository, '05_queries', 'standard',
                              'Query_09_StructuralGroupCover.sql',
                              sep = '/')
file_shrub_structure = paste(repository, '05_queries', 'standard',
                             'Query_11_ShrubStructure.sql',
                             sep = '/')
file_environment = paste(repository, '05_queries', 'standard',
                         'Query_12_Environment.sql',
                         sep = '/')
file_soils = paste(repository, '05_queries', 'standard',
                   'Query_13_Soils.sql',
                   sep = '/')

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
              file_site_visit,
              file_vegetation_cover,
              file_abiotic_cover,
              file_tussock_cover,
              file_ground_cover,
              file_structural_cover,
              file_shrub_structure,
              file_environment,
              file_soils)
output_list = c('checklist_comprehensive',
                'checklist_vascular',
                'checklist_bryophyte',
                'checklist_lichen',
                'citations_comprehensive',
                'citations_vascular',
                'database_schema',
                'database_dictionary',
                'project',
                'site',
                'site_visit',
                'vegetation_cover',
                'abiotic_top_cover',
                'whole_tussock_cover',
                'ground_cover',
                'structural_group_cover',
                'shrub_structure',
                'environment',
                'soils')

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
authentication_file = paste(authentication_folder,
                       'authentication_akveg_private.csv',
                       sep = '/')
akveg_connection = connect_database_postgresql(authentication_file)

# Loop through queries and export data
count = 1
for (query in input_set) {
  # Define output file
  output_name = output_list[count]
  output_file = paste(data_folder,
                      paste(output_name, 'csv', sep = '.'),
                      sep = '/')
  
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