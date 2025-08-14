# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Export Data from Standard Queries
# Author: Timm Nawrocki
# Last Updated: 2023-02-08
# Usage: Must be executed in R 4.0.0+.
# Description: "Export Data from Standard Queries" exports a vascular plant checklist from a PostgreSQL database and combines with review results.
# ---------------------------------------------------------------------------

# Set root directory
drive = 'N:'
root_folder = 'ACCS_Work'

# Set repository directory
repository = 'C:/Users/timmn/Documents/Repositories/akveg-database'
query_folder = paste(repository,
                     '00_miscellaneous',
                     sep = '/')

# Define output folder
data_folder = paste(drive,
                    root_folder,
                    'Projects/VegetationEcology/AKVEG_Database/Data/Tables_Taxonomy',
                    sep = '/')

# Define query files
file_checklist_vascular = paste(query_folder, 'Review_VascularPlantChecklist.sql', sep = '/')

# Define input files
review_file = paste(data_folder,
                    'review_20220511',
                    'VascularChecklist_20220930.xlsx',
                    sep = '/')

# Define output files
review_file_1 = paste(data_folder,
                      'review_20230210',
                      'VascularReview_Group1_20230210.csv',
                      sep = '/')
review_file_2 = paste(data_folder,
                      'review_20230210',
                      'VascularReview_Group2_20230210.csv',
                      sep = '/')
review_file_3 = paste(data_folder,
                      'review_20230210',
                      'VascularReview_Group3_20230210.csv',
                      sep = '/')

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

# Read input data
reviewed_list = read_xlsx(review_file, sheet = 'review') %>%
  select(name, review_status, changed, notes)

# Import database connection function
connection_script = paste(repository,
                          'package_DataProcessing',
                          'connectDatabasePostGreSQL.R',
                          sep = '/')
source(connection_script)

# Create a connection to the AKVEG PostgreSQL database
authentication = paste(drive,
                       root_folder,
                       'Administrative/Credentials/akveg_build',
                       'authentication_akveg_build.csv',
                       sep = '/')
akveg_connection = connect_database_postgresql(authentication)

# Read query into data
query = n.readLines(file_checklist_vascular, 100, comment = '--', skip = 10, header = FALSE)
query = paste(query, collapse = '\n')

# Read query from PostgreSQL database
query_result = as_tibble(dbGetQuery(akveg_connection, query))

# Join review lists with query results
checklist_vascular = query_result %>%
  left_join(reviewed_list, by = 'name') %>%
  select(family, genus, code, name, author, status, name_accepted, author_accepted,
         source, level, category, habit, native, non_native,
         review_status, changed, notes)

# Split review lists by family
review_group_1 = checklist_vascular %>%
  filter(family == 'Lythraceae' |
           family == 'Malvaceae' |
           family == 'Melanthiaceae' |
           family == 'Menyanthaceae' |
           family == 'Montiaceae' |
           family == 'Myricaceae' |
           family == 'Myrsinaceae' |
           family == 'Nymphaeaceae' |
           family == 'Onagraceae' |
           family == 'Ophioglossaceae' |
           family == 'Orchidaceae' |
           family == 'Orobanchaceae' |
           family == 'Papaveraceae' |
           family == 'Parnassiacae' |
           family == 'Phrymaceae' |
           family == 'Pinaceae' |
           family == 'Plantaginaceae' |
           family == 'Plumbaginaceae' |
           family == 'Polemoniacaee' |
           family == 'Polygonaceae' |
           family == 'Polypodiaceae' |
           family == 'Potamogetonaceae')
review_group_2 = checklist_vascular %>%
  filter(family == 'Poaceae' |
           family == 'Primulaceae' |
           family == 'Pteridaceae' |
           family == 'Ranunculaceae')
review_group_3 = checklist_vascular %>%
  filter(family == 'Rosaceae' |
           family == 'Rubiaceae' |
           family == 'Ruppiaceae' |
           family == 'Ruscaceae' |
           family == 'Salicaceae' |
           family == 'Sapindaceae' |
           family == 'Saxifragaceae' |
           family == 'Scheuchzeriaceae' |
           family == 'Scrophulariaceae' |
           family == 'Selaginellacea' |
           family == 'Solanaceae' |
           family == 'Sparganiaceae' |
           family == 'Taxaceae' |
           family == 'Thelypteridaceae' |
           family == 'Themylaeaceae' |
           family == 'Tofieldiaceae' |
           family == 'Typhaceae' |
           family == 'Urticaceae' |
           family == 'Valerianaceae' |
           family == 'Violaceae' |
           family == 'Viscaceae' |
           family == 'Woodsiaceae' |
           family == 'Zosteraceae')
  
# Export query result as csv
write.csv(review_group_1, file = review_file_1, fileEncoding = 'UTF-8', row.names = FALSE)
write.csv(review_group_2, file = review_file_2, fileEncoding = 'UTF-8', row.names = FALSE)
write.csv(review_group_3, file = review_file_3, fileEncoding = 'UTF-8', row.names = FALSE)