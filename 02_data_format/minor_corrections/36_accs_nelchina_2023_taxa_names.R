# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Minor corrections to the ACCS Nelchina 2023 upload
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2023-02-04
# Usage: Script should be executed in R 4.0.0+.
# Description: "Minor corrections to the ACCS Nelchina 2023 upload" revises name_original to use names instead of codes.
# ---------------------------------------------------------------------------

# Import required libraries
library(dplyr)
library(readr)
library(readxl)
library(RPostgres)
library(stringr)
library(tibble)
library(tidyr)

# Set root directory
drive = 'D:'
root_folder = 'ACCS_Work'

# Define input folders
data_folder = paste(drive,
                    root_folder,
                    'Projects/VegetationEcology/AKVEG_Database/Data/Data_Plots',
                    sep = '/')

# Define project
project_folder = paste(data_folder, '36_accs_nelchina_2023', sep = '/')

# Define input data files
cover_input = paste(project_folder, 'working/05_vegetationcover_accsnelchina2023_corrected.csv', sep = '/')
shrub_input = paste(project_folder, 'working/11_shrubstructure_accsnelchina2023_corrected.csv', sep = '/')

# Define output data files
cover_output = paste(project_folder, '05_vegetationcover_accsnelchina2023.csv', sep = '/')
shrub_output = paste(project_folder, '11_shrubstructure_accsnelchina2023.csv', sep = '/')

# Set repository directory
repository = 'C:/Users/timmn/Documents/Repositories/akveg-database'

# Import database connection function
connection_script = paste(repository,
                          'package_DataProcessing',
                          'connect_database_postgresql.R',
                          sep = '/')
source(connection_script)

# Create a connection to the AKVEG PostgreSQL database
authentication = paste(drive,
                       root_folder,
                       'Administrative/Credentials/akveg_build/authentication_akveg_build.csv',
                       sep = '/')
database_connection = connect_database_postgresql(authentication)

# Read constraints into dataframes from AKVEG
query_taxa = 'SELECT * FROM taxon_all'
taxa_data = as_tibble(dbGetQuery(database_connection, query_taxa))

# Convert cover data
cover_data = read_csv(cover_input) %>%
  left_join(taxa_data, by = c('name_original' = 'taxon_code')) %>%
  select(site_visit_code, taxon_name, name_adjudicated, cover_type, dead_status, cover_percent) %>%
  rename(name_original = taxon_name)
write.csv(cover_data, file = cover_output, fileEncoding = 'UTF-8', row.names = FALSE)

# Convert shrub data
shrub_data = read_csv(shrub_input) %>%
  mutate(name_adjudicated = name_original)
write.csv(shrub_data, file = shrub_output, fileEncoding = 'UTF-8', row.names = FALSE)
