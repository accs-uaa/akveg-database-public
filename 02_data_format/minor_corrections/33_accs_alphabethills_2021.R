# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Minor corrections to the ACCS Alphabet Hills 2021 upload
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2022-10-24
# Usage: Script should be executed in R 4.0.0+.
# Description: "Minor corrections to the ACCS Alphabet Hills 2022 upload" adds name_adjudicated, corrects some fields, updates project code, and enforces NODATA values on the ACCS Alphabet Hills 2022 dataset.
# ---------------------------------------------------------------------------

# Set root directory
drive = 'N:'
root_folder = 'ACCS_Work'

# Define input folders
data_folder = paste(drive,
                    root_folder,
                    'Projects/VegetationEcology/AKVEG_Database/Data/Data_Plots',
                    sep = '/')

# Define project
project_folder = '33_accs_alphabethills_2021'

# Define input data files
project_input = '01_project_accsalphabethills2021.xlsx'
site_input = '02_site_accsalphabethills2021.xlsx'
site_visit_input = '03_sitevisit_accsalphabethills2021.xlsx'
cover_input = '05_vegetationcover_accsalphabethills2021.xlsx'
environment_input = '12_environment_accsalphabethills2021.xlsx'

# Define output data files
project_output = '01_project_accsalphabethills2021'
site_output = '02_site_accsalphabethills2021'
site_visit_output = '03_sitevisit_accsalphabethills2021'
cover_output = '05_vegetationcover_accsalphabethills2021'
environment_output = '12_environment_accsalphabethills2021'
output_list = c(project_output, site_output, site_visit_output, cover_output, environment_output)

# Set repository directory
repository = 'C:/Users/timmn/Documents/Repositories/akveg-database'

# Import required libraries
library(dplyr)
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
                       'Administrative/Credentials/akveg_build/authentication_akveg_build.csv',
                       sep = '/')
database_connection = connect_database_postgresql(authentication)

# Read constraints into data frames from AKVEG
query_taxon = 'SELECT * FROM taxon_all'
taxon_data = as_tibble(dbGetQuery(database_connection, query_taxon))

# Read input files into data frames
project_original = read_excel(paste(data_folder, project_folder, 'excel', project_input, sep = '/'),
                              sheet = 'project')
site_original = read_excel(paste(data_folder, project_folder, 'excel', site_input, sep = '/')
                           , sheet = 'site')
site_visit_original = read_excel(paste(data_folder, project_folder, 'excel', site_visit_input, sep = '/')
                                 , sheet = 'site_visit')
cover_original = read_excel(paste(data_folder, project_folder, 'excel', cover_input, sep = '/')
                                 , sheet = 'cover')
environment_original = read_excel(paste(data_folder, project_folder, 'excel', environment_input, sep = '/')
                            , sheet = 'environment')

# Parse new site table
site_data = site_original %>%
  # Add prefix to site codes
  mutate(site_code = paste('ALPH', site_code, sep = '')) %>%
  # Correct erroneous data
  mutate(establishing_project_code = 'accs_alphabethills_2021') %>%
  rename(establishing_project = establishing_project_code) %>%
  # Select final fields
  select(site_code, establishing_project, perspective, cover_method, h_datum,
         latitude_dd, longitude_dd, h_error_m, positional_accuracy, plot_dimensions_m, location_type)

# Parse new site visit table
site_visit_data = site_visit_original %>%
  # Add prefix to site codes
  mutate(site_code = paste('ALPH', site_code, sep = '')) %>%
  # Add prefix to site visit id
  mutate(site_visit_id = paste('ALPH', site_visit_id, sep = '')) %>%
  # Correct erroneous data
  mutate(project_code = 'accs_alphabethills_2021') %>%
  mutate(veg_observer = 'Timm Nawrocki') %>%
  mutate(soils_observer = 'NULL') %>%
  mutate(structural_class = 'n/assess') %>%
  mutate(homogenous = 'TRUE') %>%
  # Select final fields
  select(site_visit_id, project_code, site_code, data_tier, observe_date,
         veg_observer, veg_recorder, env_observer, soils_observer, structural_class,
         scope_vascular, scope_bryophyte, scope_lichen, homogenous)

# Export new tables to csv
table_list = list(project_original, site_data, site_visit_data, cover_original, environment_original)
for (output in output_list) {
  csv_output = paste(data_folder, project_folder, paste(output, '.csv', sep = ''), sep = '/')
  export_table = table_list[match(output, output_list)]
  write.csv(export_table, file = csv_output, fileEncoding = 'UTF-8', row.names = FALSE)
}