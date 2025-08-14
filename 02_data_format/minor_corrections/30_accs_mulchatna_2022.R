# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Minor corrections to the ACCS Mulchatna 2022 upload
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2022-10-13
# Usage: Script should be executed in R 4.0.0+.
# Description: "Minor corrections to the ACCS Mulchatna 2022 upload" adds name_adjudicated, corrects some fields, updates project code, and enforces NODATA values on the ACCS Mulchatna 2022 dataset.
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
project_folder = '30_accs_mulchatna_2022'

# Define input data files
project_input = '01_project_accsmulchatna2022.xlsx'
site_input = '02_site_accsmulchatna2022.xlsx'
site_visit_input = '03_sitevisit_accsmulchatna2022.xlsx'
cover_input = '05_vegetationcover_accsmulchatna2022.xlsx'
abiotic_input = '06_abiotictopcover_accsmulchatna2022.xlsx'
tussock_input = '07_wholetussockcover_accsmulchatna2022.xlsx'
ground_input = '08_groundcover_accsmulchatna2022.xlsx'
structural_input = '09_structuralgroupcover_accsmulchatna2022.xlsx'
shrub_input = '11_shrubstructure_accsmulchatna2022.xlsx'
environment_input = '12_environment_accsmulchatna2022.xlsx'
soils_input = '13_soils_accsmulchatna2022.xlsx'

# Define output data files
site_output = '02_site_accsmulchatna2022'
site_visit_output = '03_sitevisit_accsmulchatna2022'
cover_output = '05_vegetationcover_accsmulchatna2022'
abiotic_output = '06_abiotictopcover_accsmulchatna2022'
tussock_output = '07_wholetussockcover_accsmulchatna2022'
ground_output = '08_groundcover_accsmulchatna2022'
structural_output = '09_structuralgroupcover_accsmulchatna2022'
shrub_output = '11_shrubstructure_accsmulchatna2022'
environment_output = '12_environment_accsmulchatna2022'
soils_output = '13_soils_accsmulchatna2022'
output_list = c(site_output, site_visit_output, cover_output, abiotic_output, tussock_output,
                ground_output, structural_output, shrub_output, environment_output, soils_output)

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
abiotic_original = read_excel(paste(data_folder, project_folder, 'excel', abiotic_input, sep = '/')
                            , sheet = 'abiotic')
tussock_original = read_excel(paste(data_folder, project_folder, 'excel', tussock_input, sep = '/')
                            , sheet = 'tussock')
ground_original = read_excel(paste(data_folder, project_folder, 'excel', ground_input, sep = '/')
                            , sheet = 'ground')
structural_original = read_excel(paste(data_folder, project_folder, 'excel', structural_input, sep = '/')
                            , sheet = 'structural')
shrub_original = read_excel(paste(data_folder, project_folder, 'excel', shrub_input, sep = '/')
                            , sheet = 'shrub')
environment_original = read_excel(paste(data_folder, project_folder, 'excel', environment_input, sep = '/')
                            , sheet = 'environment')
soils_original = read_excel(paste(data_folder, project_folder, 'excel', soils_input, sep = '/')
                            , sheet = 'soils')

# Parse new site table
site_data = site_original %>%
  # Add missing data
  mutate(establishing_project = 'accs_mulchatna_2022') %>%
  mutate(perspective = 'ground') %>%
  # Correct erroneous data
  mutate(h_datum = 'NAD83') %>%
  mutate(plot_dimensions_m = '12.5 radius') %>%
  # Select final fields
  select(site_code, establishing_project, perspective, cover_method, h_datum,
         latitude_dd, longitude_dd, h_error_m, positional_accuracy, plot_dimensions_m, location_type)


# Parse new site visit table
site_visit_data = site_visit_original %>%
  # Join site data
  inner_join(site_original, by = 'site_code') %>%
  # Correct erroneous data
  mutate(project_code = 'accs_mulchatna_2022') %>%
  mutate(data_tier = 'ecological land classification') %>%
  # Select final fields
  select(site_visit_id, project_code, site_code, data_tier, observe_date,
         veg_observer, veg_recorder, env_observer, soils_observer, structural_class,
         scope_vascular, scope_bryophyte, scope_lichen, homogenous)

# Parse new cover table
cover_data = cover_original %>%
  select(-name_adjudicated) %>%
  # Join name adjudicated
  left_join(taxon_data, by = c('name_original' = 'taxon_name'), keep = TRUE) %>%
  rename(name_adjudicated = taxon_name) %>%
  # Select final fields
  select(site_visit_id, name_original, name_adjudicated, cover_type, dead_status, cover_percent)

# Check if any problem taxa remain
problem_list = cover_data %>%
  filter(is.na(name_adjudicated)) %>%
  distinct(name_original, name_adjudicated)

# Parse new whole tussock cover table
tussock_data = tussock_original %>%
  mutate(cover_type = 'absolute canopy cover')

# Parse new structural group cover table
structural_data = structural_original %>%
  mutate(cover_type = 'absolute foliar cover')

# Parse new environment table
environment_data = environment_original %>%
  mutate(disturbance_severity = case_when(disturbance_severity == 'none' ~ 'NULL',
                                          TRUE ~ disturbance_severity)) %>%
  mutate(restrictive_type = case_when(restrictive_type == 'none' ~ 'NULL',
                                      TRUE ~ restrictive_type)) %>%
  mutate(surface_water = case_when(surface_water == 'yes' ~ 'TRUE',
                                   surface_water == 'no' ~ 'FALSE',
                                   TRUE ~ 'ERROR'))

# Parse new soils table
soils_data = soils_original %>%
  mutate(dominant_texture_40_cm = 'NULL') %>%
  mutate(soil_class = 'NULL') %>%
  mutate(cryoturbation = 'FALSE')

# Export new tables to csv
table_list = list(site_data, site_visit_data, cover_data, abiotic_original, tussock_data,
                  ground_original, structural_data, shrub_original, environment_data, soils_data)
for (output in output_list) {
  csv_output = paste(data_folder, project_folder, paste(output, '.csv', sep = ''), sep = '/')
  export_table = table_list[match(output, output_list)]
  write.csv(export_table, file = csv_output, fileEncoding = 'UTF-8', row.names = FALSE)
}