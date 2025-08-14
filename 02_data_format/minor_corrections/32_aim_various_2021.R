# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Minor corrections to the AIM Various 2021 upload
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2022-10-13
# Usage: Script should be executed in R 4.0.0+.
# Description: "Minor corrections to the AIM Various 2021 upload" adds name_adjudicated, corrects some fields, updates project code, and enforces NODATA values on the AIM Various 2021 dataset.
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
project_folder = '32_aim_various_2021'

# Define input data files
project_input = '01_project_aimvarious2021.xlsx'
site_input = '02_site_aimvarious2021.xlsx'
site_visit_input = '03_sitevisit_aimvarious2021.xlsx'
cover_input = '05_vegetationcover_aimvarious2021.xlsx'

# Define output data files
site_output = '02_site_aimvarious2021'
site_visit_output = '03_sitevisit_aimvarious2021'
cover_output = '05_vegetationcover_aimvarious2021'
output_list = c(site_output, site_visit_output, cover_output)

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
                       'Administrative/Credentials/accs-postgresql/authentication_akveg.csv',
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

# Parse new site table
site_data = site_original %>%
  # Join site visit data
  inner_join(site_visit_original, by = 'site_code') %>%
  # Correct erroneous data
  mutate(project_code = case_when(project_code == 'AIM Anchorage FO' ~ 'aim_campbell_2018',
                                  project_code == 'AIM GMT-2' ~ 'aim_gmt2_2021',
                                  project_code == 'AIM Kobuk-Seward NE' ~ 'aim_kobuknortheast_2021',
                                  project_code == 'AIM Kobuk-Seward W' ~ 'aim_kobukwest_2021',
                                  TRUE ~ project_code)) %>%
  rename(establishing_project = project_code) %>%
  mutate(plot_dimensions_m = '30 radius') %>%
  # Add missing data
  mutate(perspective = 'ground') %>%
  # Enforce distinct
  distinct(site_code, establishing_project, perspective, cover_method, h_datum,
           latitude_dd, longitude_dd, h_error_m, positional_accuracy, plot_dimensions_m, location_type) %>%
  # Select final fields
  select(site_code, establishing_project, perspective, cover_method, h_datum,
         latitude_dd, longitude_dd, h_error_m, positional_accuracy, plot_dimensions_m, location_type)


# Parse new site visit table
site_visit_data = site_visit_original %>%
  # Join site data
  inner_join(site_original, by = 'site_code') %>%
  # Correct erroneous data
  mutate(project_code = case_when(project_code == 'AIM Anchorage FO' ~ 'aim_campbell_2018',
                                  project_code == 'AIM GMT-2' ~ 'aim_gmt2_2021',
                                  project_code == 'AIM Kobuk-Seward NE' ~ 'aim_kobuknortheast_2021',
                                  project_code == 'AIM Kobuk-Seward W' ~ 'aim_kobukwest_2021',
                                  TRUE ~ project_code)) %>%
  mutate(data_tier = case_when(project_code == 'aim_campbell_2018' ~ 'vegetation classification',
                               TRUE ~ data_tier)) %>%
  mutate(structural_class = 'n/assess') %>%
  mutate(homogenous = 'FALSE') %>%
  # Replace na values
  mutate(veg_recorder = replace_na(veg_recorder, 'NULL')) %>%
  mutate(env_observer = replace_na(env_observer, 'NULL')) %>%
  mutate(soils_observer = replace_na(soils_observer, 'NULL')) %>%
  # Enforce distinct
  distinct(site_visit_id, project_code, site_code, data_tier, observe_date,
           veg_observer, veg_recorder, env_observer, soils_observer, structural_class,
           scope_vascular, scope_bryophyte, scope_lichen, homogenous) %>%
  # Select final fields
  select(site_visit_id, project_code, site_code, data_tier, observe_date,
         veg_observer, veg_recorder, env_observer, soils_observer, structural_class,
         scope_vascular, scope_bryophyte, scope_lichen, homogenous)

# Parse new cover table
cover_data = cover_original %>%
  # Join name adjudicated
  left_join(taxon_data, by = c('name_original' = 'taxon_name'), keep = TRUE) %>%
  rename(name_adjudicated = taxon_name) %>%
  mutate(name_adjudicated = case_when(name_original == 'Orthilia obovata' ~ 'Orthilia obtusata',
                                      name_original == 'Astragalus alpinus var. alpinus' ~
                                        'Astragalus alpinus',
                                      name_original == 'Vaccinium oxycoccos' ~ 'Oxycoccus microcarpus',
                                      TRUE ~ name_adjudicated)) %>%
  # Select final fields
  select(site_visit_id, name_original, name_adjudicated, cover_type, dead_status, cover_percent)

# Check if any problem taxa remain
problem_list = cover_data %>%
  filter(is.na(name_adjudicated)) %>%
  distinct(name_original, name_adjudicated)

# Export new tables to csv
table_list = list(site_data, site_visit_data, cover_data)
for (output in output_list) {
  csv_output = paste(data_folder, project_folder, paste(output, '.csv', sep = ''), sep = '/')
  export_table = table_list[match(output, output_list)]
  write.csv(export_table, file = csv_output, fileEncoding = 'UTF-8', row.names = FALSE)
}