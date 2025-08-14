# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Minor corrections to the ACCS Nelchina 2022 upload
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2023-02-03
# Usage: Script should be executed in R 4.0.0+.
# Description: "Minor corrections to the ACCS Nelchina 2022 upload" adds name_adjudicated, corrects some fields, updates project code, and enforces NODATA values on the ACCS Nelchina 2022 dataset.
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
project_folder = '31_accs_nelchina_2022'

# Define input data files
project_input = '01_project_accsnelchina2022.xlsx'
site_input = '02_site_accsnelchina2022.xlsx'
site_visit_input = '03_sitevisit_accsnelchina2022.xlsx'
cover_input = '05_vegetationcover_accsnelchina2022.xlsx'
abiotic_input = '06_accs_nelchina.xlsx'
tussock_input = 
ground_input = 
shrub_input = 
env_input = 
soil_input =

# Define output data files
site_output = '02_site_accsnelchina2022'
site_visit_output = '03_sitevisit_accsnelchina2022'
cover_output = '05_vegetationcover_accsnelchina2022'
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
  # Add missing data
  mutate(establishing_project = 'accs_nelchina_2022') %>%
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
  mutate(homogenous = 'TRUE') %>%
  # Select final fields
  select(site_visit_id, project_code, site_code, data_tier, observe_date,
         veg_observer, veg_recorder, env_observer, soils_observer, structural_class,
         scope_vascular, scope_bryophyte, scope_lichen, homogenous)

# Parse new cover table
cover_data = cover_original %>%
  mutate(name_original = tolower(name_original)) %>%
  # Join name adjudicated
  left_join(taxon_data, by = c('name_original' = 'taxon_code'), keep = TRUE) %>%
  rename(code_original = name_original) %>%
  rename(name_original = taxon_name) %>%
  # Correct name original
  mutate(name_original = case_when(code_original == 'dicran' ~ 'Dicranum',
                                   code_original == 'fgfeamos' ~ 'feathermoss (other)',
                                   code_original == 'fgfollic' ~ 'foliose lichen (other)',
                                   code_original == 'fgturmos' ~ 'turf moss',
                                   code_original == 'poaarc' ~ 'Poa arctica',
                                   code_original == 'polytr' ~ 'Polytrichum',
                                   code_original == 'callie' ~ 'Calliergon',
                                   code_original == 'sincli' ~ 'Cinclidium',
                                   code_original == 'sphagnum' ~ 'Sphagnum',
                                   code_original == 'calunc' ~ 'Cladonia uncialis',
                                   code_original == 'fgcrulic' ~ 'crustose lichen (non-orange)',
                                   code_original == 'carmic' ~ 'Carex microchaeta',
                                   code_original == 'brachy' ~ 'Brachythecium',
                                   code_original == 'carant' ~ 'Carex anthoxanthea',
                                   code_original == 'salarc' ~ 'Salix arctica',
                                   code_original == 'calran' ~ 'Cladonia rangiferina',
                                   code_original == 'pohwal' ~ 'Pohlia wahlenbergii',
                                   code_original == 'salbar' ~ 'Salix barrattiana',
                                   code_original == 'tepatr' ~ 'Tephroseris atropurpurea',
                                   code_original == 'silacu' ~ 'Silene acaulis',
                                   TRUE ~ name_original)) %>%
  filter(code_original != 'pyrfri' &
           code_original != 'poltur' &
           code_original != 'chegra' &
           code_original != 'micric') %>%
  mutate(name_adjudicated = name_original) %>%
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