# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Minor corrections to the NPS SWAN 2021 upload
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2022-10-13
# Usage: Script should be executed in R 4.0.0+.
# Description: "Minor corrections to the NPS SWAN 2021 upload" adds name_adjudicated, corrects some fields, updates project code, and enforces NODATA values on the NPS SWAN 2021 dataset.
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
project_folder = '25_nps_swan_2021'

# Define input data files
project_input = '01_project_npsswan2021.xlsx'
site_input = '02_site_npsswan2021.xlsx'
site_visit_input = '03_sitevisit_npsswan2021.xlsx'
cover_input = '05_vegetationcover_npsswan2021.xlsx'
abiotic_input = '06_abiotictopcover_npsswan2021.xlsx'

# Define output data files
site_output = '02_site_npsswan2021'
site_visit_output = '03_sitevisit_npsswan2021'
cover_output = '05_vegetationcover_npsswan2021'
abiotic_output = '06_abiotictopcover_npsswan2021'
output_list = c(site_output, site_visit_output, cover_output, abiotic_output)

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
abiotic_original = read_excel(paste(data_folder, project_folder, 'excel', abiotic_input, sep = '/')
                              , sheet = 'abiotic')

# Parse new site table
site_data = site_original %>%
  # Join site visit data
  inner_join(site_visit_original, by = 'site_code') %>%
  # Rename columns
  rename(establishing_project = project_code) %>%
  # Add missing data
  mutate(perspective = 'ground') %>%
  # Correct erroneous data
  mutate(establishing_project = 'nps_swan_2021') %>%
  # Select final fields
  distinct(site_code, establishing_project, perspective, cover_method, h_datum,
           latitude_dd, longitude_dd, h_error_m, positional_accuracy, plot_dimensions_m, location_type)

# Parse new site visit table
site_visit_data = site_visit_original %>%
  # Join site data
  inner_join(site_original, by = 'site_code') %>%
  # Correct erroneous data
  mutate(veg_observer = 'Unknown') %>%
  mutate(veg_recorder = 'Unknown') %>%
  mutate(env_observer = 'NULL') %>%
  mutate(soils_observer = 'NULL') %>%
  mutate(project_code = 'nps_swan_2021') %>%
  mutate(homogenous = 'TRUE') %>%
  # Select final fields
  select(site_visit_id, project_code, site_code, data_tier, observe_date,
         veg_observer, veg_recorder, env_observer, soils_observer, structural_class,
         scope_vascular, scope_bryophyte, scope_lichen, homogenous)

# Parse new cover table
cover_data = cover_original %>%
  # Replace subspecies label
  mutate_if(is.character,
            str_replace_all, pattern = 'subsp.', replacement = 'ssp.') %>%
  # Join name adjudicated
  left_join(taxon_data, by = c('name_original' = 'taxon_name'), keep = TRUE) %>%
  rename(name_adjudicated = taxon_name) %>%
  # Correct erroneous data
  mutate(name_adjudicated = case_when(name_original == 'Betula × dugleana' ~ 'Betula cf. occidentalis',
                                      name_original == 'cryptogamic crust' ~ 'biotic soil crust',
                                      name_original == 'Dryas octopetala' ~ 'Dryas ajanensis ssp. beringensis',
                                      name_original == 'Vaccinium oxycoccos' ~ 'Oxycoccus microcarpus',
                                      name_original == 'fungi' ~ 'fungus',
                                      name_original == 'Dryas alaskansis' ~ 'Dryas alaskensis',
                                      name_original == 'Trichophorum cespitosum ssp. cespitosum' ~ 'Trichophorum cespitosum',
                                      TRUE ~ name_adjudicated)) %>%
  # Select final fields
  select(site_visit_id, name_original, name_adjudicated, cover_type, dead_status, cover_percent)

# Check if any problem taxa remain
problem_list = cover_data %>%
  filter(is.na(name_adjudicated)) %>%
  distinct(name_original)

# Parse abiotic cover
abiotic_data = abiotic_original

# Export new tables to csv
table_list = list(site_data, site_visit_data, cover_data, abiotic_data)
for (output in output_list) {
  csv_output = paste(data_folder, project_folder, paste(output, '.csv', sep = ''), sep = '/')
  export_table = table_list[match(output, output_list)]
  write.csv(export_table, file = csv_output, fileEncoding = 'UTF-8', row.names = FALSE)
}