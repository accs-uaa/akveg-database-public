# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Minor corrections to the ABR Various 2019 upload
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2022-10-13
# Usage: Script should be executed in R 4.0.0+.
# Description: "Minor corrections to the ABR Various 2019 upload" adds name_adjudicated, corrects some fields, updates project code, and enforces NODATA values on the ABR Various 2019 dataset. Unique rows are enforced.
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
project_folder = '27_abr_various_2019'

# Define input data files
project_input = '01_project_abrvarious2019.xlsx'
site_input = '02_site_abrvarious2019.xlsx'
site_visit_input = '03_sitevisit_abrvarious2019.xlsx'
cover_input = '05_vegetationcover_abrvarious2019.xlsx'
els_input = 'deliverable_tnawrocki_els.xlsx'

# Define output data files
site_output = '02_site_abrvarious2019'
site_visit_output = '03_sitevisit_abrvarious2019'
cover_output = '05_vegetationcover_abrvarious2019'
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
els_original = read_excel(paste(data_folder, project_folder, 'source', els_input, sep = '/'),
                          sheet = 'deliverable_tnawrocki_els')

# Create project code table
project_codes = site_visit_original %>%
  distinct(project_code) %>%
  mutate(code_new = case_when(project_code == 'nps_alagnak_els' ~ 'nps_alagnak_2014',
                              project_code == 'nps_aniakchak_els' ~ 'nps_aniakchak_2014',
                              project_code == 'nps_arcn_els' ~ 'nps_arcn_2008',
                              project_code == 'nps_katmai_els' ~ 'nps_katmai_2017',
                              project_code == 'shell_ones_habitat' ~ 'abr_ones_2017',
                              project_code == 'nps_kenai_fjords_els' ~ 'nps_kenai_2013',
                              project_code == 'nps_lake_clark_els' ~ 'nps_lakeclark_2011',
                              project_code == 'selawik_nwr_els' ~ 'fws_selawik_2008',
                              project_code == 'shell_ones_remote_sensing' ~ 'remove',
                              project_code == 'nps_wrangell_st_elias_els' ~ 'nps_wrangell_2006',
                              project_code == 'nps_cakn_permafrost' ~ 'remove'))

# Parse new site table
site_data = site_original %>%
  # Join site visit data
  inner_join(site_visit_original, by = 'site_code') %>%
  # Join ELS data
  left_join(els_original, by = c('site_code' = 'plot_id')) %>%
  # Join corrected project codes
  left_join(project_codes, by = 'project_code') %>%
  # Add missing data
  mutate(perspective = case_when(els_plot_type == 'Aerial Plot' ~ 'aerial',
                                 TRUE ~ 'ground')) %>%
  # Correct erroneous data
  mutate(plot_dimensions_m = case_when(plot_dimensions == '1' ~ '1 radius',
                                       plot_dimensions_m == '5' ~ '5 radius',
                                       plot_dimensions_m == '10' ~ '10 radius',
                                       plot_dimensions_m == '15' ~ '15 radius',
                                       plot_dimensions_m == '20' ~ '20 radius',
                                       plot_dimensions_m == '23' ~ '23 radius',
                                       plot_dimensions_m == 'unknown' ~ '10 radius',
                                       TRUE ~ plot_dimensions_m)) %>%
  mutate(positional_accuracy = case_when(code_new == 'abr_ones_2017' ~ 'consumer grade GPS',
                                         code_new == 'nps_alagnak_2014' ~ 'consumer grade GPS',
                                         code_new == 'fws_selawik_2008' ~ 'consumer grade GPS',
                                         code_new == 'nps_aniakchak_2014' ~ 'consumer grade GPS',
                                         code_new == 'nps_arcn_2008' ~ 'consumer grade GPS',
                                         code_new == 'nps_kenai_2013' ~ 'mapping grade GPS',
                                         code_new == 'nps_lakeclark_2011' ~ 'consumer grade GPS',
                                         code_new == 'nps_wrangell_2006' ~ 'consumer grade GPS',
                                         code_new == 'nps_katmai_2017' ~ 'consumer grade GPS',
                                         TRUE ~ positional_accuracy)) %>%
  mutate(h_error_m = case_when(code_new == 'abr_ones_2017' ~ 3,
                               code_new == 'nps_alagnak_2014' ~ 3,
                               code_new == 'fws_selawik_2008' ~ 15,
                               code_new == 'nps_aniakchak_2014' ~ 3,
                               code_new == 'nps_arcn_2008' ~ 15,
                               code_new == 'nps_katmai_2017' ~ 3,
                               code_new == 'nps_kenai_2013' ~ 1,
                               code_new == 'nps_lakeclark_2011' ~ 5,
                               code_new == 'nps_wrangell_2006' ~ 15,
                               TRUE ~ -999)) %>%
  mutate(location_type = case_when(code_new == 'abr_ones_2017' ~ 'targeted',
                                   code_new == 'fws_selawik_2008' ~ 'targeted',
                                   code_new == 'nps_alagnak_2014' ~ 'targeted',
                                   code_new == 'nps_aniakchak_2014' ~ 'targeted',
                                   code_new == 'nps_arcn_2008' ~ 'targeted',
                                   code_new == 'nps_kenai_2013' ~ 'targeted',
                                   code_new == 'nps_lakeclark_2011' ~ 'targeted',
                                   code_new == 'nps_wrangell_2006' ~ 'targeted',
                                   code_new == 'nps_katmai_2017' ~ 'targeted',
                                   TRUE ~ location_type)) %>%
  rename(establishing_project = code_new) %>%
  # Select final fields
  distinct(site_code, establishing_project, perspective, cover_method, h_datum,
           latitude_dd, longitude_dd, h_error_m, positional_accuracy, plot_dimensions_m, location_type) %>%
  # Filter out remove projects
  filter(establishing_project != 'remove') %>%
  # Select final fields
  select(site_code, establishing_project, perspective, cover_method, h_datum,
         latitude_dd, longitude_dd, h_error_m, positional_accuracy, plot_dimensions_m, location_type)

# Parse new site visit table
site_visit_data = site_visit_original %>%
  # Join site data
  inner_join(site_original, by = 'site_code') %>%
  # Join corrected project codes
  left_join(project_codes, by = 'project_code') %>%
  # Join ELS data
  left_join(els_original, by = c('site_code' = 'plot_id')) %>%
  # Correct erroneous data
  mutate(data_tier = case_when(els_plot_type == 'Aerial Plot' ~ 'map development & verification',
                               els_plot_type == 'Integrated Terrain Unit (ITU) mapping plot' ~ 'map development & verification',
                               els_plot_type == 'Rapid Verification Plot or Soil Pit' ~ 'map development & verification',
                               els_plot_type == 'Standard ELS Plot or Soil Pit' ~ 'ecological land classification',
                               els_plot_type == 'veg subjective' ~ 'map development & verification',
                               els_plot_type == 'veg systematic' ~ 'map development & verification',
                               TRUE ~ 'ERROR')) %>%
  mutate(scope_vascular = case_when(els_plot_type == 'Aerial Plot' ~ 'top canopy',
                                     TRUE ~ scope_vascular)) %>%
  mutate(scope_bryophyte = case_when(els_plot_type == 'Aerial Plot' ~ 'none',
                                      TRUE ~ scope_bryophyte)) %>%
  mutate(scope_lichen = case_when(els_plot_type == 'Aerial Plot' ~ 'none',
                                   TRUE ~ scope_lichen)) %>%
  mutate(homogenous = case_when(els_plot_type == 'Aerial Plot' ~ 'FALSE',
                                TRUE ~ 'TRUE')) %>%
  # Replace NA values
  mutate(veg_recorder = replace_na(veg_recorder, 'NULL')) %>%
  mutate(env_observer = replace_na(env_observer, 'NULL')) %>%
  mutate(soils_observer = replace_na(soils_observer, 'NULL')) %>%
  # Filter out remove projects
  filter(code_new != 'remove') %>%
  # Select final fields
  distinct(site_visit_id, code_new, site_code, data_tier, observe_date,
           veg_observer, veg_recorder, env_observer, soils_observer, structural_class,
           scope_vascular, scope_bryophyte, scope_lichen, homogenous) %>%
  # Rename project code
  rename(project_code = code_new) %>%
  # Select final fields
  select(site_visit_id, project_code, site_code, data_tier, observe_date,
         veg_observer, veg_recorder, env_observer, soils_observer, structural_class,
         scope_vascular, scope_bryophyte, scope_lichen, homogenous)
  

# Parse new cover table
cover_data = cover_original %>%
  # Join name adjudicated
  left_join(taxon_data, by = c('name_original' = 'taxon_name'), keep = TRUE) %>%
  rename(name_adjudicated = taxon_name) %>%
  # Correct adjudicated names
  mutate(name_adjudicated = case_when(name_original == 'Fungus' ~ 'fungus',
                                      name_original == 'Lichen' ~ 'lichen',
                                      name_original == 'Crustose Lichen' ~ 'crustose lichen',
                                      name_original == 'Forb' ~ 'forb',
                                      name_original == 'Moss' ~ 'moss',
                                      name_original == 'Foliose Lichen' ~ 'foliose lichen',
                                      name_original == 'Liverwort' ~ 'liverwort',
                                      name_original == 'Graminoid' ~ 'graminoid',
                                      name_original == 'Algae' ~ 'algae',
                                      name_original == 'Fern' ~ 'fern',
                                      name_original == 'Cryptobiotic Crust' ~ 'biotic soil crust',
                                      name_original == 'Fruticose Lichen' ~ 'fruticose lichen',
                                      name_original == 'Dwarf Shrub' ~ 'shrub dwarf',
                                      name_original == 'Parrya nudicaulis ssp. nudicaulis' ~ 'Parrya nudicaulis',
                                      name_original == 'Solidago multiradiata ssp. multiradiata' ~ 'Solidago multiradiata',
                                      TRUE ~ name_adjudicated)) %>%
  # Limit observations to site visits
  inner_join(site_visit_data, by = 'site_visit_id') %>%
  # Select final fields
  select(site_visit_id, name_original, name_adjudicated, cover_type, dead_status, cover_percent) %>%
  filter(name_original != 'NA')

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