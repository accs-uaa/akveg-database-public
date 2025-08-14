# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Parse data from Landfire 2016 Reference Database
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2022-10-09
# Usage: Script should be executed in R 4.0.0+.
# Description: "Parse data from Landfire 2016 Reference Database" parses data tables for project, site, site_visit, and vegetation cover upload to AKVEG Database (schema 2.0). Only the USFS Kenai 2019 dataset is included in the upload.
# ---------------------------------------------------------------------------

# Set root directory
drive = 'N:'
root_folder = 'ACCS_Work'

# Set repository directory
repository = 'C:/Users/timmn/Documents/Repositories/akveg-database'

# Define input folders
data_folder = paste(drive,
                    root_folder,
                    'Projects/VegetationEcology/AKVEG_Database/Data/Data_Plots',
                    sep = '/')

# Define project
project_folder = '24_landfire_various_2016'

# Define input data files
project_input = 'lutdtVisitsSourceID.xlsx'
site_input = 'dtPoints.xlsx'
site_visit_input = 'dtVisits.xlsx'
cover_input = 'dtSpecies.xlsx'

# Define output data files
project_output = '01_project_landfire2016'
site_output = '02_site_landfire2016'
site_visit_output = '03_sitevisit_landfire2016'
cover_output = '05_vegetationcover_landfire2016'
output_list = c(project_output, site_output, site_visit_output, cover_output)

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
akveg_connection = connect_database_postgresql(authentication)

# Create species name table
taxon_query = 'SELECT * FROM TAXON_ALL'
species_names = as_tibble(dbGetQuery(akveg_connection, taxon_query))

# Read input files into data frames
project_original = read_excel(paste(data_folder, project_folder, 'source', project_input, sep = '/'),
                              sheet = 'lutdtVisitsSourceID')
site_original = read_excel(paste(data_folder, project_folder, 'source', site_input, sep = '/')
                           , sheet = 'dtPoints')
site_visit_original = read_excel(paste(data_folder, project_folder, 'source', site_visit_input,
                                       sep = '/')
                           , sheet = 'dtVisits')
cover_original = read_excel(paste(data_folder, project_folder, 'source', cover_input, sep = '/')
                            , sheet = 'dtSpecies')

# Parse project data
project_data = project_original %>%
  filter(StateRegion == 'Alaska') %>%
  filter(SourceID == 'F00402') %>%
  inner_join(site_visit_original, by = 'SourceID') %>%
  inner_join(cover_original, by = 'EventID') %>%
  filter(YYYY > 0 & MM > 5 & MM < 10 & DD > 0) %>%
  distinct(SourceID, AgencyCd, DatasetCredit, Locality, DataDescription, Protocol) %>%
  mutate(project_code = case_when(SourceID == 'F00402' ~ 'usfs_kenai_2019',
                                  TRUE ~ 'ERROR')) %>%
  mutate(project_name = case_when(SourceID == 'F00402' ~ 'Kenai Vegetation Map',
                                  TRUE ~ 'ERROR')) %>%
  mutate(manager = case_when(SourceID == 'F00402' ~ 'Kim Homan',
                             TRUE ~ 'ERROR')) %>%
  mutate(funder = case_when(SourceID == 'F00402' ~ 'USFS',
                            TRUE ~ 'ERROR')) %>%
  mutate(originator = case_when(SourceID == 'F00402' ~ 'USFS',
                                TRUE ~ 'ERROR')) %>%
  mutate(completion = 'finished') %>%
  mutate(year_start = case_when(SourceID == 'F00402' ~ 2017,
                                TRUE ~ -999)) %>%
  mutate(year_end = case_when(SourceID == 'F00402' ~ 2019,
                              TRUE ~ -999)) %>%
  mutate(project_description = case_when(SourceID == 'F00402' ~ 'Aerial plots collected for the creation of a vegetation map of the Kenai Peninsula.',
                                         TRUE ~ 'ERROR')) %>%
  mutate(private = 'FALSE') %>%
  select(SourceID, project_code, project_name, originator, funder, manager, year_start, year_end,
         completion, project_description, private)

# Parse site data
site_data = site_original %>%
  inner_join(site_visit_original, by = 'EventID') %>%
  inner_join(project_data, by = 'SourceID') %>%
  distinct(EventID, SourceID, project_code, YYYY, Type, Lat, Long) %>%
  # Create site codes
  mutate(site_prefix = case_when(SourceID == 'F00402' ~ 'KENA',
                                 TRUE ~ 'ERROR')) %>%
  rowid_to_column(var = 'site_number') %>%
  mutate(site_code = case_when(site_number < 10 ~ paste(site_prefix, YYYY, '000', site_number,
                                                        sep = ''),
                               site_number < 100 ~ paste(site_prefix, YYYY, '00', site_number,
                                                         sep = ''),
                               site_number < 1000 ~ paste(site_prefix, YYYY, '0', site_number,
                                                          sep = ''),
                               site_number < 10000 ~ paste(site_prefix, YYYY, site_number,
                                                           sep = ''),
                               TRUE ~ 'ERROR')) %>%
  # Add metadata
  mutate(perspective = case_when(Type == 'aerial survey' | Type == 'helicopter survey' ~ 'aerial',
                                 Type == 'field visit' ~ 'ground',
                                 TRUE ~ 'ERROR')) %>%
  mutate(cover_method = case_when(SourceID == 'F00402' ~ 'semi-quantitative visual estimate',
                                  TRUE ~ 'ERROR')) %>%
  mutate(h_datum = 'NAD83') %>%
  mutate(h_error_m = -999) %>%
  mutate(positional_accuracy = case_when(perspective == 'aerial' ~ 'image interpretation',
                                         perspective == 'ground' ~ 'mapping grade GPS',
                                         TRUE ~ 'ERROR')) %>%
  mutate(plot_dimensions_m = 'unknown') %>%
  mutate(location_type = case_when(SourceID == 'F00402' ~ 'targeted',
                                   TRUE ~ 'ERROR')) %>%
  # Rename fields
  rename(latitude_dd = Lat,
         longitude_dd = Long,
         establishing_project = project_code) %>%
  # Select fields
  select(EventID, SourceID, site_code, establishing_project, perspective, cover_method,
         h_datum, latitude_dd, longitude_dd, h_error_m, positional_accuracy,
         plot_dimensions_m, location_type)

# Parse site visit data
site_visit_data = site_visit_original %>%
  inner_join(site_data, by = 'EventID') %>%
  inner_join(project_data, by = c('SourceID.x' = 'SourceID')) %>%
  mutate(observe_date = case_when(DD < 10 ~ paste(YYYY, '-0', MM, '-0', DD, sep = ''),
                                  DD >= 10 ~ paste(YYYY, '-0', MM, '-', DD, sep = ''),
                                  TRUE ~ 'ERROR')) %>%
  # Create site visit id
  mutate(site_visit_id = paste(site_code,
                               str_replace_all(observe_date, '-', ''),
                               sep = '_')) %>%
  mutate(data_tier = case_when(perspective == 'ground' ~ 'vegetation classification',
                               perspective == 'aerial' ~ 'map development & verification',
                               TRUE ~ 'ERROR')) %>%
  mutate(homogenous = 'FALSE') %>%
  mutate(veg_observer = 'unknown') %>%
  mutate(veg_recorder = 'unknown') %>%
  mutate(env_observer = 'NULL') %>%
  mutate(soils_observer = 'NULL') %>%
  mutate(structural_class = 'n/assess') %>%
  mutate(scope_vascular = 'top canopy') %>%
  mutate(scope_bryophyte = 'partial') %>%
  mutate(scope_lichen = 'none') %>%
  select(EventID, SourceID.x, site_visit_id, project_code, site_code, data_tier, observe_date,
         veg_observer, veg_recorder, env_observer, soils_observer, structural_class,
         scope_vascular, scope_bryophyte, scope_lichen, homogenous) %>%
  rename(SourceID = SourceID.x)

# Parse cover data
cover_data = cover_original %>%
  inner_join(site_visit_data, by = 'EventID') %>%
  rename(name_original = SciName,
         cover_percent = LFAbsCov) %>%
  mutate(cover_type = 'top cover') %>%
  mutate(dead_status = 'FALSE') %>%
  left_join(species_names, by = c('name_original' = 'taxon_name')) %>%
  left_join(species_names, by = 'taxon_code') %>%
  mutate(name_adjudicated = taxon_name) %>%
  mutate(name_adjudicated = case_when(name_original == 'Picea lutzii' ~ 'Picea ×lutzii',
                                      name_original == 'Unknown forb' ~ 'forb',
                                      name_original == 'Unknown graminoid' ~ 'graminoid',
                                      name_original == 'Unknown moss' ~ 'moss',
                                      name_original == 'Unknown shrub' ~ 'shrub',
                                      name_original == 'Unknown fern' ~ 'fern',
                                      name_original == 'Unknown lichen' ~ 'lichen',
                                      name_original == 'Argentina egedii ssp. egedii' ~ 'Argentina egedii',
                                      name_original == 'Polygonum bistorta' ~ 'Bistorta plumosa',
                                      name_original == 'Vaccinium oxycoccus' ~ 'Oxycoccus microcarpus',
                                      name_original == 'Unknown algae' ~ 'algae',
                                      name_original == 'Dryas octopetala' ~
                                        'Dryas ajanensis ssp. beringensis',
                                      name_original == 'Anthoxanthum monticola ssp. monticola' ~
                                        'Hierochloë alpina',
                                      name_original == 'Vaccinium oxycoccos' ~ 'Oxycoccus microcarpus',
                                      TRUE ~ name_adjudicated)) %>%
  filter(name_original != 'Unknown species' & name_original != 'Unknown tree') %>%
  select(project_code, site_visit_id, name_original, name_adjudicated,cover_type, dead_status, cover_percent)

# Create lists of site codes and site visits for which cover data exist
site_visit_list = site_visit_data %>%
  inner_join(cover_data, by = 'site_visit_id') %>%
  distinct(site_visit_id)
site_code_list = site_visit_data %>%
  inner_join(cover_data, by = 'site_visit_id') %>%
  distinct(site_code)

# Prepare final tables for output
project_data = project_data %>%
  select(-SourceID)
site_data = site_data %>%
  inner_join(site_code_list, by = 'site_code') %>%
  select(site_code, establishing_project, perspective, cover_method,
         h_datum, latitude_dd, longitude_dd, h_error_m, positional_accuracy,
         plot_dimensions_m, location_type)
site_visit_data = site_visit_data %>%
  inner_join(site_visit_list, by = 'site_visit_id') %>%
  select(site_visit_id, project_code, site_code, data_tier, observe_date,
         veg_observer, veg_recorder, env_observer, soils_observer, structural_class,
         scope_vascular, scope_bryophyte, scope_lichen, homogenous)
cover_data = cover_data %>%
  select(site_visit_id, name_original, name_adjudicated,cover_type, dead_status, cover_percent)

# Export new tables to csv
table_list = list(project_data, site_data, site_visit_data, cover_data)
for (output in output_list) {
  csv_output = paste(data_folder, project_folder, paste(output, '.csv', sep = ''), sep = '/')
  export_table = table_list[match(output, output_list)]
  write.csv(export_table, file = csv_output, fileEncoding = 'UTF-8', row.names = FALSE)
}