# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Parse NPS Glacier Bay 2001 upload from schema 1.0 to 2.0
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2022-10-13
# Usage: Script should be executed in R 4.0.0+.
# Description: "Parse NPS Glacier Bay 2001 data from schema 1.0 to 2.0" updates the data from the 1.0 to 2.0 AKVEG Database schema format. New fields are added as necessary to match VTWG Minimum Standards per data tier and NODATA values are enforced. Duplicate vegetation cover entries are aggregated as the sum.
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
project_folder = '20_nps_glacierbay_2001'

# Define input data files
site_input = 'npsGlacierBayLC_2001_Site.xlsx'
cover_input = 'npsGlacierBayLC_2001_Cover.xlsx'

# Define output data files
site_output = '02_site_npsglacierbay2001'
site_visit_output = '03_sitevisit_npsglacierbay2001'
cover_output = '05_vegetationcover_npsglacierbay2001'
output_list = c(site_output, site_visit_output, cover_output)

# Import required libraries
library(dplyr)
library(readr)
library(readxl)
library(stringr)
library(tibble)
library(tidyr)

# Read input files into data frames
site_original = read_excel(paste(data_folder, project_folder, 'schema_1_0', site_input, sep = '/')
                           , sheet = 'site')
cover_original = read_excel(paste(data_folder, project_folder, 'schema_1_0', cover_input, sep = '/')
                            , sheet = 'cover')

# Parse new site table
site_data = site_original %>%
  # Rename fields
  rename(establishing_project = initial_project,
         plot_dimensions_m = plot_dimensions,
         h_datum = datum,
         latitude_dd = latitude,
         longitude_dd = longitude,
         h_error_m = error) %>%
  # Assign no data values to empty fields
  mutate(positional_accuracy = case_when(perspective == 'ground' ~ 'consumer grade GPS',
                                         perspective == 'aerial' ~ 'image interpretation',
                                         TRUE ~ 'NULL')) %>%
  mutate(location_type = 'targeted') %>%
  # Correct project code
  mutate(establishing_project = 'nps_glacierbay_2001') %>%
  # Select final fields
  select(site_code, establishing_project, perspective, cover_method, h_datum,
         latitude_dd, longitude_dd, h_error_m, positional_accuracy, plot_dimensions_m, location_type)

# Parse new site visit table
site_visit_data = cover_original %>%
  # Select distinct vegetation observations
  distinct(site_code, veg_observe_date, veg_observer, veg_recorder) %>%
  # Join site and environment tables
  left_join(site_original, by = 'site_code') %>%
  # Rename fields
  rename(observe_date = veg_observe_date) %>%
  # Assign no data value to empty fields
  mutate(structural_class = 'n/assess') %>%
  mutate(homogenous = case_when(perspective == 'ground' ~ 'TRUE',
                                perspective == 'aerial' ~ 'FALSE',
                                TRUE ~ 'NULL')) %>%
  mutate(data_tier = case_when(perspective == 'ground' ~ 'legacy classification',
                               perspective == 'aerial' ~ 'map development & verification',
                               TRUE ~ 'NULL')) %>%
  mutate(env_observer = 'NULL') %>%
  mutate(soils_observer = 'NULL') %>%
  # Replace na values
  mutate(veg_recorder = replace_na(veg_recorder, 'NULL')) %>%
  # Correct erroneous fields
  mutate(scope_vascular = case_when(perspective == 'ground' ~ 'exhaustive',
                                    perspective == 'aerial' ~ 'top canopy',
                                    TRUE ~ 'NULL')) %>%
  mutate(scope_bryophyte = case_when(perspective == 'ground' ~ 'partial',
                                     perspective == 'aerial' ~ 'none',
                                     TRUE ~ 'NULL')) %>%
  mutate(scope_lichen = case_when(perspective == 'ground' ~ 'partial',
                                  perspective == 'aerial' ~ 'none',
                                  TRUE ~ 'NULL')) %>%
  # Correct project code
  mutate(project_code = 'nps_glacierbay_2001') %>%
  # Create site visit id
  mutate(site_visit_id = paste(site_code,
                               str_replace_all(observe_date, '-', ''),
                               sep = '_')) %>%
  # Select final fields
  select(site_visit_id, project_code, site_code, data_tier, observe_date,
         veg_observer, veg_recorder, env_observer, soils_observer, structural_class,
         scope_vascular, scope_bryophyte, scope_lichen, homogenous)

# Parse new cover table
cover_data = cover_original %>%
  # Create site visit id
  mutate(site_visit_id = paste(site_code,
                               str_replace_all(veg_observe_date, '-', ''),
                               sep = '_')) %>%
  # Rename fields
  rename(cover_percent = cover) %>%
  # Assign no data value to empty fields
  mutate(dead_status = 'FALSE') %>%
  # Correct field values
  mutate(cover_type = case_when(cover_type == 'total cover' ~ 'absolute cover',
                                cover_type == 'top cover' ~ 'top cover',
                                TRUE ~ 'NULL')) %>%
  mutate(name_original = str_to_sentence(name_original)) %>%
  # Correct name adjudicated
  mutate(name_adjudicated = case_when(name_adjudicated == 'Silene acaulis var. acaulis' ~ 'Silene acaulis',
                                      name_adjudicated == 'Forb' ~ 'forb',
                                      name_adjudicated == 'Astragalus alpinus var. alpinus' ~
                                        'Astragalus alpinus',
                                      name_adjudicated == 'Crustose Lichen' ~ 'crustose lichen',
                                      name_adjudicated == 'Liverwort' ~ 'liverwort',
                                      name_adjudicated == 'Moss' ~ 'moss',
                                      name_adjudicated == 'Algae' ~ 'algae',
                                      name_adjudicated == 'Fungus' ~ 'fungus',
                                      name_adjudicated == 'Graminoid' ~ 'graminoid',
                                      name_adjudicated == 'Astragalus alpinus ssp. alpinus' ~
                                        'Astragalus alpinus',
                                      name_adjudicated == 'Lichen' ~ 'lichen',
                                      name_adjudicated == 'Cryptobiotic Crust' ~ 'biotic soil crust',
                                      name_adjudicated == 'Foliose Lichen' ~ 'foliose lichen',
                                      name_adjudicated == 'Fruticose Lichen' ~ 'fruticose lichen',
                                      name_adjudicated == 'Eriophorum vaginatum var. vaginatum' ~
                                        'Eriophorum vaginatum',
                                      name_adjudicated == 'Parrya nudicaulis ssp. nudicaulis' ~
                                        'Parrya nudicaulis',
                                      name_adjudicated == 'Solidago multiradiata var. multiradiata' ~
                                        'Solidago multiradiata',
                                      name_adjudicated ==
                                        'Artemisia campestris ssp. borealis var. borealis' ~
                                        'Artemisia campestris ssp. borealis',
                                      name_adjudicated == 'Cyanobacteria' ~ 'cyanobacteria',
                                      name_adjudicated == 'Shrub' ~ 'shrub',
                                      name_adjudicated == 'Fern' ~ 'fern',
                                      name_adjudicated == 'Lycophyte' ~ 'lycophyte',
                                      name_adjudicated == 'Artemisia cana' ~ 'Artemisia',
                                      name_adjudicated == 'Eriophorum vaginatum ssp. vaginatum' ~
                                        'Eriophorum vaginatum',
                                      name_adjudicated == 'Vaccinium oxycoccos' ~ 'Oxycoccus microcarpus',
                                      TRUE ~ name_adjudicated)) %>%
  # Select final fields
  select(site_visit_id, name_original, name_adjudicated, cover_type, dead_status, cover_percent)

# Control for duplicate cover entries
final_cover = cover_data %>%
  distinct(site_visit_id, name_original, name_adjudicated, cover_type, dead_status, cover_percent) %>%
  group_by(site_visit_id, name_original) %>%
  summarize(cover_percent = sum(cover_percent))
cover_metadata = cover_data %>%
  distinct(site_visit_id, cover_type)
name_join = cover_data %>%
  distinct(name_original, name_adjudicated)
cover_join = final_cover %>%
  left_join(cover_metadata, by = 'site_visit_id') %>%
  left_join(name_join, by = 'name_original') %>%
  mutate(dead_status = 'FALSE') %>%
  # Select final fields
  select(site_visit_id, name_original, name_adjudicated, cover_type, dead_status, cover_percent)
  

# Export new tables to csv
table_list = list(site_data, site_visit_data, cover_join)
for (output in output_list) {
  csv_output = paste(data_folder, project_folder, paste(output, '.csv', sep = ''), sep = '/')
  export_table = table_list[match(output, output_list)]
  write.csv(export_table, file = csv_output, fileEncoding = 'UTF-8', row.names = FALSE)
}