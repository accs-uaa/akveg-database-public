# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Parse FWS Interior 2014 upload from schema 1.0 to 2.0
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2022-10-13
# Usage: Script should be executed in R 4.0.0+.
# Description: "Parse FWS Interior 2014 data from schema 1.0 to 2.0" updates the data from the 1.0 to 2.0 AKVEG Database schema format. New fields are added as necessary to match VTWG Minimum Standards per data tier and NODATA values are enforced.
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
project_folder = '11_fws_interior_2014'

# Define input data files
site_input = 'usfwsInterior_2014_Site.xlsx'
cover_input = 'usfwsInterior_2014_Cover.xlsx'

# Define output data files
site_output = '02_site_fwsinterior2014'
site_visit_output = '03_sitevisit_fwsinterior2014'
cover_output = '05_vegetationcover_fwsinterior2014'
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

# Create list of sites with vegetation cover data
site_list = cover_original %>%
  # Select distinct vegetation observations
  distinct(site_code)

# Parse new site table
site_data = site_original %>%
  # Select sites that have vegetation cover observations
  inner_join(site_list, by = 'site_code') %>%
  # Rename fields
  rename(establishing_project = initial_project,
         plot_dimensions_m = plot_dimensions,
         h_datum = datum,
         latitude_dd = latitude,
         longitude_dd = longitude,
         h_error_m = error) %>%
  # Assign no data values to empty fields
  mutate(positional_accuracy = 'consumer grade GPS') %>%
  mutate(location_type = 'targeted') %>%
  # Correct project code
  mutate(establishing_project = 'fws_interior_2014') %>%
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
  mutate(homogenous = 'TRUE') %>%
  mutate(data_tier = 'legacy classification') %>%
  mutate(env_observer = 'NULL') %>%
  mutate(soils_observer = 'NULL') %>%
  # Correct erroneous values
  mutate(project_code = 'fws_interior_2014') %>%
  mutate(scope_vascular = 'exhaustive') %>%
  mutate(scope_bryophyte = 'none') %>%
  mutate(scope_lichen = 'none') %>%
  # Replace na values
  mutate(veg_recorder = replace_na(veg_recorder, 'NULL')) %>%
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
  mutate(cover_type = 'absolute cover') %>%
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

# Export new tables to csv
table_list = list(site_data, site_visit_data, cover_data)
for (output in output_list) {
  csv_output = paste(data_folder, project_folder, paste(output, '.csv', sep = ''), sep = '/')
  export_table = table_list[match(output, output_list)]
  write.csv(export_table, file = csv_output, fileEncoding = 'UTF-8', row.names = FALSE)
}