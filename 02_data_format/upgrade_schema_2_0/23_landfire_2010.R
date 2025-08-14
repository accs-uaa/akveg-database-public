# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Parse Landfire Various 2010 upload from schema 1.0 to 2.0
# Author: Timm Nawrocki, Alaska Center for Conservation Science
# Last Updated: 2022-10-13
# Usage: Script should be executed in R 4.0.0+.
# Description: "Parse Landfire Various 2010 data from schema 1.0 to 2.0" updates the data from the 1.0 to 2.0 AKVEG Database schema format. New fields are added as necessary to match VTWG Minimum Standards per data tier and NODATA values are enforced. Removed the NPS Wrangell-St. Elias 2006 dataset because it is present in the ABR Various 2019 delivery.
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
project_folder = '23_landfire_various_2010'

# Define input data files
project_input = '01_project_landfire2010.xlsx'
site_input = 'landfire_2010_Site.xlsx'
cover_input = 'landfire_2010_Cover.xlsx'

# Define output data files
site_output = '02_site_landfire2010'
site_visit_output = '03_sitevisit_landfire2010'
cover_output = '05_vegetationcover_landfire2010'
output_list = c(site_output, site_visit_output, cover_output)

# Import required libraries
library(dplyr)
library(readr)
library(readxl)
library(stringr)
library(tibble)
library(tidyr)

# Read input files into data frames
project_original = read_excel(paste(data_folder, project_folder, 'schema_1_0', project_input, sep = '/'),
                              sheet = 'project')
site_original = read_excel(paste(data_folder, project_folder, 'schema_1_0', site_input, sep = '/')
                           , sheet = 'site')
cover_original = read_excel(paste(data_folder, project_folder, 'schema_1_0', cover_input, sep = '/')
                            , sheet = 'cover')

# Create list of project codes
project_codes = project_original %>%
  select(project_code) %>%
  mutate(project_og = case_when(project_code == 'nps_katmai_1998' ~ 'Katmai Bear',
                                project_code == 'blm_dalton_2002' ~ 'Dalton EC',
                                project_code == 'blm_galena_2000' ~ 'Galena EC',
                                project_code == 'blm_goodnews_2001' ~ 'Goodnews EC',
                                project_code == 'blm_gulkana_1997' ~ 'Gulkana EC',
                                project_code == 'blm_haines_2000' ~ 'Haines EC',
                                project_code == 'fws_innoko_1998' ~ 'Innoko EC',
                                project_code == 'fws_kanuti_1998' ~ 'Kanuti EC',
                                project_code == 'blm_kenai_1998' ~ 'Kenai EC',
                                project_code == 'blm_kvichak_2001' ~ 'Kvichak EC',
                                project_code == 'fws_koyukuk_2001' ~ 'Melozitna EC',
                                project_code == 'blm_naknek_2000' ~ 'Naknek EC',
                                project_code == 'blm_innoko_1999' ~ 'Northern Innoko EC',
                                project_code == 'blm_northernyukon_2000' ~ 'Northern Yukon EC',
                                project_code == 'blm_seward_2003' ~ 'Seward Peninsula EC',
                                project_code == 'blm_southernyukon_2000' ~ 'Southern Yukon EC',
                                project_code == 'blm_stony_1999' ~ 'Stoney River EC',
                                project_code == 'blm_susitna_1999' ~ 'Susitna EC',
                                project_code == 'blm_tanana_1995' ~ 'Tanana Flats EC',
                                project_code == 'fws_tetlin_2005' ~ 'Tetlin EC',
                                project_code == 'blm_tiekel_1998' ~ 'Tiekel EC',
                                project_code == 'fws_yukondelta_2006' ~ 'Yukon Delta EC',
                                project_code == 'usfs_stikine_2007' ~ 'Stikine EC',
                                project_code == 'nps_bering_2003' ~ 'Bering LC',
                                TRUE ~ project_code))

# Create list of sites with vegetation cover data
site_list = cover_original %>%
  # Select distinct vegetation observations
  distinct(site_code)

# Parse new site table
site_data = site_original %>%
  # Select sites that have vegetation cover observations
  inner_join(site_list, by = 'site_code') %>%
  # Rename fields
  rename(plot_dimensions_m = plot_dimensions,
         h_datum = datum,
         latitude_dd = latitude,
         longitude_dd = longitude,
         h_error_m = error) %>%
  # Assign no data values to empty fields
  mutate(positional_accuracy = 'image interpretation') %>%
  mutate(location_type = 'targeted') %>%
  # Correct project code
  left_join(project_codes, by = c('initial_project' = 'project_og')) %>%
  rename(establishing_project = project_code) %>%
  # Remove the NPS Wrangell 2006 Sites
  filter(initial_project != 'Wrangell LC') %>%
  # Select final fields
  select(site_code, establishing_project, perspective, cover_method, h_datum,
         latitude_dd, longitude_dd, h_error_m, positional_accuracy, plot_dimensions_m, location_type)

# Parse new site visit table
site_visit_data = cover_original %>%
  # Select distinct vegetation observations
  distinct(project, site_code, veg_observe_date, veg_observer, veg_recorder) %>%
  # Correct project code
  left_join(project_codes, by = c('project' = 'project_og')) %>%
  # Join site table
  left_join(site_original, by = 'site_code') %>%
  # Rename fields
  rename(observe_date = veg_observe_date) %>%
  # Assign no data value to empty fields
  mutate(env_observer = 'NULL') %>%
  mutate(soils_observer = 'NULL') %>%
  mutate(structural_class = 'n/assess') %>%
  mutate(homogenous = case_when(perspective == 'ground' ~ 'TRUE',
                                perspective == 'aerial' ~ 'FALSE',
                                TRUE ~ 'NULL')) %>%
  mutate(data_tier = 'map development & verification') %>%
  # Replace na values
  mutate(veg_recorder = replace_na(veg_recorder, 'NULL')) %>%
  # Correct errors
  mutate(scope_vascular = case_when(project_code == 'nps_bering_2003' ~ 'exhaustive',
                                    project_code == 'blm_dalton_2002' ~ 'top canopy',
                                    project_code == 'blm_galena_2000' ~ 'top canopy',
                                    project_code == 'blm_goodnews_2001' ~ 'top canopy',
                                    project_code == 'blm_gulkana_1997' ~ 'top canopy',
                                    project_code == 'blm_haines_2000' ~ 'top canopy',
                                    project_code == 'fws_innoko_1998' ~ 'top canopy',
                                    project_code == 'fws_kanuti_1998' ~ 'top canopy',
                                    project_code == 'nps_katmai_1998' ~ 'exhaustive',
                                    project_code == 'blm_kenai_1998' ~ 'top canopy',
                                    project_code == 'blm_kvichak_2001' ~ 'top canopy',
                                    project_code == 'fws_koyukuk_2001' ~ 'top canopy',
                                    project_code == 'blm_naknek_2000' ~ 'top canopy',
                                    project_code == 'blm_innoko_1999' ~ 'top canopy',
                                    project_code == 'blm_northernyukon_2000' ~ 'top canopy',
                                    project_code == 'blm_seward_2003' ~ 'top canopy',
                                    project_code == 'blm_southernyukon_2000' ~ 'top canopy',
                                    project_code == 'usfs_stikine_2007' ~ 'top canopy',
                                    project_code == 'blm_stony_1999' ~ 'top canopy',
                                    project_code == 'blm_susitna_1999' ~ 'top canopy',
                                    project_code == 'blm_tanana_1995' ~ 'top canopy',
                                    project_code == 'fws_tetlin_2005' ~ 'top canopy',
                                    project_code == 'blm_tiekel_1998' ~ 'top canopy',
                                    project_code == 'fws_yukondelta_2006' ~ 'top canopy',
                                    TRUE ~ 'ERROR')) %>%
  mutate(scope_bryophyte = case_when(project_code == 'nps_bering_2003' ~ 'non-trace species',
                                     project_code == 'blm_dalton_2002' ~ 'none',
                                     project_code == 'blm_galena_2000' ~ 'none',
                                     project_code == 'blm_goodnews_2001' ~ 'none',
                                     project_code == 'blm_gulkana_1997' ~ 'none',
                                     project_code == 'blm_haines_2000' ~ 'none',
                                     project_code == 'fws_innoko_1998' ~ 'none',
                                     project_code == 'fws_kanuti_1998' ~ 'none',
                                     project_code == 'nps_katmai_1998' ~ 'partial',
                                     project_code == 'blm_kenai_1998' ~ 'none',
                                     project_code == 'blm_kvichak_2001' ~ 'none',
                                     project_code == 'fws_koyukuk_2001' ~ 'none',
                                     project_code == 'blm_naknek_2000' ~ 'none',
                                     project_code == 'blm_innoko_1999' ~ 'none',
                                     project_code == 'blm_northernyukon_2000' ~ 'none',
                                     project_code == 'blm_seward_2003' ~ 'none',
                                     project_code == 'blm_southernyukon_2000' ~ 'none',
                                     project_code == 'usfs_stikine_2007' ~ 'none',
                                     project_code == 'blm_stony_1999' ~ 'none',
                                     project_code == 'blm_susitna_1999' ~ 'none',
                                     project_code == 'blm_tanana_1995' ~ 'partial',
                                     project_code == 'fws_tetlin_2005' ~ 'partial',
                                     project_code == 'blm_tiekel_1998' ~ 'none',
                                     project_code == 'fws_yukondelta_2006' ~ 'partial',
                                     TRUE ~ 'ERROR')) %>%
  mutate(scope_lichen = case_when(project_code == 'nps_bering_2003' ~ 'non-trace species',
                                  project_code == 'blm_dalton_2002' ~ 'none',
                                  project_code == 'blm_galena_2000' ~ 'none',
                                  project_code == 'blm_goodnews_2001' ~ 'none',
                                  project_code == 'blm_gulkana_1997' ~ 'none',
                                  project_code == 'blm_haines_2000' ~ 'none',
                                  project_code == 'fws_innoko_1998' ~ 'none',
                                  project_code == 'fws_kanuti_1998' ~ 'none',
                                  project_code == 'nps_katmai_1998' ~ 'partial',
                                  project_code == 'blm_kenai_1998' ~ 'none',
                                  project_code == 'blm_kvichak_2001' ~ 'none',
                                  project_code == 'fws_koyukuk_2001' ~ 'none',
                                  project_code == 'blm_naknek_2000' ~ 'none',
                                  project_code == 'blm_innoko_1999' ~ 'none',
                                  project_code == 'blm_northernyukon_2000' ~ 'none',
                                  project_code == 'blm_seward_2003' ~ 'none',
                                  project_code == 'blm_southernyukon_2000' ~ 'none',
                                  project_code == 'usfs_stikine_2007' ~ 'none',
                                  project_code == 'blm_stony_1999' ~ 'none',
                                  project_code == 'blm_susitna_1999' ~ 'none',
                                  project_code == 'blm_tanana_1995' ~ 'none',
                                  project_code == 'fws_tetlin_2005' ~ 'partial',
                                  project_code == 'blm_tiekel_1998' ~ 'none',
                                  project_code == 'fws_yukondelta_2006' ~ 'none',
                                  TRUE ~ 'ERROR')) %>%
  # Create site visit id
  mutate(site_visit_id = paste(site_code,
                               str_replace_all(observe_date, '-', ''),
                               sep = '_')) %>%
  # Remove the NPS Wrangell 2006 Sites
  filter(project != 'Wrangell LC') %>%
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
  # Join site visit information
  inner_join(site_visit_data, by = 'site_visit_id') %>%
  # Rename fields
  rename(cover_percent = cover) %>%
  # Assign no data value to empty fields
  mutate(dead_status = 'FALSE') %>%
  # Correct field values
  mutate(cover_type = case_when(project_code == 'nps_bering_2003' ~ 'absolute cover',
                                project_code == 'nps_katmai_1998' ~ 'absolute cover',
                                TRUE ~ 'top cover')) %>%
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