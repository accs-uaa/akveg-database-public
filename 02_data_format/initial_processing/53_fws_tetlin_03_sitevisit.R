# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Tetlin 2022-2024 site visit data for AKVEG Database
# Author: Timm Nawrocki, Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2026-01-16
# Usage: Must be executed in R version 4.5.1+.
# Description: "Format Tetlin 2022-2024 site visit data for AKVEG Database" formats site visit data for entry into AKVEG Database.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr, warn.conflicts = FALSE)
library(fs)
library(readr)
library(readxl)
library(stringr)
library(tidyr)

#### SET UP DIRECTORIES AND FILES
####------------------------------

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/53_fws_tetlin_2024')
template_folder = path(project_folder, 'Data/Data_Entry')
source_folder = path(plot_folder, 'source')

# Define input datasets
site_2022_input = path(source_folder, 'Data_2022', 'TNWR_2022_points_sampled_coordinates.csv')
site_2024_first_input = path(source_folder, 'Data_2024', 'v2', 'TetlinNWR_2024_sites_compiled.csv')
site_2024_second_input = path(source_folder, 'Data_2024', 'extra_sites', '03_fws_tetlin_2024_site_visit.xlsx')

# Define input templates
template_input = path(template_folder, '03_site_visit.xlsx')

# Define output dataset
visit_output = path(plot_folder, '03_sitevisit_fwstetlin2024.csv')

# Read template column names
visit_template = colnames(read_xlsx(path=template_input))

#### PARSE 2022 VISIT DATA
####------------------------------

visit_2022 = read_csv(site_2022_input) %>%
  # Create site code
  mutate(site_code = case_when(TransectID < 10 ~ paste('TET22_00', TransectID, sep = ''),
                               TransectID < 100 ~ paste('TET22_0', TransectID, sep = ''),
                               TRUE ~ paste('TET22_', TransectID, sep = ''))) %>%
  # Create site visit code
  mutate(site_visit_code = case_when(TransectID < 10 ~ paste('TET22_00', TransectID, '_20220715', sep = ''),
                                     TransectID < 100 ~ paste('TET22_0', TransectID, '_20220715', sep = ''),
                                     TRUE ~ paste('TET22_', TransectID, '_20220715', sep = ''))) %>%
  # Add site visit metadata
  mutate(project_code = 'fws_tetlin_2024',
         data_tier = 'map development & verification',
         observe_date = '2022-07-15',
         veg_observer = 'Brent Jamison',
         veg_recorder = 'Trent Gilmore',
         env_observer = 'none',
         soils_observer = 'none',
         scope_vascular = 'top canopy',
         scope_bryophyte = 'none',
         scope_lichen = 'none',
         structural_class = 'not assessed',
         homogeneous = 'TRUE') %>%
  # Select final columns
  select(all_of(visit_template))

#### PARSE 2024 VISIT DATA - FIRST INPUT
####------------------------------

visit_2024_first = read_csv(site_2024_first_input) %>%
  # Create site code
  mutate(site_code = str_replace(Site_code, '-', '_')) %>%
  # Create site visit code
  mutate(site_visit_code = paste(site_code, '_', Date, sep='')) %>%
  # Reformat date
  mutate(observe_date = paste(str_sub(Date, 1, 4), '-', str_sub(Date, 5, 6), '-', str_sub(Date, 7, 8), sep='')) %>%
  # Rename columns
  rename(veg_observer = Veg_observer,
         veg_recorder = Recorder,
         structural_class = Structural_class) %>%
  # Correct recorder
  mutate(veg_recorder = case_when(veg_recorder == 'Carrol Mahara' ~ 'Carol Mahara',
                                  TRUE ~ veg_recorder)) %>%
  # Correct structural class
  mutate(structural_class = str_to_lower(structural_class)) %>%
  mutate(structural_class = case_when(structural_class == 'dwarf needle forest' ~ 'dwarf needleleaf forest',
                                      structural_class == 'n/a' ~ 'low shrub',
                                      structural_class == 'unknown' ~ 'dwarf broadleaf forest',
                                      TRUE ~ structural_class)) %>%
  # Add site visit metadata
  mutate(project_code = 'fws_tetlin_2024',
         data_tier = 'map development & verification',
         env_observer = 'none',
         soils_observer = 'none',
         scope_vascular = 'top canopy',
         scope_bryophyte = 'none',
         scope_lichen = 'none',
         homogeneous = 'TRUE') %>%
  # Select final columns
  select(all_of(visit_template))

#### PARSE 2024 VISIT DATA - SECOND INPUT
####------------------------------
visit_2024_second = read_xlsx(site_2024_second_input) %>%
  # Create site code
  mutate(site_code = str_replace(site_code, '-', '_')) %>%
  # Create date string
  mutate(date_string = str_remove_all(observe_date, "-")) %>% 
  # Create site visit code
  mutate(site_visit_code = paste0(site_code, '_', date_string)) %>% 
  # Correct observer
  mutate(veg_observer = case_when(veg_observer == 'Hunter Gravely' ~ 'Hunter Gravley',
                                  .default = veg_observer)) %>%
  # Correct structural class
  mutate(structural_class = case_when(structural_class == 'se' ~ 'sedge emergent',
                                      structural_class == 'dnf' ~ 'dwarf needleleaf forest',
                                      structural_class == 'sm' ~ 'sedge meadow',
                                      .default = structural_class)) %>%
  # Add site visit metadata
  mutate(project_code = 'fws_tetlin_2024',
         homogeneous = 'TRUE') %>%
  # Select final columns
  select(all_of(visit_template))

#### MERGE AND EXPORT 2022 AND 2024 DATA
####------------------------------

# Merge data
visit_data = rbind(visit_2022, visit_2024_first, visit_2024_second) %>%
  filter(site_visit_code != 'TET24_036_20240728' & site_visit_code != 'TET22_540_20220715')

# Export data
write_csv(visit_data, visit_output)

# Clear workspace
rm(list=ls())
