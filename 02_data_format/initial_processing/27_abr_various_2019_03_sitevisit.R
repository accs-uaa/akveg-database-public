# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Visit Table for ABR Various 2019 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-09-25
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Site Visit Table for ABR Various 2019 data" formats information about site visits for ingestion into the AKVEG Database. The script formats dates, creates site visit codes, parses personnel names, populates required metadata, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(stringr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database', 'Data')
plot_folder = path(project_folder, 'Data_Plots', '27_abr_various_2019')
source_folder = path(plot_folder, 'source')
template_folder = path(project_folder, "Data_Entry")

# Define datasets ----

# Define input datasets
site_visit_input = path(source_folder, 'deliverable_tnawrocki_els.xlsx')
vegetation_input = path(source_folder, 'deliverable_tnawrocki_veg.xlsx')
site_input = path(plot_folder, '02_site_abrvarious2019.csv')
template_input = path(template_folder, "03_site_visit.xlsx")

# Define output dataset
site_visit_output = path(plot_folder, '03_sitevisit_abrvarious2019.csv')

# Read in data ----
site_visit_original = read_xlsx(site_visit_input)
vegetation_original = read_xlsx(vegetation_input)
site_original = read_csv(site_input)
template = colnames(read_xlsx(path=template_input))

# Format site code ----
# Capitalize all site codes for consistency
# Convert dashes to underscores
site_visit = site_visit_original %>% 
  mutate(site_code = str_to_upper(plot_id),
         site_code = str_replace_all(site_code, "-", "_"))

# Exclude 'problem' sites ----
# 2 sites without coordinates and 25 sites with sus cover data
site_visit = site_original %>% 
  select(site_code, establishing_project_code) %>% 
  left_join(site_visit, by='site_code')

# Ensure all sites in Site Visit table are in Site table
which(!(site_visit$site_code %in% site_original$site_code))

# Format date & site visit code ----

# Explore data
summary(as.Date(site_visit$env_field_start_ts))
hist(as.Date(site_visit$env_field_start_ts), 
     breaks = "year", 
     xlab = "Survey Date") 

# Explore sites with no data
area_list = site_visit %>% 
  filter(is.na(env_field_start_ts)) %>% 
  mutate(area_name = str_remove(site_code, '_\\d+$')) %>% 
  distinct(project_id, area_name) # All from nps_wrangell_2006 project

area_date_list = site_visit %>% 
  filter(!is.na(env_field_start_ts)) %>% 
  mutate(area_name = str_remove(site_code, '_\\d+$'),
         observe_date = as.Date(env_field_start_ts)) %>% 
  filter(area_name %in% area_list$area_name) %>% 
  distinct(area_name, observe_date) %>% 
  rename(recommended_date = observe_date)

# Assign dates to sites with missing data
site_visit = site_visit %>% 
  mutate(area_name = str_remove(site_code, '_\\d+$'),
         observe_date = as.Date(env_field_start_ts)) %>% 
  left_join(area_date_list, by = 'area_name') %>% 
  mutate(observe_date = case_when(is.na(observe_date) & !is.na(recommended_date) ~ recommended_date,
                                  is.na(observe_date) ~ as.Date('2006-07-09'), # Assign generic date within range of sampled sites in that area
                                  .default = observe_date))

# Ensure missing dates have been addressed
summary(as.Date(site_visit$observe_date))

site_visit %>% 
  filter(area_name %in% area_list$area_name) %>% 
  distinct(area_name, observe_date)

# Create site visit code
site_visit = site_visit %>% 
  mutate(date_string = str_replace_all(observe_date, "-", ""),
         site_visit_code = paste(site_code, date_string, sep = "_"))

# Format personnel names ----
# Join with vegetation data to get veg observer
site_visit = vegetation_original %>% 
  mutate(plot_id = str_remove(plot_id, "_\\d+$|-\\d+$")) %>% 
  select(plot_id, veg_observer_code) %>% 
  right_join(site_visit, by = 'plot_id')

unique(site_visit$veg_observer_code)
unique(site_visit$env_observer_code)

site_observers = site_visit %>% 
  mutate(veg_observer = case_when(veg_observer_code == 'pfl' ~ 'Patricia Loomis',
                                  str_starts(veg_observer_code, 'jer') ~ 'Joanna Roth',
                                  veg_observer_code == 'gvf' ~ 'Gerald Frost',
                                  veg_observer_code == 'wad' ~ 'Wendy Davis',
                                  veg_observer_code == 'mtj' ~ 'Torre Jorgenson',
                                  veg_observer_code == 'tcc' ~ 'Timothy Cater',
                                  veg_observer_code == 'erp' ~ 'Erik Pullman',
                                  veg_observer_code == 'msd' ~ 'Michael Duffy',
                                  veg_observer_code == 'tj' ~ 'Torre Jorgenson',
                                  str_starts(veg_observer_code, 'me') ~ 'Michael Emers', 
                                  str_starts(veg_observer_code, 'sfs') ~ 'Sharon Schlentner',
                                  veg_observer_code == 'tfl' ~ 'unknown',
                                  veg_observer_code == 'md' ~ 'Michael Duffy',
                                  veg_observer_code == 'elb' ~ 'unknown',
                                  veg_observer_code == 'afw' ~ 'Aaron Wells',
                                  veg_observer_code == 'nmt' ~ 'Naomi Tomco',
                                  veg_observer_code == 'mjm' ~ 'Matthew Macander',
                                  veg_observer_code == 'ees' ~ 'Emily Sousa',
                                  veg_observer_code == 'mkr' ~ 'unknown',
                                  veg_observer_code == 'maw' ~ 'unknown',
                                  veg_observer_code == 'ert' ~ 'unknown',
                                  veg_observer_code == 'kmp' ~ 'unknown',
                                  veg_observer_code == 'ekj' ~ 'Erin Johnson',
                                  veg_observer_code == 'lmf' ~ 'unknown',
                                  veg_observer_code == 'txc' ~ 'Tracy Christopherson',
                                  veg_observer_code == 'jgk' ~ 'Janet Kidd',
                                  veg_observer_code == 'sli' ~ 'Susan Ives',
                                  .default = 'error'
                                  ),
         env_observer = case_when(els_plot_type == 'Integrated Terrain Unit (ITU) mapping plot' ~ 'none',
                                  env_observer_code == 'mtj' ~ 'Torre Jorgenson',
                                  env_observer_code == 'jeg' ~ 'Jesse Grunblatt',
                                  env_observer_code == 'tcc' ~ 'Timothy Cater',
                                  env_observer_code == 'erp' ~ 'Erik Pullman',
                                  env_observer_code == 'cbh' ~ 'Chandra Heaton',
                                  str_starts(env_observer_code, 'jsm') ~ 'Jennifer Mitchell',
                                  env_observer_code == 'th mtj' ~ 'Torre Jorgenson',
                                  str_starts(env_observer_code, 'sfs') ~ 'Sharon Schlentner',
                                  env_observer_code == 'th sfs' ~ 'Sharon Schlentner',
                                  str_starts(env_observer_code, 'me') ~ 'Michael Emers',
                                  env_observer_code == 'th jer' ~ 'Joanna Roth',
                                  str_starts(env_observer_code, 'mtj') ~ 'Torre Jorgenson',
                                  env_observer_code == 'th me' ~ 'Michael Emers',
                                  str_starts(env_observer_code, 'jer') ~ 'Joanna Roth',
                                  env_observer_code == 'jmm' ~ 'unknown',
                                  env_observer_code == 'jg' ~ 'Jesse Grunblatt',
                                  env_observer_code == 'klr' ~ 'Kumi Rattenbury',
                                  env_observer_code == 'gvf' ~ 'Gerald Frost',
                                  env_observer_code == 'kr' ~ 'Kumi Rattenbury',
                                  env_observer_code == 'miller' ~ 'Eric Miller',
                                  env_observer_code == 'lba' ~ 'Lauren Attanas',
                                  env_observer_code == 'twl' ~ 'unknown',
                                  env_observer_code == 'ndk' ~ 'unknown',
                                  env_observer_code == 'eam' ~ 'Eric Miller',
                                  env_observer_code == 'mmw' ~ 'unknown',
                                  env_observer_code == 'msd' ~ 'Michael Duffy',
                                  env_observer_code == 'elb' ~ 'unknown',
                                  env_observer_code == 'mjm' ~ 'Matthew Macander',
                                  env_observer_code == 'txc' ~ 'Tracy Christopherson',
                                  env_observer_code == 'nmt' ~ 'Naomi Tomco',
                                  env_observer_code == 'afw' ~ 'Aaron Wells',
                                  env_observer_code == 'ksa' ~ 'unknown',
                                  env_observer_code == 'maw' ~ 'unknown',
                                  env_observer_code == 'lmf' ~ 'unknown',
                                  env_observer_code == 'ank' ~ 'unknown',
                                  env_observer_code == 'sli' ~ 'Susan Ives',
                                  env_observer_code == 'rwm' ~ 'Robert McNown',
                                  .default = 'error'
                                  ),
         veg_recorder = case_when(site_code == 'KATM_T37_04' ~ 'Aaron Wells',
                                  .default = env_observer),
         soils_observer = case_when(site_chemistry_calc == 'no data' & 
                                      (soil_sample_method == 'No Data' | soil_sample_method == 'Not Assessed' | is.na(soil_sample_method)) 
                                    & (soil_dom_texture_40cm == 'No Data' | soil_dom_texture_40cm == 'Not Assessed' | is.na(soil_dom_texture_40cm)) ~ 'none',
                                    els_plot_type == 'Aerial Plot' ~ 'none',
                                       .default = veg_observer))

# Ensure there are no 'error' flags
site_observers %>% filter(veg_observer == 'error' | env_observer == 'error')

# Format structural class ----
unique(site_observers$veg_structure_ecotype)

site_observers = site_observers %>% 
  mutate(structural_class = str_to_lower(veg_structure_ecotype))

# Format data tier
table(site_observers$els_plot_type)

site_observers = site_observers %>% 
  mutate(data_tier = case_when(els_plot_type == 'Standard ELS Plot or Soil Pit' ~ 'ecological land classification',
                               els_plot_type == 'Aerial Plot' | els_plot_type == 'Integrated Terrain Unit (ITU) mapping plot' ~ 'map development & verification',
                               .default = 'vegetation classification'))

# Populate remaining columns ----
site_visit_final = site_observers %>% 
  rename(project_code = establishing_project_code) %>% 
  mutate(scope_vascular = 'exhaustive',
         scope_bryophyte = 'common species',
         scope_lichen = 'common species',
         homogenous = 'TRUE') %>% 
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(site_visit_final, is.na)
    , sum))

# Export as CSV ----
write_csv(site_visit_final, site_visit_output)

# Clear workspace ----
rm(list=ls())
