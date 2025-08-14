# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Visit Table for ABR 2019 Arctic Refuge data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-04-26
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Site Visit Table for ABR 2019 Arctic Refuge data" formats information about site visits for ingestion into the AKVEG Database. The script formats dates, creates site visit codes, parses personnel names, re-classifies structural class data, populates required metadata, and performs QA/QC checks. The script also combined data from aerial and ground surveys into a single dataset. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = path(project_folder, 'Data', 'Data_Plots', '29_abr_arcticrefuge_2019')
source_folder = path(plot_folder, 'source')
reference_folder = path(project_folder, 'References')
template_folder = path(project_folder, 'Data', "Data_Entry")

# Define datasets ----

# Define input datasets
cover_aerial_input = path(source_folder, "abr_anwr_ns_lc_veg_aerial_deliverable_part2_wider.csv")
cover_ground_input = path(source_folder, "abr_anwr_ns_lc_veg_deliverable.csv")
els_input = path(source_folder, "abr_anwr_ns_lc_els_deliverable.csv")
structural_ref_input = path(reference_folder, 'structural_class_ref.xlsx')
template_input = path(template_folder, "03_site_visit.xlsx")
site_input = path(plot_folder, "02_site_abrarcticrefuge2019.csv")

# Define output dataset
site_visit_output = path(plot_folder,"03_sitevisit_abrarcticrefuge2019.csv")

# Read in data ----
cover_aerial_original = read_csv(cover_aerial_input)
cover_ground_original = read_csv(cover_ground_input)
els_original = read_csv(els_input)
structural_ref = read_xlsx(structural_ref_input, sheet="viereck_level_4")
template = colnames(read_xlsx(path=template_input))
site_original = read_csv(site_input)

# Format site data ----
site_data = site_original %>% 
  mutate(original_code = str_c(site_code,"2019",sep="_"))

# Format aerial surveys ----
# Remove plots for which observer & timestamp are blank. These plots do not have accompanying cover data.
cover_aerial = cover_aerial_original %>% 
  filter(plot_id %in% site_data$original_code & !is.na(veg_aerial_observer)) %>% 
  select(plot_id, veg_aerial_observer, veg_aerial_field_start_ts, veg_field_viereck_4) %>% 
  distinct(plot_id, .keep_all=TRUE) %>% 
  mutate(observe_date = as.Date(veg_aerial_field_start_ts))

# Create date string and site visit ID
cover_aerial = cover_aerial %>% 
  mutate(date_string = str_remove_all(observe_date, pattern="-"),
         site_code = str_remove_all(plot_id, pattern="_2019"),
         site_visit_code = str_c(site_code, date_string, sep="_"))

# Convert Viereck Level IV to appropriate structural class
cover_aerial = cover_aerial %>% 
  left_join(structural_ref, 
            by=c("veg_field_viereck_4" = "viereck_level_4"))

cover_aerial %>% filter(is.na(structural_class))

# Final formatting
cover_aerial_final = cover_aerial %>% 
  mutate(project_code = "abr_arcticrefuge_2019",
         scope_vascular = "top canopy",
         scope_bryophyte = "partial",
         scope_lichen = "partial",
         data_tier = "map development & verification",
         veg_observer = "Matthew Macander",
         veg_recorder = "Robert McNown",
         env_observer = "none",
         soils_observer = "none",
         homogeneous = TRUE) %>%
  select(all_of(template))

# Format ELS data ----
els_data = els_original %>% 
  filter(plot_id %in% site_data$original_code & !is.na(env_observer_code)) %>% 
  select(plot_id, env_observer_code)

# Format ground surveys ----
cover_ground = cover_ground_original %>% 
  filter(plot_id %in% site_data$original_code) %>% 
  select(plot_id, veg_observer_code, veg_field_start_ts, veg_viereck_4, veg_completeness) %>% 
  mutate(observe_date = as.Date(veg_field_start_ts))

# Create date string and site visit ID
cover_ground = cover_ground %>% 
  mutate(date_string = str_remove_all(observe_date, pattern="-"),
         site_code = str_remove_all(plot_id,pattern="_2019"),
         site_visit_code = str_c(site_code,
                                 date_string,sep="_"))

# Format observer names 
cover_ground = cover_ground %>% 
  left_join(els_data,by="plot_id") %>% # Join with ELS data to get environmental observer
  mutate(veg_observer = case_when(veg_observer_code == 'afw' ~ 'Aaron Wells',
                                  .default = 'unknown'),
         env_observer = case_when(env_observer_code == 'afw' ~ 'Aaron Wells',
                                  env_observer_code == 'txc' ~ 'Tracy Christopherson',
                                  .default = 'unknown'),
         veg_recorder = 'Tracy Christopherson',
         soils_observer = env_observer)

# Join with structural class reclassification
# Manually classify one site
cover_ground = cover_ground %>% 
  left_join(structural_ref, by=c("veg_viereck_4" = "viereck_level_4")) %>% 
  mutate(structural_class = case_when(veg_viereck_4 == 'Elymus' ~ 'Grass Meadow',
                                      .default = structural_class))

cover_ground %>% filter(is.na(structural_class))

# Final formatting
cover_ground_final = cover_ground %>% 
  mutate(project_code = "abr_arcticrefuge_2019",
         scope_vascular = "exhaustive",
         scope_bryophyte = "partial",
         scope_lichen = "partial",
         data_tier = "ecological land classification",
         homogeneous = TRUE) %>%
  select(all_of(template))

# Combine ground and aerial datasets ----
site_visit_final = cover_aerial_final %>% 
  bind_rows(cover_ground_final) %>% 
  arrange(site_visit_code)

# Ensure date range is reasonable
hist(site_visit_final$observe_date, 
     breaks = "day", 
     xlab = "Survey Date") 
summary(as.Date(site_visit_final$observe_date))

# Convert date to string
site_visit_final = site_visit_final %>% 
  mutate(observe_date = as.character.Date(observe_date))
unique(site_visit_final$observe_date)

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(site_visit_final, is.na)
    , sum))

# Verify personnel names
unique(site_visit_final$veg_observer)
unique(site_visit_final$veg_recorder)
unique(site_visit_final$env_observer)
unique(site_visit_final$soils_observer)

# Verify that all structural class values match a constrained value
table(site_visit_final$structural_class)

# Verify homogeneous values
table(site_visit_final$homogeneous)

# Ensure all sites in Site table are in Site Visit table
which(!(site_original$site_code %in% site_visit_final$site_code))
which(!(site_visit_final$site_code %in% site_original$site_code))

# Export data ----
write_csv(site_visit_final, site_visit_output)

# Clear workspace ----
rm(list=ls())
