# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Visit Table for USFWS St Matthew 2018 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-24
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Site Visit Table for USFWS St Matthew 2018 data" formats information about site visits for ingestion into the AKVEG Database. The script formats dates, creates site visit codes, parses personnel names, formats structural class data, and populates required columns. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/39_fws_stmatthew_2018')
source_folder = path(plot_folder, 'source', 'FieldData', 'Originals')
template_folder = path(project_folder, "Data/Data_Entry")

# Define datasets ----

# Define input datasets
site_visit_input = path(source_folder, 'stmatthew_data_veg_structure.csv')
envr_input = path(source_folder, 'stmatthew_data_environment.csv')
site_input = path(plot_folder, "02_site_fwsstmatthew2018.csv")
template_input = path(template_folder, "03_site_visit.xlsx")

# Define output dataset
site_visit_output = path(plot_folder, '03_sitevisit_fwsstmatthew2018.csv')

# Read in data ----
site_visit_original = read_csv(site_visit_input)
site_original = read_csv(site_input)
envr_original = read_csv(envr_input)
template = colnames(read_xlsx(path=template_input))

# Format site code ----
# Drop year from site code
site_visit_data = site_visit_original %>% 
  mutate(site_code = str_remove(plot_id, "_2018"))

# Exclude sites without cover data ----
# As well as specimen collection plots
site_visit_data = site_original %>% 
  select(site_code) %>% 
  left_join(site_visit_data, by="site_code")

# Ensure all sites in Site table are in Site Visit table
which(!(site_original$site_code %in% site_visit_data$site_code))

# Format date & site visit code ----

# Ensure that date range is reasonable
hist(as.Date(site_visit_data$veg_field_start_ts), breaks = "day", xlab = "Survey Date") 
summary(as.Date(site_visit_data$veg_field_start_ts))

site_visit_data = site_visit_data %>% 
  mutate(observe_date = as.Date(veg_field_start_ts),
         date_string = str_replace_all(observe_date, "-", ""),
         site_visit_code = paste(site_code, date_string, sep = "_"))

# Format personnel names ----
unique(site_visit_data$observer) # Only one name

site_visit_data = site_visit_data %>% 
  mutate(veg_observer = "Aaron Wells",
         veg_recorder = "Aaron Wells",
         env_observer = "Aaron Wells",
         soils_observer = "none")

# Ensure values were correctly translated
unique(site_visit_data$veg_observer) # Should only be 1 name
unique(site_visit_data$veg_recorder) # Should only be 1 name
unique(site_visit_data$env_observer) # Should only be 1 name

# Format structural class ----

# Begin by joining with environment data, which contains a 'structural class' column that uses AKVEG constrained values
site_visit_class = envr_original %>% 
  select(plot_id, vegetation_structure, env_field_note) %>%
  mutate(site_code = str_remove(plot_id, "_2018")) %>% 
  right_join(site_visit_data, by = "site_code")

unique(site_visit_class$vegetation_structure)
site_visit_class %>% filter(is.na(vegetation_structure)) # 2 sites w/o vegetation structure data; use structural group cover data and Viereck Level 4 to fill in

site_visit_class = site_visit_class %>% 
  mutate(structural_class = str_to_lower(vegetation_structure),
         structural_class = case_when(site_code == "stmatt_B09" ~ "lichen tundra",
                                      site_code == "stmatt_D16" ~ "sedge emergent",
                                      .default = structural_class))

# Format homogeneous column ----
# Assume sites are homogeneous unless field notes column states otherwise
site_visit_class = site_visit_class %>% 
  mutate(homogenous = case_when(site_code == "stmatt_D04" ~ "FALSE",
                                site_code == "stmatt_G03" ~ "FALSE",
                                .default = "TRUE"))

# Populate remaining columns ----
site_visit_final = site_visit_class %>% 
  mutate(project_code = "fws_stmatthew_2018",
         data_tier = "map development & verification",
         scope_vascular = "exhaustive",
         scope_bryophyte = "category", # Glance at cover data suggests moss identification limited to Sphagnum vs non-Sphagnum
         scope_lichen = "none") %>% 
  select(all_of(template))

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
table(site_visit_final$homogenous) # Should only be 2 FALSE

# Export as CSV ----
write_csv(site_visit_final, site_visit_output)

# Clear workspace ----
rm(list=ls())