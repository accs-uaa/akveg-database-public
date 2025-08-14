# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Visit Table for DATASET_NAME data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: CURRENT_DATE
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Site Visit Table for DATASET_NAME data" formats information about site visits for ingestion into the AKVEG Database. The script formats dates, creates site visit codes, parses personnel names, re-classifies structural class data, populates required metadata, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
plot_folder = plot_folder_path
template_folder = path(project_folder, "Data", "Data_Entry")

# Define datasets ----

# Define input datasets
visit_input = site_visit_path
site_input = path(plot_folder, paste0("02_site_", code_for_output, ".csv"))
template_input = path(template_folder, "03_site_visit.xlsx")

# Define output dataset
visit_output = path(plot_folder, paste0("03_sitevisit_", code_for_output, ".csv"))

# Read in data ----
visit_original = read_csv(visit_input)
site_original = read_csv(site_input)
template = colnames(read_xlsx(path=template_input))

# Format site code ----

# Exclude sites without cover data ----

# Ensure all sites in Site table are in Site Visit table
which(!(site_original$site_code %in% visit_data$site_code))

# Format date & site visit code ----

# Format date
visit_data = visit_data %>% 
  mutate(observe_date = as.Date(visit_data$date_column_name))

# Ensure date range is reasonable
hist(as.Date(visit_data$observe_date), 
     breaks = "day", 
     xlab = "Survey Date") 
summary(as.Date(visit_data$observe_date))

# Create site visit code and convert date to string
visit_data = visit_data %>% 
  mutate(date_string = str_remove_all(observe_date, "-"),
         site_visit_code = str_c(site_code, date_string, sep = "_"),
         observe_date = as.character.Date(observe_date))

# Verify results
head(visit_data$observe_date)
head(visit_data$site_visit_code)

# Format personnel names ----
unique(visit_data$observer)

visit_data = visit_data %>% 
  mutate(veg_observer = "your name here",
         veg_recorder = "your name here",
         env_observer = "your name here",
         soils_observer = "none")

# Format structural class ----
visit_class = visit_data

# Populate remaining columns ----
visit_final = visit_class %>% 
  mutate(project_code = code_name,
         data_tier = "map development & verification",
         scope_vascular = "exhaustive",
         scope_bryophyte = "common",
         scope_lichen = "common",
         homogeneous = "TRUE") %>% 
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(visit_final, is.na)
    , sum))

# Verify personnel names
unique(visit_final$veg_observer)
unique(visit_final$veg_recorder)
unique(visit_final$env_observer)
unique(visit_final$soils_observer)

# Verify that all structural class values match a constrained value
table(visit_final$structural_class)

# Verify homogeneous values
table(visit_final$homogeneous)

# Export as CSV ----
write_csv(visit_final, visit_output)

# Clear workspace ----
rm(list=ls())

# Did you remember to update the script header?
