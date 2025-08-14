# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Visit Table for Harvard University Alder Data"
# Author: Calvin Heslop, Harvard University
# Last Updated: 2024-07-08
# Usage: Must be executed in R version 4.4.0+.
# Description: "Format Site Visit Table for Harvard University Alder Data" formats site visit data collected and entered by Calvin Heslop for ingestion into the AKVEG Database. The script removes sites with missing cover data, creates site visit codes, formats dates and personnel names, populates fields with values that match constrained values in the AKVEG Database, and adds required fields where missing. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readxl)
library(sf)
library(stringr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/50_harvard_alder_2023')
template_folder = path(project_folder, "Data/Data_Entry")
source_folder = path(plot_folder, 'source')

# Define input dataset
site_visit_input = path(source_folder, 'Model_validation_points.shp')
template_input = path(template_folder, "03_site_visit.xlsx")

# Define output dataset
site_visit_output = path(plot_folder, '03_sitevisit_harvardalder2023.csv')

# Read in data ----
site_Heslop_original <- read_sf(site_visit_input)
template <- colnames(read_xlsx(path=template_input))

# Remove incomplete plots ----

# Remove rows with missing cover estimates
site_Heslop_original <- subset(site_Heslop_original, !is.na(alder_cove))

# Remove ArtPos sites (n=2)
site_Heslop_original = site_Heslop_original %>% 
  filter(!(grepl(pattern = "ArtPos", name)))

# Format data ----

# Fill in missing dates
# Seven sites with no dates, arbitrarily assign a date to them (I chose the last date of the survey season)
site_Heslop_original = site_Heslop_original %>% 
  mutate(time = case_when(is.na(time) ~ max(time, na.rm=TRUE),
                          .default = time))

# Populate fields
site_visit_heslop <- as.data.frame(site_Heslop_original) %>% 
  rename(site_code = 'name') %>% 
  mutate(observe_date = substr(time, start = 1, stop = 10) %>% 
           str_replace_all(pattern = '/', '-')) %>% 
  mutate(date_string = str_replace_all(observe_date, pattern = '-', ''),
         site_visit_code = paste0(site_code, '_',date_string),
         project_code = 'harvard_alder_2023',
         data_tier = 'map development & verification',
         veg_observer = ifelse(grepl('MV', site_code), 'Calvin Heslop', 'Nicholas Daley'),
         veg_recorder = veg_observer,
         env_observer = "NULL",
         soils_observer = "NULL",
         structural_class = 'tall shrub',
         scope_vascular = 'partial',
         scope_bryophyte = "none",
         scope_lichen = "none",
         homogenous = "TRUE"
         ) %>%
  select(all_of(template))
  
# QA/QC ----

# Ensure there aren't any null values that need to be addressed
cbind(
  lapply(
    lapply(site_visit_heslop, is.na)
    , sum)
)

# Verify values of categorical columns
unique(site_visit_heslop$veg_observer)
unique(site_visit_heslop$veg_recorder)
unique(site_visit_heslop$env_observer) # Should be NULL
unique(site_visit_heslop$soils_observer) # Should be NULL
unique(site_visit_heslop$structural_class)
unique(site_visit_heslop$scope_vascular)
unique(site_visit_heslop$scope_lichen) # Should be none
unique(site_visit_heslop$scope_bryophyte) # Should be none

# Export as CSV ----
write.csv(site_visit_heslop, site_visit_output,
          fileEncoding = "UTF-8",
          row.names = FALSE)

# Clear workspace ----
rm(list=ls())