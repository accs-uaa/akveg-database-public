# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Visit Table for USFWS Unalaska 2007 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-26
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Site Visit Table for USFWS Unalaska 2007 data" formats information about site visits for ingestion into the AKVEG Database. The script formats dates, creates site visit codes, parses personnel names, re-classifies structural class data, populates required metadata, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
plot_folder = 'C:/ACCS_Work/OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database/Data/Data_Plots/40_fws_unalaska_2007'
template_folder = path(project_folder, "Data/Data_Entry")

# Define datasets ----

# Define input datasets
site_visit_input = 'C:/ACCS_Work/OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database/Data/Data_Plots/40_fws_unalaska_2007/source/modified_source_data/aava_unalaska_stalbot_2010_allenv_modrc.xlsx'
site_input = path(plot_folder, paste0("02_site_", 'fwsunalaska2007', ".csv"))
template_input = path(template_folder, "03_site_visit.xlsx")

# Define output dataset
site_visit_output = path(plot_folder, paste0("03_sitevisit_", 'fwsunalaska2007', ".csv"))

# Read in data ----
site_visit_original = read_xlsx(site_visit_input, skip = 6)
site_original = read_csv(site_input)
template = colnames(read_xlsx(path=template_input))

# Format site code ----
site_visit_data = site_visit_original %>% 
  mutate(site_code = str_c("FWS", `FIELD RELEVE NUMBER (PROVIDED BY AUTHOR)`, sep = "_"))

# Ensure all sites in Site table are in Site Visit table
which(!(site_original$site_code %in% site_visit_data$site_code))

# Format date & site visit code ----

# Create site visit code
site_visit_data = site_visit_data %>% 
  mutate(site_visit_code = paste(site_code, `SITE VISIT DATE`, sep = "_"))

# Convert date string to date
# Solution from user agstudy: https://stackoverflow.com/questions/41655171/what-is-an-elegant-way-to-add-dashes-in-number-strings
site_visit_data = site_visit_data %>% 
  mutate(observe_date = str_replace(`SITE VISIT DATE`,"(\\d{4})(\\d{2})(\\d{2})$",
                                    "\\1-\\2-\\3"))

# Ensure that date range is reasonable
hist(as.Date(site_visit_data$observe_date), 
     breaks = "day", 
     xlab = "Survey Date") 
summary(as.Date(site_visit_data$observe_date))

# Format personnel names ----
site_visit_data = site_visit_data %>% 
  mutate(veg_observer = "Stephen Talbot",
         veg_recorder = "Stephen Talbot",
         env_observer = "Stephen Talbot",
         soils_observer = "none")

# Format structural class ----
unique(site_visit_data$`PLANT COMMUNITY NAME`)

site_visit_class = site_visit_data %>% 
  mutate(structural_class = case_when(`PLANT COMMUNITY NAME` == "Carex-Plantago mire" ~ "sedge emergent",
                                      `PLANT COMMUNITY NAME` == "Athyrium-Aconitum mesic meadow" ~ "forb meadow",
                                      `PLANT COMMUNITY NAME` == "Linnaea-Empetrum heath" ~ "dwarf shrub",
                                      `PLANT COMMUNITY NAME` == "Athyrium-Calamagrostis mesic meadow" ~ "grass meadow",
                                      `PLANT COMMUNITY NAME` == "Erigeron-Thelypteris mesic meadow" ~ "forb meadow",
                                      `PLANT COMMUNITY NAME` == "Phyllodoce heath" ~ "dwarf shrub",
                                      `PLANT COMMUNITY NAME` == "Honckenya beach dry coastal meadow" ~ "forb meadow",
                                      `PLANT COMMUNITY NAME` == "Leymus dune meadow" ~ "grass meadow",
                                      `PLANT COMMUNITY NAME` == "Vaccinium-Thamnolia fellfield" ~ "dwarf shrub",
                                      `PLANT COMMUNITY NAME` == "Salix-Athyrium thicket" ~ "low shrub",
                                      `PLANT COMMUNITY NAME` == "Carex snowbed meadow" ~ "sedge meadow",
                                      .default = "unknown"))

# Populate remaining columns ----
site_visit_final = site_visit_class %>% 
  mutate(project_code = 'fws_unalaska_2007',
         data_tier = "map development & verification",
         scope_vascular = "exhaustive",
         scope_bryophyte = "common species",
         scope_lichen = "common species",
         homogenous = "TRUE") %>% 
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
table(site_visit_final$homogenous) # Should all be TRUE

# Export as CSV ----
write_csv(site_visit_final, site_visit_output)

# Clear workspace ----
rm(list=ls())
