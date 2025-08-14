# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Site Visit table for USFWS Alaska Peninsula data.
# Author: Amanda Droghini
# Last Updated: 2023-05-30
# Usage: Code chunks must be executed sequentially in R 4.3.0+.
# Description: "Format Site Visit table data" renames columns, populates unknowns, re-classifies "structural class" categories, and formats entries so that the data match standards adopted by the Alaska Vegetation Technical Working Group.
# ---------------------------------------------------------------------------

rm(list=ls())

# Load packages ----
library(readxl)
library(tidyverse)

# Define directories ----
drive <- "D:"
root_folder <- file.path(drive,"ACCS_Work")
data_folder <- file.path(root_folder, "Projects/AKVEG_Database/Data")
template_folder <- file.path(data_folder,"Data_Entry")
project_folder <- file.path(data_folder, "Data_Plots/37_fws_alaskapeninsula_2006")
temp_folder <- file.path(project_folder,'temp')

# Define inputs ----
input_site <- file.path(temp_folder, "site.xlsx")
input_site_subset <- file.path(temp_folder,"02_site_fwsakpen2006.csv")
input_cover_class <- file.path(temp_folder,"unique_cover_class.csv")
input_template <- file.path(template_folder,"03_site_visit.xlsx")

# Define outputs ----
output_visit <- file.path(project_folder, "03_sitevisit_fwsakpen2006.csv")

# Read in data ----
site_data <- read_xlsx(path=input_site)
site_subset <- read_csv(file=input_site_subset)
cover_class <- read_csv(file=input_cover_class)
template_columns <- colnames(read_xlsx(path=input_template))

# Restrict sites ----
# Include only those for which coordinates are available
# Create unique site ID to use as a join key

visit_data <- site_data %>% 
  mutate(site_code = paste(area_name, year, site_number, sep="_"))

site_subset <- site_subset %>% 
  select(site_code)

visit_data <- visit_data %>% 
  right_join(site_subset,by="site_code")

# Reclassify structural class ----
# According to constrained values in the VTWG Minimum Standards
visit_data <- visit_data %>% 
  left_join(cover_class,by="calculated_class")

# Format data ----
visit_formatted <- visit_data %>% 
  mutate(project_code = "fws_akpen_2006",
         observe_date = as.character(as.Date(observation_date, format="yyyy-mm-dd")),
         date_string = str_replace_all(observe_date, 
                                       pattern="-", replace = ""),
         site_visit_code = paste(site_code,date_string,sep="_"),
         data_tier = "map development & verification",
         veg_observer = "unknown",
         veg_recorder = "unknown",
         env_observer = "none",
         soils_observer = "none",
         scope_vascular = "top canopy",
         scope_bryophyte = "category",
         scope_lichen = "category",
         homogenous = case_when(grepl(pattern = "HETERO", x = comments) ~ "FALSE",
                                .default = "TRUE")) %>% 
  select(all_of(template_columns))

# QA/QC ----

# Check if there are any NAs
sapply(visit_formatted, function(x) sum(is.na(x)))

# Check that all structural classes have been coded properly
unique(visit_formatted$structural_class)

# Export CSV ----
write_csv(visit_formatted,output_visit)