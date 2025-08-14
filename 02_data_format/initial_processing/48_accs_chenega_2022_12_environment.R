# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Environment Table for ACCS Chenega Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-02
# Usage: Must be executed in R version 4.4.0+.
# Description: "Format Environment Cover Table for ACCS Chenega Data" formats environmental data collected and entered by ACCS for ingestion into the AKVEG Database. The script reclassifies values to match constrained values, imposes correct unknown codes onto missing data, renames columns, and adds required metadata fields where missing. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(stringr)
library(tidyr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/48_accs_chenega_2022')
template_folder = path(project_folder, "Data/Data_Entry")
source_folder = path(plot_folder, 'source')

# Define datasets ----

# Define input datasets
environment_input = path(source_folder,'Chenega Appendices 28Apr2023.xlsx')
site_visit_input = path(plot_folder, '03_sitevisit_accschenega2022.csv')
template_input = path(template_folder, "12_environment.xlsx")

# Define output datasets
environment_output = path(plot_folder, '12_environment_accschenega2022.csv')

# Read in data ----
environment_original = read_xlsx(environment_input, sheet="App II Ground Plot Metadata", range="A2:Q20")
site_visit_original = read_csv(site_visit_input)
template = colnames(read_xlsx(path=template_input))

# Obtain site visit code ----

# Format site code to match new convention
environment_data = environment_original %>%
  mutate(Plot = str_replace_all(Plot, "CHE_GRN_", "CHEN_"))

# Join with Site Visit table to obtain site visit code
environment_data = site_visit_original %>% 
  select(site_code, site_visit_code) %>% 
  left_join(environment_data, by = c("site_code" = "Plot"))

# Ensure correct number of sites (n=18)
length(unique(environment_data$site_visit_code))

# Ensure all sites have a site code
environment_data %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Reclassify drainage ----
environment_data = environment_data %>% 
  mutate(drainage = case_when(grepl(pattern="continuously saturated",
                                    x=`Land Cover Type (Detailed)`) ~ "aquatic",
                              grepl(pattern="permanently flooded",
                                    x=`Land Cover Type (Detailed)`) ~ "aquatic",
                              grepl(pattern="seasonally saturated",
                                    x=`Land Cover Type (Detailed)`) ~ "flooded",
                              grepl(pattern="flooded",
                                    x=`Land Cover Type (Detailed)`) ~ "flooded",
                              .default = "NULL"))

# Reclassify disturbance ----
table(environment_data$Disturbance)

environment_data = environment_data %>% 
  mutate(disturbance = case_when(Disturbance == "tidal,wildlife_grazing" ~ "tidal",
                                 Disturbance == "wildlife_digging,wildlife_trails" ~ "wildlife digging",
                                 .default = Disturbance))

# Check that all values now match constrained disturbance values
table(environment_data$disturbance)

# Reclassify water depth ----
environment_data = environment_data %>% 
  mutate(depth_water_cm = case_when(grepl("na",`Water Depth (cm)`) ~ "-999",
                                    .default = `Water Depth (cm)`))

# Convert to numeric
environment_data$depth_water_cm = as.numeric(environment_data$depth_water_cm)

# Create surface_water variable ----
environment_data = environment_data %>% 
  mutate(surface_water = case_when(depth_water_cm < 0 ~ FALSE,
                                   depth_water_cm >= 0 ~ TRUE))

# Reclassify moss/duff depth ----
# Change 'na' values to -999 (no moss/duff present)
environment_data = environment_data %>% 
  mutate(depth_moss_duff_cm = case_when(`Moss/Duff Depth (cm)` == "na" ~ "-999",
                                        .default = `Moss/Duff Depth (cm)`)) %>% 
  mutate(depth_moss_duff_cm = as.numeric(depth_moss_duff_cm))

# Populate missing columns ----
# Use appropriate NULL values
environment_data = environment_data %>%
  mutate(disturbance_severity = "NULL",
         disturbance_time_y = -999,
         depth_restrictive_layer_cm = -999,
         restrictive_type = "NULL",
         microrelief_cm = -999,
         soil_class = "NULL",
         cryoturbation = "FALSE",
         dominant_texture_40_cm = "NULL",
         depth_15_percent_coarse_fragments_cm = -999)

# Rename columns ----
environment_data = environment_data %>% 
  rename(physiography = Physiography,
         geomorphology = Geomorphology,
         macrotopography = Macrotopography,
         microtopography = Microtopography,
         moisture_regime = `Moisture Regime`) %>%
  select(all_of(template)) # Restrict to only those columns required by AKVEG

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(environment_data, is.na)
    , sum)
)

# Export as CSV ----
write_csv(environment_data, environment_output)

# Clear workspace ----
rm(list=ls())