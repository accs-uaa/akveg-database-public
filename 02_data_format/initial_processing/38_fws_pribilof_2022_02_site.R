# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Table for USFWS Pribilof Islands 2022 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-08-06
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Site Table for USFWS Pribilof Islands 2022 data" formats site-level information for ingestion into the AKVEG Database. It depends upon the output of the '38_fws_pribilof_2022.py' script in the datum_conversion sub-folder of the akveg-database repository. This script appends site coordinates and populates fields with values that match constrained values in the AKVEG Database. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
plot_folder = path(project_folder, 'Data/Data_Plots/38_fws_pribilof_2022')
source_folder = path(plot_folder, 'source')
workspace_folder = path(plot_folder, 'working')
template_folder = path(project_folder, "Data/Data_Entry")

# Define input datasets
site_input = path(source_folder, 'pribilofs2022.xlsx')
coordinates_input = path(workspace_folder, 'pribilof_centroid_coordinates.csv')
template_input = path(template_folder, "02_site.xlsx")

# Define output datasets
site_output = path(plot_folder, '02_site_fwspribilof2022.csv')

# Read in data ----
site_original = read_xlsx(site_input,
                          range="A1:AN102")
coordinates_original = read_csv(coordinates_input)
template = colnames(read_xlsx(path=template_input))

# Explore data ----

# Ensure that each site code is unique
length(unique(site_original$`Releve number`)) == nrow(site_original) 

# Ensure prefixes for site codes are consistent. In this case, either STG22 or STP22.
site_original %>% 
  mutate(site_prefix = str_split_i(`Releve number`, "-", 1)) %>% 
  distinct(site_prefix)

# Drop sites in human-dominated environments ----
# 6 sites to drop
site_data = site_original %>% 
  filter(!grepl("Anthroscape", Avc_descrp))

# Obtain site coordinates ----
# Coordinates have already been checked for spatial outliers in GIS

# Join with coordinates_original
site_data = left_join(site_data,
            coordinates_original, 
                          join_by("Releve number" == "Relevé"))

# Are any sites missing coordinates?
site_data %>% 
  filter(is.na(POINT_X) | is.na(POINT_Y))

# Do the coordinates look reasonably positioned in space?
plot(site_data$POINT_X, site_data$POINT_Y)

# Format site code ----
# Replace dash with underscore
site_data = site_data %>% 
  mutate(site_code = str_replace_all(`Releve number`, "-", "_"))

print(site_data$site_code) # Check that codes look good

# Populate remaining columns ----
site_data = site_data %>% 
  mutate(establishing_project_code = 'fws_pribilof_2022',
         perspective = "ground",
         cover_method = "braun-blanquet visual estimate",
         h_datum = "NAD83",
         longitude_dd = round(POINT_X, digits = 5),
         latitude_dd = round(POINT_Y, digits = 5),
         h_error_m = -999,
         positional_accuracy = "consumer grade GPS",
         plot_dimensions_m = '20 radius',
         location_type = "targeted")%>% 
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(site_data, is.na)
    , sum))

# Export as CSV ----
write_csv(site_data, site_output)

# Clear workspace ----
rm(list=ls())
