# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Table for ACCS Shemya Data"
# Author: Amanda Droghini Alaska Center for Conservation Science
# Last Updated: 2024-08-07
# Usage: Must be executed in R version 4.4.0+.
# Description: "Format Site Table for ACCS Shemya Data" formats site-level data collected and entered by ACCS for ingestion into the AKVEG Database. The script re-projects coordinates to NAD83, excludes sites with no cover data, corrects a duplicate site code, and populates fields with values that match constrained values in the AKVEG Database. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(sf)
library(stringr)
library(tidyr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/49_accs_shemya_2022')
template_folder = path(project_folder, "Data/Data_Entry")
source_folder = path(plot_folder, 'source')

# Define input datasets
site_shemya_input = path(source_folder,'02_Site_Shemya_2022.xlsx')
site_visit_input = path(source_folder,'03_Site_Visit_Shemya_2022.xlsx')
template_input = path(template_folder, "02_site.xlsx")

# Define output datasets
site_shemya_output = path(plot_folder, '02_site_accsshemya2022.csv')

# Read in data ----
site_shemya_original = read_xlsx(site_shemya_input)
site_visit_original = read_xlsx(site_visit_input)
template = colnames(read_xlsx(path=template_input))

# Correct site code ----
# There are two sites listed with the same code (eas22r313), even though they relate to different sites (different coordinates, personnel, structural class, etc.)
# From Anjanette: the second set of Lat/longs is correct
which(site_shemya_original$site_code=='eas22r313')
site_shemya_original = site_shemya_original[-12,]

# Re-project coordinates ----

# Coordinates for Shemya: 52.7228° N, 174.1124° E
summary(site_shemya_original$latitude_dd) # Y-axis
summary(site_shemya_original$longitude_dd) # X-axis

# Convert to sf object
# Datum is WGS 84 (listed EPSG is 5936, but project report suggests the more conventional 4326)
site_sf = st_as_sf(site_shemya_original, 
                      coords = c("longitude_dd", "latitude_dd"),
                      crs = 4326)

# Re-project to NAD 83 (EPSG 4269)
site_sf_project = st_transform(x=site_sf, crs=4269)
st_crs(site_sf_project) # Ensure correct EPSG is listed

# Extract coordinates back into df
# Drop old coordinates to avoid confusion
site_shemya = site_shemya_original %>% 
  bind_cols(st_coordinates(site_sf_project)) %>% 
  select(-c(longitude_dd, latitude_dd))

# Exclude sites w/o cover data ----
exclude_site = site_shemya[which(!(site_shemya$site_code %in% site_visit_original$site_code)),]$site_code

site_shemya = site_shemya %>% 
  filter(!(site_code %in% exclude_site))

# Ensure codes are the same in both the Site and Site Visit tables
which(!(site_shemya$site_code %in% site_visit_original$site_code))
which(!(site_visit_original$site_code %in% site_shemya$site_code))

# Format data ----

# Populate existing fields with names that match constrained values in AKVEG
# GPS coordinates obtained using built-in GPS on the tablet
site_shemya = site_shemya %>% 
  mutate(establishing_project_code = 'accs_shemya_2022',
         perspective = 'ground',
         cover_method = 'semi-quantitative visual estimate',
         longitude_dd = round(X, digits = 5),
         latitude_dd = round(Y, digits = 5),
         h_datum = "NAD83",
         positional_accuracy = "consumer grade GPS",
         plot_dimensions_m = "3 radius",
         location_type = "random",
         h_error_m = case_when(is.na(h_error_m) | h_error_m == 0 ~ -999,
                               .default = h_error_m),
         h_error_m = round(h_error_m, digits = 2)) %>% 
  select(all_of(template))

# Export as CSV ----
write_csv(site_shemya, site_shemya_output)

# Clear workspace ----
rm(list=ls())