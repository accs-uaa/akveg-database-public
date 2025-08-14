# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Table for ACCS Chenega Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-03
# Usage: Must be executed in R version 4.4.0+.
# Description: "Format Site Table for ACCS Chenega Data" formats site-level data collected and entered by ACCS for ingestion into the AKVEG Database. The script re-projects coordinates, formats site codes, populates fields with values that match constrained values in the AKVEG Database, and adds required fields where missing. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
plot_folder = path(project_folder, 'Data/Data_Plots/48_accs_chenega_2022')
template_folder = path(project_folder, "Data/Data_Entry")
source_folder = path(plot_folder, 'source')

# Define input datasets
site_chenega_input = path(source_folder,'02_Site_Chenega_2022.xlsx')
template_input = path(template_folder, "02_site.xlsx")

# Define output datasets
site_chenega_output = path(plot_folder, '02_site_accschenega2022.csv')

# Read in data ----
site_chenega_original = read_xlsx(site_chenega_input)
template = colnames(read_xlsx(path=template_input))

# Explore data ----

# Ensure each site code is unique
site_chenega_original %>% distinct(site_code) %>% nrow == nrow(site_chenega_original)

# Re-project coordinates ----

# Coordinates for Chenega: 60.0649° N, 148.0112° W
summary(site_chenega_original$latitude_dd) # Y-axis, positive values
summary(site_chenega_original$longitude_dd) # X-axis, negative values

# Convert to sf object
# Datum is WGS 84 (listed EPSG is 5936, but I'm assuming what was meant was the more conventional 4326)
site_sf = st_as_sf(site_chenega_original, 
                   coords = c("longitude_dd", "latitude_dd"),
                   crs = 4326)
plot(site_sf["site_code"], key.pos=NULL) # Quick check that coordinates are generally where we expect them to be

# Re-project to NAD 83 (EPSG 4269)
site_sf_project = st_transform(x=site_sf, crs=4269)
st_crs(site_sf_project) # Ensure correct EPSG is listed

# Extract coordinates back into df
# Drop old coordinates to avoid confusion
site_chenega = site_chenega_original %>% 
  bind_cols(st_coordinates(site_sf_project)) %>% 
  select(-c(longitude_dd, latitude_dd))

# Format site code ----
# Remove abbreviation for 'ground' (GRN). Plots surveyed by air are not included in this dataset and site visit code (which includes date) will be unnecessarily long if original names are kept
site_chenega = site_chenega %>% 
  mutate(site_code = str_replace_all(site_code, "CHE_GRN_", "CHEN_"))

# Ensure names are still unique
length(unique(site_chenega$site_code)) # Should be 18

# Populate remaining fields ----
# With names that match constrained values in AKVEG
site_chenega = site_chenega %>% 
  mutate(establishing_project_code = 'accs_chenega_2022',
         perspective = 'ground',
         cover_method = 'semi-quantitative visual estimate',
         longitude_dd = round(X, digits = 5),
         latitude_dd = round(Y, digits = 5),
         h_datum = "NAD83",
         h_error_m = -999,
         positional_accuracy = "consumer grade GPS",
         plot_dimensions_m = "10 radius",
         location_type = "random") %>% 
  select(all_of(template))

# Export as CSV ----
write_csv(site_chenega, site_chenega_output)

# Clear workspace ----
rm(list=ls())