# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Table for DATASET_NAME data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: CURRENT_DATE
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Site Table for DATASET_NAME data" formats site-level information for ingestion into the AKVEG Database. The script drops sites with no vegetation cover data, standardizes site codes and plot dimension values, projects coordinates to NAD83, and adds required metadata fields. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(ggmap)
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
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = plot_folder_path
template_folder = path(project_folder, 'Data', 'Data_Entry')
repository_folder = path(drive, root_folder, 'Repositories', 'akveg-database')

# Define datasets ----

# Define inputs
site_input = site_path
template_input = path(template_folder, "02_site.xlsx")

# Define outputs
site_output = path(plot_folder, paste0("02_site_", code_for_output, ".csv"))

# Define functions ----
# Import database connection function
plotting_script = path(repository_folder,
                       'package_DataProcessing','map_spatial_outliers.R')
source(plotting_script)

# Read in data ----
site_original = read_csv(site_input)
template = colnames(read_xlsx(path=template_input))

# Explore data ----

# Ensure that each site code is unique
length(unique(site_original$plot_id)) == nrow(site_original) 

# Are any sites missing coordinates?
site_original %>% 
  filter(is.na(lat_dd84) | is.na(long_dd84))

# Ensure prefixes for site codes are consistent
site_original %>% 
  mutate(site_prefix = str_split_i(`plot_id`, "_", 1)) %>% 
  distinct(site_prefix)

# Drop sites that are missing data ----

# Format site codes ----
# Add FWS as a prefix
site_data = site_original %>% 
  mutate(site_code = str_c("FWS", plot_id, sep = "_"))

# Re-project coordinates ----

# Coordinates for Unalaska: 53.873° N, 166.538° W
summary(site_data$lat_dd84) # Y-axis, positive values
summary(site_data$long_dd84) # X-axis, negative values

# Convert to sf object
# Datum is WGS 84
site_sf = st_as_sf(site_data, 
                   coords = c("long_dd84", "lat_dd84"),
                   crs = 4326)

# Re-project to NAD 83 (EPSG 4269)
site_sf_project = st_transform(x=site_sf, crs=4269)
st_crs(site_sf_project) # Ensure correct EPSG is listed

# Extract coordinates back into df
# Drop old coordinates to avoid confusion
site_data = site_data %>% 
  bind_cols(st_coordinates(site_sf_project)) %>% 
  select(-c(long_dd84, lat_dd84))

# Look for spatial outliers ----

plot_display = "FALSE"

if (plot_display == "TRUE"){
  plot_outliers(site_sf_project, api_key, 12)
}

# Format plot dimensions ----

# Populate remaining columns ----
site_final = site_data %>%
  mutate(establishing_project_code = code_name,
         perspective = "ground",
         cover_method = "braun-blanquet visual estimate",
         h_datum = "NAD83",
         longitude_dd = round(X, digits = 5),
         latitude_dd = round(Y, digits = 5),
         h_error_m = -999,
         positional_accuracy = "consumer grade GPS",
         location_type = "targeted")%>% 
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(site_final, is.na)
    , sum))

# Are all site codes unique?
length(unique(site_final$site_code)) == nrow(site_final)

# Export as CSV ----
write_csv(site_final, site_output)

# Clear workspace ----
rm(list=ls())

# Did you remember to update the script header?
