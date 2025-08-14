# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Table for Harvard University Alder Data"
# Author: Calvin Heslop, Harvard University
# Last Updated: 2024-07-08
# Usage: Must be executed in R version 4.4.0+.
# Description: "Format Site Table for Harvard University Alder Data" formats site-level data collected and entered by Calvin Heslop for ingestion into the AKVEG Database. The script removes sites with missing cover data, re-projects coordinates to NAD83, renames columns, and adds metadata fields. The output is a CSV file that can be used in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readxl)
library(sf)

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
site_input = path(source_folder, 'Model_validation_points.shp')
template_input = path(template_folder, "02_site.xlsx")

# Define output dataset
site_output = path(plot_folder, '02_site_harvardalder2023.csv')

# Read in data ----
site_Heslop_original <- read_sf(site_input)
template <- colnames(read_xlsx(path=template_input)) 

# Explore data ----

# Ensure each site code is unique

length(unique(site_Heslop_original$name)) == nrow(site_Heslop_original)

# Remove incomplete plots ----

# Remove rows with missing cover estimates
site_Heslop_original <- subset(site_Heslop_original, !is.na(alder_cove))

# Remove ArtPos sites (n=2)
site_Heslop_original = site_Heslop_original %>% 
  filter(!(grepl(pattern = "ArtPos", name)))

# Re-project coordinates ----

# Re-project to NAD 83 (EPSG 4269)
site_sf_project = st_transform(x=site_Heslop_original, crs=4269)
st_crs(site_sf_project) # Ensure correct EPSG is listed

# Change names and populate remaining fields ----
site_heslop <- as.data.frame(site_sf_project) %>% 
  rename(site_code = 'name') %>% 
  cbind(st_coordinates(site_sf_project)) %>% 
  mutate(establishing_project_code = 'harvard_alder_2023',
         perspective = 'ground',
         cover_method = 'semi-quantitative visual estimate',
         longitude_dd = round(X, digits = 5),
         latitude_dd = round(Y, digits = 5),
         h_datum = "NAD83",
         h_error_m = -999, # missing
         positional_accuracy = "consumer grade GPS",
         plot_dimensions_m = "5 radius",
         location_type = "targeted") %>% 
  select(all_of(template))

# Export as CSV ----
write.csv(site_heslop, site_output,
          row.names = FALSE,
          fileEncoding = "UTF-8")

# Clear workspace ----
rm(list=ls())