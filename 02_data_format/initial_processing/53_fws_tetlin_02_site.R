# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Tetlin 2022-2024 site data for AKVEG Database
# Author: Timm Nawrocki, Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2026-01-24
# Usage: Must be executed in R version 4.5.1+.
# Description: "Format Tetlin 2022-2024 site data for AKVEG Database" formats site data for entry into AKVEG Database. Three input data tables were provided: one for 2022 sites, one for 2024 early summer sites ('2024 first input'), and one for 2024 late summer sites ('2024 second input'). This script formats these tables separately, ensuring fields and columns are aligned with the AKVEG schema, and then merges the harmonized table into a single table that contains all site data from 2022 and 2024. The output is a CSV file that can be converted to a SQL INSERT statement for loading to the AKVEG Database.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr, warn.conflicts = FALSE)
library(fs)
library(readr)
library(readxl)
library(sf)
library(stringr)
library(tidyr)

#### SET UP DIRECTORIES AND FILES
####------------------------------

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/53_fws_tetlin_2024')
template_folder = path(project_folder, 'Data/Data_Entry')
source_folder = path(plot_folder, 'source')

# Define input datasets
site_2022_input = path(source_folder, 'Data_2022', 'TNWR_2022_points_sampled_coordinates.csv')
site_2024_first_input = path(source_folder, 'Data_2024', 'v2', 'TetlinNWR_2024_sites_compiled.csv')
site_2024_second_input = path(source_folder, 'Data_2024', 'extra_sites', '02_fws_tetlin_2024_site.xlsx')

# Define input template
template_input = path(template_folder, '02_site.xlsx')

# Define output dataset
site_output = path(plot_folder, '02_site_fwstetlin2024.csv')

# Read in template column names
site_template = colnames(read_xlsx(path=template_input))

#### PARSE 2022 SITE DATA
####------------------------------

site_2022 = read_csv(site_2022_input) %>%
  # Convert geometries to points with EPSG:4326 (WGS84)
  st_as_sf(x = ., coords = c('POINT_X', 'POINT_Y'), crs = 4326, remove = FALSE) %>%
  # Reproject coordinates to EPSG 4269 (NAD83)
  st_transform(crs = st_crs(4269)) %>%
  # Add EPSG:4269 centroid coordinates
  mutate(longitude_dd = st_coordinates(.$geometry)[,1],
         latitude_dd = st_coordinates(.$geometry)[,2]) %>%
  st_zm(drop = TRUE, what = "ZM") %>%
  st_drop_geometry() %>%
  # Create site code
  mutate(site_code = case_when(TransectID < 10 ~ paste('TET22_00', TransectID, sep = ''),
                               TransectID < 100 ~ paste('TET22_0', TransectID, sep = ''),
                               TRUE ~ paste('TET22_', TransectID, sep = ''))) %>%
  # Add site metadata
  mutate(establishing_project_code = 'fws_tetlin_2024',
         perspective = 'ground',
         cover_method = 'subplot transect visual estimate',
         h_datum = 'NAD83',
         h_error_m = 3,
         positional_accuracy = 'consumer grade GPS',
         plot_dimensions_m = '1×10',
         location_type = 'targeted') %>%
  # Select final columns
  select(all_of(site_template))

#### PARSE 2024 SITE DATA - FIRST INPUT
####------------------------------

site_2024_first = read_csv(site_2024_first_input) %>%
  # Convert geometries to points with EPSG:4326
  st_as_sf(x = ., coords = c('Longitude', 'Latitude'), crs = 4326, remove = FALSE) %>%
  # Reproject coordinates to EPSG 4269
  st_transform(crs = st_crs(4269)) %>%
  # Add EPSG:4269 centroid coordinates
  mutate(longitude_dd = st_coordinates(.$geometry)[,1],
         latitude_dd = st_coordinates(.$geometry)[,2]) %>%
  st_zm(drop = TRUE, what = "ZM") %>%
  st_drop_geometry() %>%
  # Create site code
  mutate(site_code = str_replace(Site_code, '-', '_')) %>%
  # Add site metadata
  mutate(establishing_project_code = 'fws_tetlin_2024',
         perspective = 'ground',
         cover_method = 'subplot transect visual estimate',
         h_datum = 'NAD83',
         positional_accuracy = 'consumer grade GPS',
         plot_dimensions_m = '1×10',
         location_type = 'targeted') %>%
  # Correct horizontal error
  rename(h_error_m = Cord_error) %>%
  mutate(h_error_m = case_when(h_error_m == 0 ~ 3,
                               TRUE ~ h_error_m)) %>%
  # Select final columns
  select(all_of(site_template))

#### PARSE 2024 SITE DATA - SECOND INPUT
####------------------------------

site_2024_second = read_xlsx(site_2024_second_input) %>%
  # Convert geometries to points with EPSG:4326
  st_as_sf(x = ., coords = c('longitude_original', 'latitude_original'), crs = 4326, remove = FALSE) %>%
  # Reproject coordinates to EPSG 4269
  st_transform(crs = st_crs(4269)) %>%
  # Add EPSG:4269 coordinates
  mutate(longitude_dd = st_coordinates(.$geometry)[,1],
         latitude_dd = st_coordinates(.$geometry)[,2]) %>%
  st_zm(drop = TRUE, what = "ZM") %>%
  st_drop_geometry() %>%
  # Create site code
  mutate(site_code = str_replace(site_code, '-', '_')) %>%
  # Add site metadata
  mutate(establishing_project_code = 'fws_tetlin_2024',
         plot_dimensions_m = '12.5 radius',    ## Correct plot dimensions
         h_datum = 'NAD83') %>%
  # Select final columns
  select(all_of(site_template))

#### MERGE AND EXPORT 2022 AND 2024 DATA
####------------------------------
site_data = rbind(site_2022, site_2024_first, site_2024_second) %>%
  filter(site_code != 'TET24_036' & site_code != 'TET22_540')

# Export data
write_csv(site_data, site_output)

# Clear workspace
rm(list=ls())
