# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Table for USFS Glacier Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-05-09
# Usage: Must be executed in R version 4.4.0+.
# Description: "Format Site Table for USFS Glacier  Data" uses data from surveys conducted by the U.S. Forest Service's Chugach National Forest to extract relevant site-level information for ingestion into the AKVEG Database. The script omits sites that are missing coordinates or cover data (vegetation or abiotic). The script also creates standardized site codes and populates required metadata. It depends upon the output from the corresponding script in the datum_conversion folder.
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
plot_folder = path(project_folder, 'Data/Data_Plots/46_usfs_glacier_2023')
template_folder = path(project_folder, "Data/Data_Entry")
workspace_folder = path(plot_folder, 'working')
source_folder = path(plot_folder, 'source')

# Define input datasets
coordinates_input = path(workspace_folder,"site_glacier_cordova_coordinates.csv") 
glacier_original_input = path(source_folder,'GRD_AccessDB_08182023.xlsx')
template_input = path(template_folder, "02_site.xlsx")

# Define output datasets
site_glacier_output = path(plot_folder, '02_site_usfsglacier2023.csv')
site_codes_output = path(workspace_folder, 'site_codes_glacier.csv')

# Read in data ----
coords_glacier = read_csv(coordinates_input)
site_original_glacier = read_xlsx(glacier_original_input,
                                  sheet="01_NRT_DX_SITE_GENERAL",range="B2:B531", 
                                  col_names = "id_site")
cover_veg_glacier = read_xlsx(glacier_original_input, 
                              sheet="11_NRT_DX_OC_PLANT_COVER",
                              range="B2:B5938", col_names = "id_cover")
cover_abiotic_glacier = read_xlsx(glacier_original_input, 
                                  sheet="10_NRT_DX_OC_GROUND_COVER",
                                  range="B2:B200", col_names = "id_cover")
template = colnames(read_xlsx(path=template_input))

# Drop sites with insufficient data ----
# Sites must have coordinates and either vegetation or abiotic cover data

# Combine list of sites that have veg or abiotic cover data
cover_glacier = bind_rows(cover_abiotic_glacier, cover_veg_glacier)

cover_glacier = cover_glacier %>% 
  filter(!is.na(id_cover)) %>%
  distinct(id_cover) %>% 
  arrange(id_cover)

# Are any of these sites missing coordinates?
which(!(cover_glacier$id_cover %in% coords_glacier$SITE_ID_1))

# Do all sites with cover data have an entry in the 'site' sheet?
cover_glacier[which(!(cover_glacier$id_cover %in% site_original_glacier$id_site)),]

# Restrict sites in 'site' sheet to only those w/ cover data
# Drop site 2022GRD0628024: site does not have any 'ground cover' data and total veg cover % is only 79.2%
site_glacier = site_original_glacier %>% 
  filter(id_site %in% cover_glacier$id_cover) %>% 
  filter(id_site != "2022GRD0628024")
  
# Create new site codes ----
# New site codes will match formatting of USFS Kenai sites that are already in the AKVEG database 
# Begin with site prefix: GRDYYYY
# Restrict to sites that have cover data
site_glacier = site_glacier %>%
  arrange(id_site) %>% 
  mutate(site_prefix = case_when(grepl("2022", x = id_site) ~ "GRD2022",
                                 grepl("2021", x = id_site) ~ "GRD2021",
                                 .default = "ERROR")) %>% 
  group_by(site_prefix) %>% 
  mutate(site_number = row_number(),
         site_number = str_pad(site_number,width=4,pad="0",side="left"),
         new_site_code = str_c(site_prefix, site_number, sep="")) %>% 
  ungroup() %>% 
  select(-c(site_number, site_prefix)) %>% 
  rename(original_code = id_site)

# Check site prefixes: Should be either GRD2021 or GRD2022
site_glacier %>% 
  mutate(site_code = str_sub(new_site_code, end=-5L)) %>% 
  distinct(site_code)

# Save as separate object for export
# Table will be used to link old site ID to new site code
site_codes_link = site_glacier

# Format coordinates ----

# Add coordinates data to site table
site_glacier = site_glacier %>% 
  left_join(coords_glacier, by = c("original_code" = "SITE_ID_1"))

# Ensure every site has a coordinate
site_glacier %>% 
  filter(is.na(POINT_X) | is.na(POINT_Y))

# Rename latitude and longitude columns, restrict to 5 decimal places, populate correct datum
site_glacier = site_glacier %>%
  mutate(latitude_dd = round(POINT_Y, digits = 5),
         longitude_dd = round(POINT_X, digits = 5),
         h_datum = "NAD83")

# Populate remaining columns ----
# Assume sites were ground sites
# Plot radius was recorded as 50 feet
site_glacier = site_glacier %>% 
  rename(site_code = new_site_code) %>% 
  mutate(establishing_project_code = "usfs_glacier_2023",
         plot_dimensions_m = "15 radius",
         perspective = "ground",
         cover_method = "semi-quantitative visual estimate",
         h_error_m = -999,
         positional_accuracy = "consumer grade GPS",
         location_type = "targeted") %>% 
  select(all_of(template))

# Export as CSV ----
write_csv(site_glacier, site_glacier_output)
write_csv(site_codes_link, site_codes_output)

# Clear workspace ----
rm(list=ls())