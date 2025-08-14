# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Site table for USFWS Alaska Peninsula data.
# Author: Amanda Droghini
# Last Updated: 2023-05-30
# Usage: Code chunks must be executed sequentially in R 4.3.0+.
# Description: "Format Site table data" creates a unique site code, adds coordinates, renames columns,  populates unknowns, and formats entries so that the data match standards adopted by the Alaska Vegetation Technical Working Group.
# ---------------------------------------------------------------------------

rm(list=ls())

# Load packages ----
library(readxl)
library(tidyverse)

# Define directories ----
drive <- "G:"
root_folder <- file.path(drive,"ACCS_Work")
data_folder <- file.path(root_folder, "Projects/AKVEG_Database/Data")
template_folder <- file.path(data_folder,"Data_Entry")
project_folder <- file.path(data_folder, "Data_Plots/37_fws_alaskapeninsula_2006")
temp_folder <- file.path(project_folder,'temp')

# Define inputs ----
input_site <- file.path(temp_folder, "site.xlsx")
input_coords <- file.path(temp_folder,"site_centroid_coordinates.csv")
input_template <- file.path(template_folder,"02_site.xlsx")

# Define outputs ----
output_site <- file.path(project_folder, "02_site_fwsakpen2006.csv")

# Read in data ----
site_data <- read_xlsx(path=input_site)
site_coordinates <- read_csv(file=input_coords)
template_columns <- colnames(read_xlsx(path=input_template))

# Join data ----
# Join coordinates to site data by creating a unique site code key
site_coordinates <- site_coordinates %>% 
  mutate(site_code = paste(AREA_NAME, YEAR_, SITE_NO, sep="_")) %>% 
  select(-c(OID_, AREA_NAME, YEAR_, SITE_NO, File_Name))

site_data <- site_data %>% 
  mutate(site_code = paste(area_name, year, site_number, sep="_"))

site_data <- site_data %>% 
  left_join(site_coordinates, by="site_code")

# Format data ----
site_formatted <- site_data %>% 
  filter(!(is.na(POINT_X) | is.na(POINT_Y))) %>% 
  rename(latitude_dd = POINT_Y,
         longitude_dd = POINT_X) %>% 
  mutate(establishing_project_code = "fws_akpen_2006",
         perspective="aerial",
         cover_method = "semi-quantitative visual estimate",
         h_datum = "NAD83",
         h_error_m = -999,
         positional_accuracy = "image interpretation",
         plot_dimensions_m = "unknown",
         location_type = "targeted") %>% 
  select(all_of(template_columns))

# Export CSV ----
write_csv(site_formatted,output_site)