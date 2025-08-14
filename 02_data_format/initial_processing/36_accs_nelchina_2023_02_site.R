# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Site Table for ACCS 2023 Nelchina data.
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-08-07
# Usage: Code chunks must be executed sequentially in R version 4.4.1+.
# Description: "Format Site Table for ACCS 2023 Nelchina data" parses data from a Survey123 form and combines it with manually entered data. It also creates site visit codes, renames columns, re-rpojects coordinates to NAD83, and selects specific columns to match the Site table in the AKVEG Database. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import libraries ----
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
plot_folder = path(project_folder, 'Data/Data_Plots/36_accs_nelchina_2023')
source_folder = path(plot_folder, 'source')
template_folder = path(project_folder, "Data/Data_Entry")

# Define inputs ----
input_survey123 = path(source_folder, "Survey123_Site.xlsx")
input_manual_entry = path(source_folder, "Nelchina_2023_Site.xlsx")
input_template = path(template_folder, "02_Site.xlsx")

# Define outputs ----
output_site = path(plot_folder,"02_site_accsnelchina2023.csv")

# Read in data ----
survey123_original = read_xlsx(path=input_survey123)
manual_entry_original = read_xlsx(path=input_manual_entry)
template = colnames(read_xlsx(path=input_template))

# Format manually entered data ----
manual_entry_data = manual_entry_original %>%
  mutate(establishing_project_code = "accs_nelchina_2023") %>% # Update project name
  select(all_of(template)) # Select only the relevant columns

# Format Survey123 data ----
survey123_data = survey123_original %>%
  rename(perspective = Perspective,
         cover_method = `Cover Method`,
         h_datum = `Horizontal Datum`,
         h_error_m = error_m,
         positional_accuracy = `Positional Accuracy`,
         plot_dimensions_m = `Plot Dimensions (m)`,
         location_type = `Location Type`) %>% 
  mutate(establishing_project_code = "accs_nelchina_2023",
         plot_dimensions_m = str_replace_all(plot_dimensions_m, 
                                             pattern ="m radius", 
                                             replacement = "radius"),
         site_code = str_remove(`Site Code`, "2023")) %>% 
  select(all_of(template))

# Append dataframes ----
site_data = survey123_data %>% 
  bind_rows(manual_entry_data) %>% 
  arrange(site_code)

# Round error to 2 decimal places
site_data = site_data %>% 
  mutate(h_error_m = round(h_error_m,digits=2))

# Re-project coordinates ----
summary(site_data$latitude_dd) # Y-axis, positive values
summary(site_data$longitude_dd) # X-axis, negative values

# Convert to sf object
# Datum is WGS 84
site_sf = st_as_sf(site_data, 
                   coords = c("longitude_dd", "latitude_dd"),
                   crs = 4326)

# Re-project to NAD 83 (EPSG 4269)
site_sf_project = st_transform(x=site_sf, crs=4269)
st_crs(site_sf_project) # Ensure correct EPSG is listed

# Extract coordinates back into df
# Drop old coordinates to avoid confusion
site_data = site_data %>% 
  bind_cols(st_coordinates(site_sf_project)) %>% 
  select(-c(longitude_dd, latitude_dd))
  
# Rename coordinate columns and update datum
site_data_final = site_data %>% 
  mutate(h_datum = "NAD83",
         longitude_dd = round(X, digits = 5),
         latitude_dd = round(Y, digits = 5)) %>% 
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(site_data_final, is.na)
    , sum))

unique(site_data_final$establishing_project_code)
unique(site_data_final$plot_dimensions_m)
unique(str_split_i(site_data_final$site_code, pattern = "_", 1))
unique(site_data_final$perspective)
table(site_data_final$cover_method)
unique(site_data_final$positional_accuracy)
table(site_data_final$location_type)

# Export data ----
write_csv(x=site_data_final, file=output_site)

# Clear workspace ----
rm(list=ls())
