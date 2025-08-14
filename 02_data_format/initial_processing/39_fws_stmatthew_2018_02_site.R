# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Table for USFWS St Matthew 2018 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-23
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Site Table for USFWS St Matthew 2018 data" formats site-level information for ingestion into the AKVEG Database. The script drops sites with no vegetation cover data, standardizes site codes and plot dimension values, projects coordinates to NAD83, and adds required metadata fields. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(ggmap)
library(readr)
library(readxl)
library(sf)
library(stringr)
library(terra)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/39_fws_stmatthew_2018')
source_folder = path(plot_folder, 'source', 'FieldData', 'Originals')
template_folder = path(project_folder, "Data/Data_Entry")

# Define input datasets
site_input = path(source_folder, 'stmatthew_data_veg_structure.csv')
veg_cover_input = path(source_folder, "stmatthew_data_veg_cover.csv")
envr_input = path(source_folder, "stmatthew_data_environment.csv")
template_input = path(template_folder, "02_site.xlsx")

# Define output datasets
site_output = path(plot_folder, '02_site_fwsstmatthew2018.csv')

# Read in data ----
site_original = read_csv(site_input)
veg_cover_original = read_csv(veg_cover_input)
environment_original = read_csv(envr_input)
template = colnames(read_xlsx(path=template_input))

# Explore data ----

# Ensure that each site code is unique
length(unique(site_original$plot_id)) == nrow(site_original) 

# Are any sites missing coordinates?
site_original %>% 
  filter(is.na(lat_dd84) | is.na(long_dd84))

# Drop sites without vegetation data
sites_with_data = unique(veg_cover_original$plot_id)

site_data = site_original %>% 
  filter(plot_id %in% sites_with_data) # As expected, only sites with veg_completeness = "None" were dropped

# Ensure prefixes for site codes are consistent. In this case, should all start with 'stmatt_'.
site_data %>% 
  mutate(site_prefix = str_split_i(`plot_id`, "_", 1)) %>% 
  distinct(site_prefix)

# Second part of the site code should start with a capital; letter
site_data %>% 
  mutate(site_prefix = substr(str_split_i(plot_id, "_", 2),1,1)) %>% 
  distinct(site_prefix)

# Drop specimen collection plots. These start with the prefix stmatt_s###
site_data = site_data %>% 
  filter(str_starts(plot_id, "stmatt_s", negate=TRUE))

# Re-project coordinates ----

# Coordinates for St Matthew: 60.42° N, 172.74° W
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
# Add option to skip plotting so that I don't have to register my API key every session.
plot = "FALSE"

if (plot == "TRUE") {
  # Register Google API key. API key can be obtained from: https://mapsplatform.google.com/ Keys & Credentials
  
  # register_google(key=)
  
  # Define spatial extent
  data_extent = terra::ext(site_sf_project)
  
  # Download basemap
  basemap = ggmap::get_map(
    c(
      left = as.numeric(data_extent$xmin),
      bottom = as.numeric(data_extent$ymin),
      right = as.numeric(data_extent$xmax),
      top = as.numeric(data_extent$ymax)
    ),
    maptype = "hybrid",
    zoom=10 # Make sure there is no warning about values outside the scale range when mapping
  )
  
  # Plot locations
  # Coordinate system warning can be ignored
  ggmap(basemap) +
    geom_sf(data = site_sf_project, aes(color="#ccbb44"), 
            inherit.aes=FALSE, 
            na.rm=FALSE, show.legend=FALSE) +
    xlab("Longitude") +
    ylab("Latitude")
  
}


# Format plot dimensions ----
unique(environment_original$plot_dimensions_code)

site_data_join = environment_original %>% 
  select(plot_id, plot_dimensions_code) %>% 
  right_join(site_data, by = "plot_id") %>% 
  mutate(plot_dimensions_m = case_when(plot_dimensions_code == "10m radius" ~ "10 radius",
                                       plot_dimensions_code == "5x10m" ~ "5×10",
                                       .default = "unknown"))

unique(site_data_join$plot_dimensions_m) # No sites listed as unknown

# Format site code ----
# Drop year from site code since this will be redundant once date is added to create a site visit code
site_data_join = site_data_join %>% 
  mutate(site_code = str_remove(plot_id, "_2018"))

# Populate remaining columns ----
site_data_final = site_data_join %>%
  mutate(establishing_project_code = 'fws_stmatthew_2018',
         perspective = "ground",
         cover_method = "semi-quantitative visual estimate",
         h_datum = "NAD83",
         longitude_dd = round(X, digits = 5),
         latitude_dd = round(Y, digits = 5),
         h_error_m = signif(gps_accuracy_m, digits = 4),
         positional_accuracy = "consumer grade GPS",
         location_type = "targeted")%>% 
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(site_data_final, is.na)
    , sum))

# Are error values within a reasonable range?
unique(site_data_final$h_error_m)

# Export as CSV ----
write_csv(site_data_final, site_output)

# Clear workspace ----
rm(list=ls())