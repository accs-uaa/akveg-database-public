# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Table for Yukon Biophysical Plots data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-12
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Site Table for Yukon Biophysical Plots data" formats site-level information for ingestion into the AKVEG Database. The script drops sites with no cover data or incorrect coordinates, re-projects coordinates to NAD83, and populates fields with values that match constrained values in the AKVEG Database, including translating information on plot dimension, positional accuracy, and horizontal error. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(ggmap)
library(janitor)
library(lubridate)
library(readr)
library(readxl)
library(sf)
library(stringr)
library(terra)
library(tidyr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/51_yukon_biophysical_2015')
source_folder = path(plot_folder, 'source')
template_folder = path(project_folder, "Data/Data_Entry")

# Define input datasets
site_input = path(source_folder, 'yukon_pft_2021', 'raw_data_yukon_biophysical', 'YukonPlotsSince2000.xlsx')
plot_locations_input = path(source_folder, 'Biophysical_plots')
template_input = path(template_folder, "02_site.xlsx")

# Define output datasets
site_yukon_output = path(plot_folder, '02_site_yukonbiophysical2015.csv')

# Read in data ----
site_original = read_xlsx(site_input,
                               sheet = 'Env')
veg_cover_original = read_xlsx(site_input,
                               sheet = 'Veg',
                               col_types = c("text","text","guess",
                                             rep("numeric",26),
                                             rep("guess",15)))
plot_locations_original = read_sf(plot_locations_input,
                                  "Biophysical_plots")
template = colnames(read_xlsx(path=template_input))

# Explore non-spatial data ----

# Ensure that each site code is unique
length(unique(site_original$PlotNumber)) == nrow(site_original) 

# Verify that each code starts with letters, otherwise add a prefix to differentiate from other sites/projects
site_original %>% 
  mutate(site_prefix = substr(PlotNumber,1,2)) %>% 
  distinct(site_prefix) # One site that starts with '83' gets dropped (spatial outlier); no need to correct

# Remove empty/unnecessary rows and columns
site_data = site_original %>%
  remove_empty(which = c("rows", "cols"))

# Drop sites that do not have useable cover data ----

# Format vegetation cover data
veg_cover = veg_cover_original %>%
  replace_na(list(Cover2 = 0, # Replace NA values with 0
                  Cover3 = 0,
                  Cover4 = 0,
                  Cover5 = 0,
                  Cover5a = 0,
                  Cover5b = 0,
                  Cover5c = 0,
                  CoverB = 0,
                  Cover6 = 0,
                  Cover7 = 0)) %>%
  remove_empty(which = c("rows", "cols")) %>% # Remove empty/unnecessary rows and columns
  select(-c(Flag, ID, TotalA, TotalB)) %>%
  mutate(Species = str_to_lower(Species)) %>% # Convert species code to lower case
  pivot_longer(Cover2:Cover7, names_to="vertical_strata", values_to="cover") %>% # Convert to long format
  filter(cover != 0) # Drop entries with 0% cover

# Remove plots with unknown species cover >5% (n=5)
plots_to_exclude = veg_cover %>% 
  filter(Species == "unknown" & cover >5) %>% 
  select(PlotNumber) %>%
  distinct()

veg_cover = veg_cover %>% 
  anti_join(plots_to_exclude, by="PlotNumber")

# Obtain list of unique sites with useable cover data
plots_to_include = veg_cover %>% distinct(PlotNumber)

# Restrict site data to only those sites (n=681)
site_data = semi_join(site_data, plots_to_include, by="PlotNumber")

rm(veg_cover, veg_cover_original, plots_to_exclude, plots_to_include)

# Explore coordinates ----
# Datum is WGS 84, based on spatial reference of plot locations shapefile and Yukon's Field Manual for Describing Yukon Ecosystems 
# Shapefile: https://yukon.maps.arcgis.com/home/item.html?id=36fa31f7490c4966bc9c04095f6933ac
# Field Manual: https://yukon.ca/en/field-manual-describing-yukon-ecosystems
summary(site_data$Latitude)
summary(site_data$Longitude)
summary(site_data$UTMEasting) # Mostly NAs - Do not use.
summary(site_data$UTMNorthing) # Mostly NAs - Do not use.

# Format coordinates ----

# Add negative sign to longitude
site_data = site_data %>% 
  mutate(Longitude = Longitude * -1)

# Convert data to sf object
site_sf = st_as_sf(site_data, 
                   coords = c("Longitude", "Latitude"),
                   na.fail = TRUE,
                   crs = 4326) # WGS84

# Re-project coordinates
# To NAD 83 (EPSG 4269)
site_sf_project = st_transform(x=site_sf, crs=4269)
st_crs(site_sf_project) # Ensure correct EPSG is listed

# Check for spatial outliers
# Add option to skip plotting so that I don't have to register my API key every session
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
  zoom=6 # Trial and error. Make sure there is no warning about values outside the scale range when mapping
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

# Drop sites with incorrect coordinates ----

# Some sites with incorrect locations noted in column "OfficeNotes" 
site_sf_project %>% 
  filter(!is.na(OfficeNotes)) %>% 
  filter(grepl("lat|lon|locat", OfficeNotes, ignore.case=TRUE)) %>% 
  distinct(PlotNumber, OfficeNotes) # Check notes + check that plot numbers are unique within dataset

site_sf_project = site_sf_project %>% 
  filter(!PlotNumber %in% c("HA00469","HA00490","YK02127", "DM2207"))

# Drop outlier at longitude -132.99
# Coordinates are correct based on location description, but plot is in NWT, not YT, and is not included in plot locations shapefile
site_sf_project = site_sf_project %>% 
  filter(PlotNumber != "83RH017")

# Extract coordinates back into df
# Drop old coordinates to avoid confusion
site_data_project = site_data %>% 
  semi_join(site_sf_project,by="PlotNumber") %>% # Carry over dropped sites
  bind_cols(st_coordinates(site_sf_project)) %>% 
  select(-c(Longitude, Latitude))

# Format plot dimensions ----
# See Page 1-10 of the Field Manual
table(site_data_project$PlotSize)

# Plot dimensions of 400 are problematic because they can be either a circular plot (with radius of 11.3 m) or a 20x20m rectangular plot. Assume circular plot for now, choose '11.6 radius' since that is the closest value that exists in the data dictionary
# Additional information not in shapefile
# Drop 1 plot with plot dimensions of '50'.
site_data_project = site_data_project %>%
  mutate(plot_dimensions_m = case_when(PlotSize == 100 ~ "10×10",
                                       PlotSize == 314 ~ "10 radius",
                                       PlotSize == 400 ~ "11.6 radius",
                                       .default = "unknown")) %>% 
  filter(PlotSize != 50 | is.na(PlotSize))

# Format error and positional accuracy ----

# Use information on plot number and project ID to create a common 'site code' column to join plot_locations data with site_data

# Determine unique project IDs present in site data
# Most projects have a simple numeric ID
project_ids = data.frame(original_name = unique(site_data_project$ProjectID))
project_ids = project_ids %>% 
  mutate(project_id = case_when(original_name == "North Coast - 3876" ~ "3876",
                                grepl("^IV", original_name) ~ original_name, # Not sure how to address these
                                .default = substr(original_name,1,3)))

# Determine unique project id/names in shapefile
plot_locations_unique = plot_locations_original %>% 
  distinct(PROJECT_ID, PROJECT_NA)
plot_locations_unique$PROJECT_ID = as.character(plot_locations_unique$PROJECT_ID)

# Check that all project IDs unique
anyDuplicated(plot_locations_unique$PROJECT_ID)

# Join project IDs from both tables
# Project IDs in site data without a match in shapefile seem to be truly missing from shapefile dataset. These are also the ones with non-standard Project IDs and PlotNumbers that do not start with 'YK'
project_ids = project_ids %>% 
  left_join(plot_locations_unique, join_by(project_id == PROJECT_ID))

# Create site code column in shapefile that matches up with PlotNumber in site data
plot_locations = plot_locations_original %>% 
  mutate(site_code = case_when(PROJECT_ID == 212 ~ paste0("YK00",BPH_PLOT_I),
                               PROJECT_ID == 213 ~ paste0("YK0",BPH_PLOT_I),
                               PROJECT_ID == 353 ~ paste0("YK0",BPH_PLOT_I),
                               PROJECT_ID == 355 ~ paste0("YK0",BPH_PLOT_I),
                               PROJECT_ID == 366 ~ paste0("YK0",BPH_PLOT_I),
                               PROJECT_ID == 367 ~ paste0("YK0",BPH_PLOT_I),
                               PROJECT_ID == 375 ~ paste0("YK0",BPH_PLOT_I),
                               PROJECT_ID == 383 ~ paste0("YK0",BPH_PLOT_I),
                               PROJECT_ID == 404 ~ paste0("YK0",BPH_PLOT_I),
                               PROJECT_ID == 405 ~ paste0("YK0",BPH_PLOT_I),
                               PROJECT_ID == 482 ~ paste0("YK0",BPH_PLOT_I),
                               PROJECT_ID == 537 ~ paste0("YK",BPH_PLOT_I),
                               .default = as.character(BPH_PLOT_I)))

# Join to site_data to obtain information on positional accuracy and error
site_data_join = site_data_project %>% 
  left_join(plot_locations, join_by("PlotNumber" == "site_code"))

# Sites starting with 'YK' (n=419) should have matched with an entry in the shapefile
site_data_join %>% 
  filter(grepl("YK",PlotNumber)) %>% 
  filter(!is.na(COORD_SOUR)) %>% 
  nrow() == site_data_join %>% filter(grepl("YK", PlotNumber)) %>% nrow()

# Sites not starting in 'YK' should not have returned a match
site_data_join %>% 
  filter(!grepl("YK",PlotNumber)) %>% 
  filter(is.na(COORD_SOUR)) %>% 
  nrow() # 256 sites do not start with YK; all NA

# Translate information on positional accuracy to correspond with constrained values in AKVEG
unique(site_data_join$COORD_SOUR)
unique(site_data_join$COORD_PREC)

site_data_join = site_data_join %>% 
  mutate(positional_accuracy = case_when(COORD_SOUR == "Navigational uncorrected GPS" ~ "consumer grade GPS",
                                         COORD_SOUR == "Topographic map of unknown scale" ~ "map interpretation",
                                         COORD_SOUR == "Digitized orthophoto 1.0m res" ~ "image interpretation",
                                         COORD_SOUR == "1:50K NTS map" ~ "map interpretation",
                                         PlotNumber == "YK02661" ~ "image interpretation", # Per notes, used Google Earth
                                         lubridate::year(Date) <= 2001 ~ "map interpretation",
                                         lubridate::year(Date) >= 2006 ~ "consumer grade GPS",
                                         .default = "NULL"),
         h_error_m = case_when(PlotNumber=="YK11091" ~ "20", # Originally listed as >100m but no notes to suggest why error would have been so large
                               grepl("^±\\dm", COORD_PREC) ~ str_sub(COORD_PREC,-2,-2),
                               grepl("^±\\d{2}m", COORD_PREC) ~ str_sub(COORD_PREC,-3,-2),
                               COORD_PREC == ">100m" ~ "150",
                               positional_accuracy == "consumer grade GPS" & COORD_PREC == "Unknown" ~ "10",
                               positional_accuracy == "consumer grade GPS" & is.na(COORD_PREC) ~ "10",
                               positional_accuracy == "map interpretation" & is.na(COORD_PREC) ~ "150",
                               positional_accuracy == "map interpretation" & COORD_PREC == "Unknown" ~ "150",
                               PlotNumber == "YK02661" ~ "10", # Same as other image interpretation sites
                               .default = "-999"),
         h_error_m = as.numeric(h_error_m)) %>%
  filter(PlotNumber != "YK02675") # Notes say that coordinates were not recorded and listed coordinates are simply the same as another site that was nearby

# Verify that mutate/case_when worked as expected
table(site_data_join$COORD_SOUR, 
      site_data_join$positional_accuracy, 
      useNA = "always")
table(site_data_join$COORD_PREC, 
      site_data_join$h_error_m, 
      useNA = "always")

table(site_data_join$positional_accuracy, 
      useNA = "always") # Ensure no unknown values
table(site_data_join$h_error_m)

rm(site_data_project, project_ids, plot_locations, plot_locations_unique)

# Populate remaining columns ----
site_data_final = site_data_join %>% 
  rename(site_code = PlotNumber) %>% 
  mutate(establishing_project_code = "yukon_biophysical_2015",
         perspective = "ground", # Review
         cover_method = "semi-quantitative visual estimate", # Assumed from Field Manual
         h_datum = "NAD83",
         location_type = "targeted", # Assumed from Field Manual
         longitude_dd = round(X, digits = 5),
         latitude_dd = round(Y, digits = 5)) %>% 
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(site_data_final, is.na)
    , sum))

# Export as CSV ----
write_csv(site_data_final, site_yukon_output)

# Clear workspace ----
rm(list=ls())
