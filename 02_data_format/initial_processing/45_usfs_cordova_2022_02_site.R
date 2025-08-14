# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Table for USFS Cordova Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-05-09
# Usage: Must be executed in R version 4.4.0+.
# Description: "Format Site Table for USFS Cordova Data" uses data from surveys conducted by the U.S. Forest Service's Chugach National Forest to extract relevant site-level information for ingestion into the AKVEG Database. The script omits sites that are missing coordinates or cover data (vegetation or abiotic). The script also creates standardized site codes and populates required metadata. It depends upon the output from the corresponding script in the datum_conversion folder.
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
plot_folder = path(project_folder, 'Data/Data_Plots/45_usfs_cordova_2022')
template_folder = path(project_folder, "Data/Data_Entry")
workspace_folder = path(plot_folder, 'working')
source_folder = path(plot_folder, 'source')

# Define input datasets
coordinates_input = path(workspace_folder,"site_glacier_cordova_coordinates.csv") 
cordova_original_input = path(source_folder,'CRD_Veg_Final_ADB_08112023.xlsx')
template_input = path(template_folder, "02_site.xlsx")

# Define output datasets
site_cordova_output = path(plot_folder, '02_site_usfscordova2022.csv')
site_codes_output = path(workspace_folder, 'site_codes_cordova.csv')

# Read in data ----
coords_cordova = read_csv(coordinates_input)
site_original_cordova = read_xlsx(cordova_original_input,
                                  sheet="01_NRT_DX_SITE_GENERAL",range="B2:B1001", 
                                  col_names = "id_site")
cover_veg_cordova = read_xlsx(cordova_original_input, sheet="11_NRT_DX_OC_PLANT_COVER",
                        range="A1:H3780")
cover_abiotic_cordova = read_xlsx(cordova_original_input, sheet="GROUND_COVER_TYPE",
                            range="A1:E202", , col_names = c("id_cover", "visit_date",
                                                             "cover_code", "cover_percent",
                                                             "cover_code2"))
template = colnames(read_xlsx(path=template_input))

# Drop sites with insufficient data ----
# Sites must have coordinates and either vegetation or abiotic cover data

# For veg cover, drop entries for which life form & plant code are both unknown
cover_veg_cordova = cover_veg_cordova %>% 
  rename("id_cover" = "SITE_ID") %>% 
  mutate(exclude_unknowns = case_when(LIFE_FORM == "UN" & grepl(pattern="other",x=PLANT_CODE,ignore.case=TRUE) ~ "TRUE",
                                      LIFE_FORM == "UN" & is.na(PLANT_CODE) ~ "TRUE",
                                      .default = "FALSE")) %>% 
  filter(exclude_unknowns == "FALSE") %>% 
  filter(!is.na(COVER_PERCENT)) %>% 
  select(id_cover)

# For abiotic cover, drop entries with "unknown" cover_code
cover_abiotic_cordova = cover_abiotic_cordova  %>% 
  filter(!(cover_code == "UNKN"))

# Combine list of sites that have veg or abiotic cover data
cover_cordova = bind_rows(cover_abiotic_cordova, cover_veg_cordova)

cover_cordova = cover_cordova %>% 
  filter(!is.na(id_cover)) %>%
  distinct(id_cover) %>% 
  arrange(id_cover)

# Which of these sites do not have coordinates?
print(cover_cordova[which(!(cover_cordova$id_cover %in% coords_cordova$SITE_ID_1)),], n = 100) # Emailed our USFS collaborator about these sites to see if they have the coordinates

# Drop sites without coordinates 
cover_cordova = cover_cordova %>% 
  filter(cover_cordova$id_cover %in% coords_cordova$SITE_ID_1)

# Drop 7 additional sites with insufficient data (vegetation + abiotic cover <90%)
cover_cordova = cover_cordova %>% 
  filter(!(id_cover %in% c("2019-CRD-COR_0971", "2019-CRD-COR_0973", 
                           "2019-CRD-COR_1060", "2019-CRD-COR_0769", 
                           "2019-CRD-COR_0975", "2019-CRD-COR_0972", 
                           "2019-CRD-COR_1017")))
         
# Create new site codes ----
# New site codes will match formatting of USFS Kenai sites that are already in the AKVEG database 
# Begin with site prefix: CRD2019

# Rename sites that exist in the 'site' sheet under a different name
# Keep both names so that they can easily be linked to each other if needed
cover_cordova[which(!(cover_cordova$id_cover %in% site_original_cordova$id_site)),]

site_cordova = site_original_cordova %>%
  mutate(
    id_alternate = case_when(
      id_site == "2019CRD GALENA SWEETGALE MUSKEG" ~ "2019-CRD-GALENA SWEETGA MUSK",
      id_site == "2019-CRD_Copper_New_015" ~ "2019-CRD_Copper_New_15",
      id_site == "2019-CRD_SimpsonBay_0535alternate" ~ "2019-CRD_SimpsonBay_0535alt",
      id_site == "2019CRD Mountain Hemlock Galena 1" ~ "2019CRD Mtn Hemlock Galena1",
      id_site == "2019CRD Port Etches Opportunistic 6" ~ "2019CRD Port Etches Opp 6",
      id_site == "2019CRD- Opportunistic Bligh Island 2" ~ "2019CRD- Opp Bligh Island 2",
      id_site == "2019CRD. Port Etches Opportunistic 1" ~ "2019CRD. Port Etches Opp 1",
      id_site == "2019CRD. Port Etches Opportunistic 2" ~ "2019CRD. Port Etches Opp 2",
      id_site == "2019CRD. Port Etches Opportunistic 3" ~ "2019CRD. Port Etches Opp 3",
      id_site == "2019CRD. Zaikof Opportunistic 2" ~ "2019CRD.Zaikof Opp2",
      .default = id_site)) %>% 
  filter(id_alternate %in% cover_cordova$id_cover)

# Create new site codes starting with prefix 'CRD2019'
site_cordova = site_cordova %>%
  arrange(id_site) %>% 
  mutate(site_number = row_number(),
         site_number = str_pad(site_number,width=4,pad="0",side="left"),
         new_site_code = str_c("CRD2019", site_number, sep="")) %>% 
  select(-site_number) %>% 
  rename(original_code = id_site,
         original_code_alternate = id_alternate)

# Save as separate object for export
# Table will be used to link old site ID to new site code
site_codes_link = site_cordova

# Format coordinates ----

# Add coordinates data to site table
site_cordova = site_cordova %>% 
  left_join(coords_cordova, by = c("original_code_alternate" = "SITE_ID_1"))

# Ensure every site has a coordinate
site_cordova %>% 
  filter(is.na(POINT_X) | is.na(POINT_Y))

# Rename latitude and longitude columns, restrict to 5 decimal places, populate correct datum
site_cordova = site_cordova %>%
  mutate(latitude_dd = round(POINT_Y, digits = 5),
         longitude_dd = round(POINT_X, digits = 5),
         h_datum = "NAD83")

# Populate remaining columns ----
# Assume plot radius is 50 feet (same as GRD data)
# Assume sites were ground sites (information on tree height is available for many of them)
site_cordova = site_cordova %>% 
  rename(site_code = new_site_code) %>% 
  mutate(establishing_project_code = "usfs_cordova_2022",
         plot_dimensions_m = "15 radius",
         perspective = "ground",
         cover_method = "semi-quantitative visual estimate",
         h_error_m = -999,
         positional_accuracy = "consumer grade GPS",
         location_type = "targeted") %>% 
  select(all_of(template))

# Export as CSV ----
write_csv(site_cordova, site_cordova_output)
write_csv(site_codes_link, site_codes_output)

# Clear workspace ----
rm(list=ls())