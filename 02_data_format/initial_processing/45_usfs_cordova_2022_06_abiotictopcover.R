# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Abiotic Top Cover for USFS Cordova Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-05-09
# Usage: Must be executed in R version 4.4.0+.
# Description: "Calculate Abiotic Top Cover for USFS Cordova Data" uses data from surveys conducted by the U.S. Forest Service to calculate abiotic top cover percent. The script standardizes naming conventions for abiotic elements and renames columns to match formatting in the AKVEG database.
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
template_folder = path(project_folder, 'Data','Data_Entry')
plot_folder = path(project_folder, 'Data/Data_Plots/45_usfs_cordova_2022')
workspace_folder = path(plot_folder, 'working')
source_folder = path(plot_folder, 'source')

# Define input datasets
site_visit_input = path(plot_folder,"03_sitevisit_usfscordova2022.csv")
abiotic_cover_input = path(source_folder,'CRD_Veg_Final_ADB_08112023.xlsx')
vegetation_cover_input = path(plot_folder, '05_vegetationcover_usfscordova2022.csv')
site_codes_input = path(workspace_folder, 'site_codes_cordova.csv')
template_input = path(template_folder, "06_abiotic_top_cover.xlsx")

# Define output dataset
abiotic_cover_output = path(plot_folder, '06_abiotictopcover_usfscordova2022.csv')

# Read in data ----
site_visit_original = read_csv(site_visit_input)
abiotic_original = read_xlsx(abiotic_cover_input, sheet="GROUND_COVER_TYPE", 
                             range = "A1:E400", col_names = c("site_id", "visit_date",
                                                                "cover_code", "cover_percent",
                                                                "cover_code2"))
vegetation_cover = read_csv(vegetation_cover_input)
site_codes = read_csv(site_codes_input)
template = colnames(read_xlsx(path=template_input))

# Obtain site visit codes ----

# Link site visit codes original site codes using site code table
site_visit = site_visit_original %>% 
  left_join(site_codes,by=c("site_code"="new_site_code")) %>% 
  select(original_code_alternate,
         site_code,site_visit_code)

# Join site visit code to cover data
# Exclude all sites which did not match with a site code
abiotic_data = abiotic_original %>%
  left_join(site_visit, by=c("site_id"="original_code_alternate")) %>% 
  filter(!is.na(site_code))

# Drop entries with "unknown" cover_code. Not enough information to include in database
abiotic_data = abiotic_data %>% 
  filter(!(cover_code == "UNKN"))

# Format percent cover ----

# Convert to numeric
abiotic_data$abiotic_top_cover_percent = as.numeric(abiotic_data$cover_percent)
summary(abiotic_data$abiotic_top_cover_percent)

# Change entries listed as 0% to 0.1% (minimum in AKVEG)
abiotic_data = abiotic_data %>% 
  mutate(abiotic_top_cover_percent = case_when(abiotic_top_cover_percent == 0 ~ 0.1,
                                   .default = abiotic_top_cover_percent))

# Format abiotic element codes ----
table(abiotic_data$cover_code, abiotic_data$cover_code2)

# Rename codes to match AKVEG constrained values
abiotic_data = abiotic_data %>%
  mutate(abiotic_element = case_when(cover_code == "WATE" ~ "water",
                                     cover_code == "LITT" ~ "litter (< 2 mm)",
                                     cover_code == "GRAV" ~ "rock fragments",
                                     cover_code == "ROCK" ~ "rock fragments",
                                     cover_code == "BARE" ~ "soil",
                                    .default = "ERROR"))

# Ensure that all codes have been reclassified
abiotic_data %>% 
  filter(abiotic_element == "ERROR")

# Summarize percent cover ----
# For each site, group entries with the same abiotic element to calculate total percent cover for that element
abiotic_data = abiotic_data %>% 
  group_by(site_visit_code, abiotic_element) %>% 
  summarise(abiotic_top_cover_percent = sum(abiotic_top_cover_percent)) %>% 
  ungroup()

# Select only required columns ----
abiotic_data = abiotic_data %>%
  select(all_of(template))

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(abiotic_data, is.na)
    , sum)
)

# Ensure values for % cover are reasonable
summary(abiotic_data$abiotic_top_cover_percent) # Minimum should be 0.1

# Ensure that all ground cover codes match a constrained value in AKVEG
table(abiotic_data$abiotic_element)

# Ensure abiotic top cover and vegetation cover sum to 100%
# Minimum total cover is 90%, maximum is 107%. I can live with that.
vegetation_cover = vegetation_cover %>% 
  group_by(site_visit_code) %>% 
  summarise(vegetation_total_cover = sum(cover_percent))

temp = abiotic_data %>%
  group_by(site_visit_code) %>% 
  summarise(abiotic_total_cover = sum(abiotic_top_cover_percent)) %>% 
  full_join(vegetation_cover, by="site_visit_code") %>% 
  group_by(site_visit_code) %>% 
  summarise(total_cover = sum(abiotic_total_cover, 
                              vegetation_total_cover, na.rm = TRUE)) %>% 
  arrange(total_cover)

# Export as CSV ----
write_csv(abiotic_data,abiotic_cover_output)

# Clear workspace ----
rm(list=ls())