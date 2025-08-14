# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Visit Table for USFS Glacier Ranger District Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-02-12
# Usage: Must be executed in R version 4.4.0+.
# Description: "Format Site Visit Table for USFS Glacier Ranger District Data" uses data from surveys conducted in the U.S. Forest Service's Glacier Ranger District to extract relevant site-level information for ingestion into the AKVEG Database. The script adds a site visit code, formats information about personnel, reclassifies existing data on vegetation type to create a structural class variable, and adds required fields with appropriate values and unknown codes.
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
workspace_folder = path(plot_folder, 'working')
template_folder = path(project_folder, "Data/Data_Entry")
source_folder = path(plot_folder, 'source')

# Define input datasets
site_visit_input = path(source_folder,'GRD_AccessDB_08182023.xlsx')
site_codes_input = path(workspace_folder, 'site_codes_glacier.csv')
template_input = path(template_folder, "03_site_visit.xlsx")

# Define output dataset
site_visit_output = path(plot_folder, '03_sitevisit_usfsglacier2023.csv')
analysis_output = path(workspace_folder,'03_sitevisit_analysis_usfsglacier2023.csv')

# Read in data ----
site_visit_original = read_xlsx(site_visit_input, sheet="01_NRT_DX_SITE_GENERAL")
structural_class_original = read_xlsx(site_visit_input, 
                                  sheet="14_NRT_DX_SITE_CLASSIFICATION")
site_codes = read_csv(site_codes_input)
template = colnames(read_xlsx(path=template_input))

# Restrict to sites with sufficient data ----
# Sites with sufficient data are listed in the site_codes table
site_visit = site_visit_original %>% 
  right_join(site_codes, by = c("SITE_ID" = "original_code")) %>% 
  rename(site_code = new_site_code)

# Drop columns for which all observations are NAs. This helps us to see what columns actually contain information that might be pertinent to the Site Visit table
# Solution from @cole: https://forum.posit.co/t/drop-all-na-columns-from-a-dataframe/5844/3
site_visit = site_visit %>% 
  select_if(function(x){!all(is.na(x))})

# Append structural class data
site_visit = site_visit %>% 
  left_join(structural_class_original, by="SITE_ID")

# Ensure that all observations are associated with a (AKVEG-generated) site code
site_visit %>% 
  filter(is.na(site_code))

# Format date & create site visit codes ----

# Summarize visit dates to ensure range of values is reasonable
summary(site_visit$VISIT_DATE.x) # Ensure there are no NAs
hist(site_visit$VISIT_DATE.x,breaks="months")

# Convert visit date to character string
site_visit$observe_date = as.character(site_visit$VISIT_DATE.x)

# Combine visit date with site code to generate site visit code
site_visit = site_visit %>% 
  mutate(date_string = str_remove_all(observe_date, pattern="-"),
         site_visit_code = str_c(site_code,date_string,sep="_")) 

# Format observer names ----
# Assume that observer 1 is the 'veg_observer' and observer 2 is the 'veg_recorder'. Ignore other personnel for now. As far as I know, data on environmental & soils were not recorded as part of this data collection effort.
site_visit = site_visit %>% 
  mutate(veg_observer = str_c(PERSON_FIRST_NM1, PERSON_LAST_NM1, sep = " "),
         veg_recorder = str_c(PERSON_FIRST_NM2, PERSON_LAST_NM2, sep = " ")) %>% 
  # Correct typo in one of the staff's last names
  mutate(veg_recorder = case_when(veg_recorder == "Alex Cataudell" ~ "Alex Cataudella",
                                  .default = veg_recorder)) %>% 
  # For sites that do not have any personnel data, list observer/recorder as "unknown"
  mutate(veg_observer = case_when(is.na(veg_observer) ~ "unknown",
                                  .default = veg_observer),
         veg_recorder = case_when(is.na(veg_recorder) ~ "unknown",
                                  .default = veg_recorder))

# Format structural class ----
# For now, list all of the classes that are not easily attributable to an AKVEG constrained value as "not available". Classes are: Aquatic Herbaceous, Dry Herbaceous, Forested Peatland, Sedge Peatland, Wet Herbaceous, and Mesic Herbaceous. Would need to look at dominant plant cover for each site to make a decision.
unique(site_visit$CLASS_SHORT_NAME)

site_visit = site_visit %>% 
  mutate(structural_class = case_when(CLASS_SHORT_NAME == "Alder" ~ "tall shrub",
                                      CLASS_SHORT_NAME == "Hemlock" ~ "needleleaf forest",
                                      CLASS_SHORT_NAME == "Hemlock - Yellow Cedar" ~ "needleleaf forest",
                                      CLASS_SHORT_NAME == "Hemlock - Sitka Spruce" ~ "needleleaf forest",
                                      CLASS_SHORT_NAME == "Dwarf Mountain Hemlock" ~ "dwarf needleleaf forest",
                                      CLASS_SHORT_NAME == "Sweetgale" ~ "low shrub",
                                      CLASS_SHORT_NAME == "Alder-Willow" ~ "tall shrub",
                                      CLASS_SHORT_NAME == "Sitka Spruce" ~ "needleleaf forest",
                                      CLASS_SHORT_NAME == "Sitka Spruce - Black Cottonwood" ~ "mixed forest",
                                      CLASS_SHORT_NAME == "Ericaceous Dwarf Shrub" ~ "dwarf shrub",
                                      CLASS_SHORT_NAME == "Sparse Vegetation" ~ "barrens or partially vegetated",
                                      .default = "not available"))

# Ensure that values were correctly reclassified
table(site_visit$structural_class)
table(site_visit$structural_class, site_visit$CLASS_SHORT_NAME)

# Populate remaining metadata ----
site_visit = site_visit %>% 
  mutate(project_code = "usfs_glacier_2023",
         data_tier = "vegetation classification",
         env_observer = "NULL",
         soils_observer = "NULL",
         scope_vascular = "top canopy",
         scope_bryophyte = "partial",
         scope_lichen = "none",
         homogeneous = "TRUE")

# Create dataset for AKVEG export
site_visit_akveg = site_visit %>% 
  select(all_of(template))

# Create more comprehensive dataset for USFS CNF Revegetation analysis
site_visit_full = site_visit %>% 
  select(all_of(template),CLASS_SHORT_NAME) %>% 
  rename(dominance_type = CLASS_SHORT_NAME)

# QA/QC ----

# Ensure that there are no typos or missing data in the personnel names
site_visit_akveg %>% 
  filter(is.na(veg_observer) | is.na(veg_recorder)) %>% 
  distinct(site_code)

table(site_visit_akveg$veg_observer)
table(site_visit_akveg$veg_recorder)

# Ensure that there is never a situation where veg_observer is "unknown" but veg_recorder is present
site_visit_akveg %>% 
  filter(veg_observer == "unknown") %>% 
  distinct(veg_recorder)

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(site_visit_akveg, is.na)
    , sum)
)

# Check structural class values
unique(site_visit_akveg$structural_class)

# Check that homogeneous column is Boolean
unique(site_visit_akveg$homogeneous)

# Export as CSV ----
write_csv(site_visit_akveg,site_visit_output)
write_csv(site_visit_full,analysis_output)

# Clear workspace ----
rm(list=ls())

