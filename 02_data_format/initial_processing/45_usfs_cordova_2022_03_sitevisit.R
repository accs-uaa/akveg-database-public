# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Visit Table for USFS Cordova Ranger District Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-02-12
# Usage: Must be executed in R version 4.4.0+.
# Description: "Format Site Visit Table for USFS Cordova Ranger District Data" uses data from surveys conducted in the U.S. Forest Service's Cordova Ranger District to extract relevant site-level information for ingestion into the AKVEG Database. The script adds a site visit code, formats information about personnel, reclassifies data on plant community type to structural class, and adds required fields with appropriate values and unknown codes.
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
workspace_folder = path(plot_folder, 'working')
template_folder = path(project_folder, "Data/Data_Entry")
source_folder = path(plot_folder, 'source')

# Define input datasets
site_visit_input = path(source_folder,'CRD_Veg_Final_ADB_08112023.xlsx')
site_codes_input = path(workspace_folder, 'site_codes_cordova.csv')
manual_reclass_input = path(workspace_folder, 'structural_class_cordova.csv')
template_input = path(template_folder, "03_site_visit.xlsx")

# Define output dataset
site_visit_output = path(plot_folder, '03_sitevisit_usfscordova2022.csv')
analysis_output = path(workspace_folder,'03_sitevisit_analysis_usfscordova2022.csv')

# Read in data ----
site_visit_original = read_xlsx(site_visit_input,sheet="01_NRT_DX_SITE_GENERAL")
structural_class_original = read_xlsx(site_visit_input,sheet="0")
manual_reclass = read_csv(manual_reclass_input)
site_codes = read_csv(site_codes_input)
template = colnames(read_xlsx(path=template_input))

# Restrict to sites with sufficient data ----
# Sites with sufficient data are listed in the site_codes table
# select_if code from @cole: https://forum.posit.co/t/drop-all-na-columns-from-a-dataframe/5844/3
site_visit = site_visit_original %>% 
  right_join(site_codes, by = c("SITE_ID" = "original_code")) %>% 
  rename(site_code = new_site_code) %>% 
  select_if(function(x){!all(is.na(x))}) # Drop columns for which all observations are NAs.

# Ensure that all observations are associated with a (AKVEG-generated) site code
site_visit %>% 
  filter(is.na(site_code))

# Format date & create site visit codes ----

# Summarize visit dates to ensure range of values is reasonable
summary(site_visit$VISIT_DATE) # Ensure there are no NAs
hist(site_visit$VISIT_DATE,breaks="months")

# Convert visit date to character string
site_visit$observe_date = as.character(as.Date(site_visit$VISIT_DATE))

# Combine visit date with site code to generate site visit code
site_visit = site_visit %>% 
  mutate(date_string = str_remove_all(observe_date, pattern="-"),
         site_visit_code = str_c(site_code,date_string,sep="_")) 

# Format observer names ----
# Assume that observer 1 is the 'veg_observer' and observer 2 is the 'veg_recorder'. Ignore other personnel for now. As far as I know, data on environmental & soils were not recorded as part of this data collection effort.
site_visit = site_visit %>% 
  mutate(veg_observer = str_c(PERSON_FIRST_NM1, PERSON_LAST_NM1, sep = " "),
         veg_recorder = str_c(PERSON_FIRST_NM2, PERSON_LAST_NM2, sep = " ")) %>% 
  # Correct typo in staff names
  mutate(veg_observer = case_when(veg_observer == "Elizabeth Camarate" ~ "Elizabeth Camarata",
                                  .default = veg_observer),
         veg_recorder = case_when(veg_recorder == "Daniel Shmalzer" ~ "Daniel Schmalzer",
                                  veg_recorder == "Elizabether Camarata" ~ "Elizabeth Camarata",
                                  veg_recorder == "James Lanni" ~ "James Ianni",
                                  .default = veg_recorder)) %>% 
  # For sites that do not have any personnel data, list observer/recorder as "unknown"
  mutate(veg_observer = case_when(is.na(veg_observer) ~ "unknown",
                                  .default = veg_observer),
         veg_recorder = case_when(is.na(veg_recorder) ~ "unknown",
                                  .default = veg_recorder))

# Format structural class data ----

# Obtain site visit code first by joining with site_codes link (structural class table uses alternate id codes), then by linking with site_visit table
structural_class = structural_class_original %>% 
  left_join(site_codes,by=c("site_id...3" = "original_code_alternate")) %>% 
  left_join(site_visit,by=c("new_site_code" = "site_code")) %>% 
  select(original_code_alternate,
         site_visit_code, site_notes, dom_lf, other_dom_type, open_closed, plant_comms,
         other_com_type) %>% 
  filter(!is.na(site_visit_code))

# Explore USFS categories
table(structural_class$dom_lf,structural_class$other_dom_type)

# Reclassify 'dominant types' to match constrained 'structural class' values in AKVEG
# Begin by appending manual reclassification table. These are sites whose structural class could not be determined based on the data in the structural class sheet alone
structural_class = structural_class %>% 
  left_join(manual_reclass, by = c("original_code_alternate" = "original_code")) %>% 
  mutate(structural_class = case_when(grepl("Alder",other_dom_type) ~ "tall shrub",
                                      other_dom_type == "Aquatic Herbaceous" ~ "aquatic forb",
                                      dom_lf == "broadleaf_forest" ~ "broadleaf forest",
                                      other_dom_type == "Dry Herbaceous" ~ "grass meadow",
                                      other_dom_type == "Dwarf Mountain Hemlock" ~ "dwarf needleleaf forest",
                                      other_dom_type == "Ericaceous Dwarf Shrub" ~ "dwarf shrub",
                                      dom_lf == "needleleaf_forest" ~ "needleleaf forest",
                                      plant_comms == "CAMA11" ~ "sedge meadow",
                                      other_dom_type == "Mesic Herbaceous" 
                                      & grepl("CACA4", plant_comms) ~ "grass meadow",
                                      other_dom_type == "Mesic Herbaceous" 
                                      & plant_comms == "EPAN2" ~ "forb meadow",
         other_dom_type == "Mesic Herbaceous" & plant_comms == "LAMA3" ~ "forb meadow",
         other_dom_type == "Sedge Peatland" & plant_comms == "CAPA19" ~ "sedge emergent",
         other_dom_type == "Sedge Peatland" & plant_comms == "CAPL6" ~ "sedge emergent",
         other_dom_type == "Sedge Peatland" & grepl("CASI3", plant_comms) ~ "sedge emergent",
         other_dom_type == "Sedge Peatland" & grepl("^ERAN6", plant_comms) ~ "sedge emergent",
         other_dom_type == "Sedge Peatland" & plant_comms == "ERRU2" ~ "sedge emergent",
         other_dom_type == "Sedge Peatland" & plant_comms == "FACR_TRCA30" ~ "bog meadow",
         other_dom_type == "Sedge Peatland" & plant_comms == "TRCA30" ~ "sedge emergent",
         other_dom_type == "Sitka Spruce - Black Cottonwood" ~ "mixed forest",
         other_dom_type == "Sparse Vegetation" ~ "barrens or partially vegetated",
         other_dom_type == "Sweetgale" ~ "low shrub",
         plant_comms == "CAAQD" ~ "sedge emergent",
         plant_comms == "CALY3" ~ "sedge emergent",
         plant_comms == "ELPA3" ~ "sedge emergent",
         plant_comms == "EQFL" ~ "forb emergent",
         plant_comms == "GLPA6" ~ "graminoid emergent",
         plant_comms == "POEG" ~ "forb emergent",
         plant_comms == "POPA14" ~ "forb emergent",
         dom_lf == "shrub" & plant_comms == "RUSP_ATFI" ~ "low shrub",
         is.na(dom_lf) & other_dom_type == "Black Cottonwood" ~ "broadleaf forest",
         is.na(dom_lf) & other_dom_type == "Sitka Spruce" ~ "needleleaf forest",
         is.na(dom_lf) & other_dom_type == "Western Hemlock" ~ "needleleaf forest",
         is.na(structural_class) ~ "not available",
         .default = structural_class))

# Ensure that all sites have been assigned a structural class
structural_class %>% 
  filter(structural_class == "not available")

# Parse out comment fields ----

# Some of the comments in the structural_data table allow us to assess whether or not the site was homogeneous
structural_class = structural_class %>% 
  mutate(homogeneous = case_when(grepl("not homogenous",site_notes,
                                      ignore.case=TRUE) ~ "FALSE",
                                .default = "TRUE"))

# Join with site visit data
site_visit = site_visit %>% 
  left_join(structural_class, by = "site_visit_code")

# Populate remaining metadata ----
site_visit = site_visit %>% 
  mutate(project_code = "usfs_cordova_2022",
         data_tier = "vegetation classification",
         env_observer = "NULL",
         soils_observer = "NULL",
         scope_vascular = "top canopy",
         scope_bryophyte = "partial",
         scope_lichen = "none")

# Create dataset for AKVEG export
site_visit_akveg = site_visit %>% 
  select(all_of(template))

# Create more comprehensive dataset for USFS CNF Revegetation analysis
site_visit_full = site_visit %>% 
  select(all_of(template),other_dom_type) %>% 
  rename(dominance_type = other_dom_type)

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(site_visit_akveg, is.na)
    , sum)
)

# Ensure that there are no typos or missing data in the personnel names
site_visit_akveg %>% 
  filter(is.na(veg_observer) | is.na(veg_recorder)) %>% 
  distinct(site_code)

table(site_visit_akveg$veg_observer)
table(site_visit_akveg$veg_recorder)

# Ensure that all "unknown" veg_observers are paired with an "unknown" veg_recorder
site_visit_akveg %>% 
  filter(veg_observer == "unknown") %>% 
  distinct(veg_recorder)

# Check structural class values
unique(site_visit_akveg$structural_class)

# Check that homogeneous column is Boolean
unique(site_visit_akveg$homogenous)

# Export as CSV ----
write_csv(site_visit_akveg,site_visit_output)
write_csv(site_visit_full,analysis_output)

# Clear workspace ----
rm(list=ls())
