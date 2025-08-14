# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Visit Table for USFWS Pribilof Islands 2022 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-08-05
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Site Visit Table for USFWS Pribilof Islands 2022 data" formats information about site visits for ingestion into the AKVEG Database. The script creates site visit codes, parses personnel names, and populates required columns. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(stringr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/38_fws_pribilof_2022')
source_folder = path(plot_folder, 'source')
template_folder = path(project_folder, "Data/Data_Entry")

# Define input datasets
site_visit_input = path(source_folder, 'pribilofs2022.xlsx')
template_input = path(template_folder, "03_site_visit.xlsx")

# Define output dataset
site_visit_output = path(plot_folder, '03_sitevisit_fwspribilof2022.csv')

# Read in data ----
site_visit_original = read_xlsx(site_visit_input,
                                range="A1:AN102")
template = colnames(read_xlsx(path=template_input))

# Drop sites in human-dominated environments ----
# 6 sites to drop
site_visit_data = site_visit_original %>% 
  filter(!grepl("Anthroscape", Avc_descrp))

# Format site code ----
# Replace dash with underscore
site_visit_data = site_visit_data %>% 
  mutate(site_code = str_replace_all(`Releve number`, "-", "_"))

print(site_visit_data$site_code) # Check that codes look good

# Format date & site visit code ----
site_visit_data = site_visit_data %>% 
  mutate(observe_date = paste(Year, Month, Day, sep = "-"),
         site_visit_code = paste(site_code, `Date (year/month/day)`, sep = "_"))

# Ensure that date range is reasonable
hist(as.Date(site_visit_data$observe_date), breaks = "day", xlab = "Survey Date") 
summary(as.Date(site_visit_data$observe_date))

# Format personnel names ----
unique(site_visit_data$Observers)

site_visit_data = site_visit_data %>% 
  mutate(veg_observer = "Hunter Gravley",
         veg_recorder = case_when(grepl("Erin", Observers) ~ "Erin Lefkowitz",
                                   .default = "Hunter Gravley"),
         env_observer = "Hunter Gravley",
         soils_observer = "none")

# Ensure values were correctly translated
unique(site_visit_data$veg_observer) # Should only be 1 name
unique(site_visit_data$env_observer) # Should only be 1 name
table(site_visit_data$Observers, site_visit_data$veg_recorder)

# Format structural class ----
site_visit_data$`Cover open water (%)` = as.numeric(site_visit_data$`Cover open water (%)`)

site_visit_data = site_visit_data %>% 
  mutate(cover_rock = `Cov_bed drock` + `Cov_boulder > 100 cm` + 
           `Cov_talus 20 - 100 cm` + `Cov_scree < 20 cm`,
         cover_moss = Cov_sphagnum + `Cov_feather moss` + `Cov_other moss`,
         cover_vegetation = `Cov_dwarf shrub` + Cov_grass + Cov_forb + Cov_sedge + cover_moss) %>% 
  mutate(structural_class = case_when(cover_rock > cover_vegetation ~ "barrens or partially vegetated",
                                      Cov_sand > 90 ~ "barrens or partially vegetated",
                                      `Cov_mineral soil` > 90 ~ "barrens or partially vegetated",
                                      `Cov_dwarf shrub` >= 20 ~ "dwarf shrub",
                                      Cov_sedge > `Cov_dwarf shrub` & 
                                        Cov_sedge > Cov_forb & 
                                        Cov_sedge > Cov_grass & 
                                        grepl("hy", Moist_regi) ~ "sedge emergent",
                                      Cov_sedge > `Cov_dwarf shrub` & 
                                        Cov_sedge > Cov_forb & 
                                        Cov_sedge > Cov_grass & 
                                        Cov_sedge > cover_moss ~ "sedge meadow",
                                      Cov_grass > `Cov_dwarf shrub` & 
                                        Cov_grass > Cov_forb & 
                                        Cov_grass > Cov_sedge & 
                                        Cov_grass > cover_moss &
                                        (grepl("mesic", Moist_regi) | grepl("subhygric", Moist_regi)) ~ "grass meadow",
                                      Cov_forb > `Cov_dwarf shrub` & 
                                        Cov_forb > Cov_sedge & 
                                        Cov_forb > Cov_grass & 
                                        Cov_forb > cover_moss ~ "forb meadow",
                                      grepl("marine", Avc_descrp, ignore.case = TRUE) & `Cover open water (%)` > 90 ~ "coastal water",
                                      grepl("Aquatic", Avc_descrp) & 
                                        (grepl("Lake", Remarks) | grepl("Pond", Remarks)) &
                                        !grepl("Ephemeral", Remarks) ~ "lake water",
                                      Avc_code == "IIIC1b" ~ "bryoid herbaceous",
                                      site_code == "STP22_044" | site_code == "STG22_046" ~ "forb meadow",
                                      
                                      site_code == "STG22_014" ~ "forb meadow",
                                      site_code == "STG22-028" ~ "forb meadow",
                                      site_code == "STP22_042" ~ "barrens or partially vegetated",
                                      .default = "not available"
                                      ))

# Ensure all sites are associated with a structural class
site_visit_data %>% filter(structural_class == "not available")

# Format homogeneous column ----
site_visit_data = site_visit_data %>% 
  mutate(homogenous = case_when(site_code == "STP22_044" | site_code == "STG22_046" ~ "FALSE",
                                grepl("IIIA2?", Avc_code) ~ "FALSE",
                                grepl("IIIA2?", Avc_descrp) ~ "FALSE",
                                .default = "TRUE"))

# Format scope ----
site_visit_data = site_visit_data %>% 
  mutate(scope_bryophyte = case_when(`Mosses identified (y/n)` == "N" ~ "none",
                                     `Mosses identified (y/n)` == "Y" ~ "common"),
         scope_lichen = case_when(`Lichens identified (y/n)` == "N" ~ "none",
                                  `Lichens identified (y/n)` == "Y" ~ "common"),
         scope_vascular = "exhaustive")

# Check values
unique(site_visit_data$scope_bryophyte) # Should only be none (no 'Y' in original dataset)
unique(site_visit_data$scope_lichen) # Should only be none

# Populate remaining columns ----
site_visit_final = site_visit_data %>% 
  mutate(project_code = "fws_pribilof_2022",
         data_tier = "map development & verification") %>% 
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(site_visit_final, is.na)
    , sum))

# Verify personnel names
unique(site_visit_final$veg_observer)
unique(site_visit_final$veg_recorder)
unique(site_visit_final$env_observer)
unique(site_visit_final$soils_observer)

# Verify that all structural class values match a constrained value
table(site_visit_final$structural_class)

# Export as CSV ----
write_csv(site_visit_final, site_visit_output)

# Clear workspace ----
rm(list=ls())