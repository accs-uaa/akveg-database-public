# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format ACCS Alphabet Hills Site Visit data to match minimum standards.
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2022-10-13
# Usage: Script should be executed in R 4.0.0+.
# Description: "Format ACCS Alphabet Hills Site Visit data to match minimum standards" renames fields, converts personnel initials to full names,  creates additional fields, and enforces NODATA values on the ACCS Alphabet Hills dataset.
# ---------------------------------------------------------------------------

rm(list=ls())

# Load packages ----
library(readxl)
library(tidyverse)

# Define directories ----
drive <- "F:"
root_folder <- file.path(drive,"ACCS_Work/Projects/AKVEG_Database")
data_folder <- file.path(root_folder, "Data/Data_Plots")
project_folder <- file.path(data_folder, "29_accs_alphabethills")

# Define inputs ----
input_site <- file.path(project_folder, "source", "2021_AlphabetHills_Data.xlsx")
input_initials <- file.path(project_folder, "temp_files", "observer_codes.csv")
input_template <- file.path(root_folder, "Data/Data_Entry","03_Site_Visit.xlsx")

# Define outputs ----
output_visit <- output_site <- file.path(project_folder, "temp_files","03_accs_alphabethills.csv")

# Read in data ----
site_data <- read_xlsx(path=input_site,sheet="Site")
template <- colnames(read_xlsx(path=input_template))
obs_codes <- read_csv(input_initials)

# Convert initials to observer names ----
# I can't imagine there isn't a better way to do this?!
# Only use observer_1 as the veg_observer. Each of the people fields can only accept 1 person.
observer <- site_data %>% 
  select(observer_1) %>% 
  left_join(obs_codes, by = c("observer_1" = "initials")) %>% 
  rename(veg_observer = full_name) %>% 
  select(veg_observer)

recorder <- site_data %>% 
  select(recorder) %>% 
  left_join(obs_codes, by = c("recorder" = "initials")) %>% 
  rename(veg_recorder = full_name) %>% 
  select(veg_recorder)

observers <- cbind(observer,recorder)

site_data <- cbind(site_data,observers)
rm(observer,recorder,observers,obs_codes)

# Create site visit id ----
site_data$date <- as.character(site_data$date)
site_data$date_string <- str_replace_all(site_data$date,pattern="-",replacement="")
site_data$site_visit_id <- paste("ALPH",site_data$site,site_data$date_string,sep="_")

site_visit <- site_data %>% 
  rename(observe_date = date) %>% 
  mutate(project_code = "accs_alphabethills",
         data_tier = "map development & verification",
         scope_vascular = "partial",
         scope_bryophyte = "none",
         scope_lichen = "none",
         env_observer = veg_recorder,
         soils_observer = "NULL",
         structural_class = "n/assess",
         site_code = paste("ALPH",site_data$site,sep="_")) %>%
  select(all_of(template))

# Export data ----
write_csv(site_visit,output_visit)