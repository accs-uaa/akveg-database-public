# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format ACCS Alphabet Hills Environment data to match minimum standards.
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2022-10-13
# Usage: Script should be executed in R 4.0.0+.
# Description: "Format ACCS Alphabet Hills Environment data to match minimum standards" renames fields, creates additional fields, and enforces NODATA values on the ACCS Alphabet Hills dataset.
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
input_envr <- file.path(project_folder, "source","2021_AlphabetHills_Data.xlsx")
input_template <- file.path(root_folder, "Data/Data_Entry","12_Environment.xlsx")
input_sites <- file.path(project_folder,"03_accs_alphabethills.xlsx")

# Define outputs ----
output_envr <- file.path(project_folder, "temp_files","12_accs_alphabethills.csv")

# Read in data ----
envr_data <- read_xlsx(path=input_envr,sheet="Site")
template <- colnames(read_xlsx(path=input_template))
site_data <- read_xlsx(path=input_sites)

# Append site_visit_id ----
# For envr_data, add "ALPH_" prefix to "site" to match site_data formatting
site_data <- site_data %>% 
  select(site_visit_id,site_code)

envr_data <- envr_data %>% 
  mutate(site_code = paste("ALPH",site,sep="_")) %>% 
  left_join(site_data,by="site_code")

# Format data ----
envr_data <- envr_data %>% 
  rename(moisture_regime = moisture) %>% 
  mutate(drainage = "NULL",
         disturbance_severity = "NULL",
         disturbance_time_y = -999,
         depth_water_cm = -999,
         depth_moss_duff_cm = -999,
         depth_restrictive_layer_cm = -999,
         restrictive_type = "NULL",
         microrelief_cm = -999,
         surface_water = "NULL") %>% 
  select(all_of(template))

# Export data ----
write_csv(envr_data,output_envr)