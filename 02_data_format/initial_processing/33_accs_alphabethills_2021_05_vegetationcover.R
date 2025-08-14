# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format ACCS Alphabet Hills cover data.
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2022-10-19
# Usage: Script should be executed in R 4.0.0+.
# Description: "Format ACCS Alphabet Hills cover data" adds site_visit_id and performs final QA/QC to ensure table matches requirements for the "Vegetation Cover" table in AK VEG Database. 
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
input_cover <- file.path(project_folder, "source","2021_AlphabetHills_PlotCover_20221019.csv")
input_template <- file.path(root_folder, "Data/Data_Entry","05_Vegetation_Cover.xlsx")
input_sites <- file.path(project_folder,"03_accs_alphabethills.xlsx")

# Define outputs ----
output_cover <- file.path(project_folder, "temp_files","05_accs_alphabethills.csv")

# Read in data ----
cover_data <- read_csv(input_cover)
template <- colnames(read_xlsx(path=input_template))
site_data <- read_xlsx(path=input_sites)

# Append site_visit_id ----
# For cover_data, add "ALPH_" prefix to "site" to match site_data formatting
site_data <- site_data %>% 
  select(site_visit_id,site_code)

cover_data <- cover_data %>% 
  mutate(site_code = paste("ALPH",site,sep="_")) %>% 
  left_join(site_data,by="site_code")

# QA/QC ----
# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(cover_data, is.na)
    , sum)
)

# Final formatting ----
cover_data <- cover_data %>%
  select(all_of(template))

# Export data ----
write_csv(cover_data,output_cover)