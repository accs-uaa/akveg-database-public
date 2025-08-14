# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Site Visit Table for ACCS 2023 Nelchina data.
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-08-07
# Usage: Code chunks must be executed sequentially in R version 4.4.1+.
# Description: "Format Site Visit Tables for ACCS 2023 Nelchina data" parses data from a Survey123 form and combines it with manually entered data. It also creates site visit codes, renames columns, and selects specific columns to match the Site Visit table in the AKVEG Database. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import libraries ----
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
plot_folder = path(project_folder, 'Data/Data_Plots/36_accs_nelchina_2023')
source_folder = path(plot_folder, 'source')
template_folder = path(project_folder, "Data/Data_Entry")

# Define inputs ----
input_survey123 = path(source_folder, "Survey123_Site.xlsx")
input_manual_entry = path(source_folder, "Nelchina_2023_Site.xlsx")
input_template = path(template_folder, "03_Site_Visit.xlsx")
input_site = path(plot_folder, '02_site_accsnelchina2023.csv')

# Define output ----
output_visit = path(plot_folder,"03_sitevisit_accsnelchina2023.csv")

# Read in data ----
survey123_original = read_xlsx(path=input_survey123)
manual_entry_original = read_xlsx(path=input_manual_entry)
site_original = read_csv(input_site)
template = colnames(read_xlsx(path=input_template))

# Format manually entered data ----
manual_entry_data = manual_entry_original %>%
  mutate(project_code = 'accs_nelchina_2023',
         date_string = str_replace_all(observe_date, pattern="-", replacement=""),
         site_visit_code=paste(site_code,date_string,sep="_"),
         structural_class = str_to_title(structural_class),
         homogenous = "TRUE") %>% 
  select(all_of(template))

# Format Survey123 data ---- 
survey123_data = survey123_original %>%
  rename(data_tier = `Data Tier`,
         observe_date = observed_date,
         veg_observer = `Veg. Observer`,
         veg_recorder = `Veg. Recorder`,
         env_observer = `Env. Observer`,
         soils_observer = `Soils Observer`,
         structural_class = `Structural Class`,
         scope_vascular = `Vascular Scope`,
         scope_bryophyte = `Bryophyte Scope`,
         scope_lichen = `Lichen Scope`) %>% 
  mutate(project_code = "accs_nelchina_2023",
         site_code = str_remove(`Site Code`, "2023"),
         date_string = str_replace_all(observe_date, pattern="-", replacement=""),
         site_visit_code=paste(site_code,date_string,sep="_"),
         structural_class = str_to_title(structural_class),
         homogenous = "TRUE") %>% 
  select(all_of(template))
  
# Append dataframes ----
site_visit_final = survey123_data %>% 
  bind_rows(manual_entry_data) %>% 
  arrange(site_code)

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(site_visit_final, is.na)
    , sum))

unique(site_visit_final$project_code)
unique(str_split_i(site_visit_final$site_code, pattern = "_", 1))
unique(site_visit_final$scope_lichen)
unique(site_visit_final$scope_bryophyte)
unique(site_visit_final$scope_vascular)
table(site_visit_final$structural_class)
unique(site_visit_final$veg_observer)
unique(site_visit_final$veg_recorder)

# Do the site codes match with the ones in the Site table?
which(site_visit_final$site_code != site_original$site_code)
which(site_original$site_code != site_visit_final$site_code)

# Export data ----
write_csv(x=site_visit_final,file=output_visit)

# Clear workspace ----
rm(list=ls())
