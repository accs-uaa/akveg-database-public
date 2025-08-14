# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Whole Tussock Cover for ACCS Nelchina 2023 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2022-10-17
# Usage: Must be executed in R version 4.3.1+.
# Description: "Format Whole Tussock Cover for ACCS Nelchina 2023 data" appends unique site visit identifier and keeps relevant columns to match the format of the AKVEG database.
# ---------------------------------------------------------------------------

rm(list=ls())

# Load packages ----
library(dplyr)
library(readr)
library(readxl)
library(stringr)
library(tidyr)

# Define directories ----
drive <- "C:"
project_folder <- file.path(drive,"ACCS_Work/Projects")
db_folder <- file.path(project_folder,"AKVEG_Database", "Data")
template_folder <- file.path(db_folder, "Data_Entry")
data_folder <- file.path(project_folder, "Caribou_Nelchina","Data", "Summer 2023")
output_folder <- file.path(db_folder,"Data_Plots","36_accs_nelchina_2023")

# Define inputs ----
input_tussock <- file.path(data_folder, "07_accs_nelchina_tussock_2023.xlsx")
input_template <- file.path(template_folder, "07_Whole_Tussock_Cover.xlsx")
input_site_visit <- file.path(output_folder, "03_accs_nelchina_2023.csv")

# Define outputs ----
output_tussock <- file.path(output_folder, "07_accs_nelchina_2023.csv")

# Read in data ----
tussock_data <- read_xlsx(path=input_tussock)
template <- colnames(read_xlsx(path=input_template))
site_visit <- read_csv(input_site_visit)

# Format site visit data ----
site_visit <- site_visit %>% 
  select(site_code, site_visit_code)

# Format tussock data ----
tussock_data <- tussock_data %>%
  mutate(site_code = str_replace_all(site_code, 
                                     pattern ="NLC", replacement = "NLC2023")) %>% 
  left_join(site_visit,by="site_code") %>% # Append site visit code
  select(all_of(template)) # Keep only required columns

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(tussock_data, is.na)
    , sum)
)

# Verify total number of sites (n=80)
tussock_data %>% distinct(site_visit_code) %>% nrow()

# Verify range of values
tussock_data %>% distinct(cover_type)
summary(tussock_data$tussock_percent_cover)

# Export data ----
write_csv(tussock_data,file=output_tussock)

# Clean workspace ----
rm(list=ls())