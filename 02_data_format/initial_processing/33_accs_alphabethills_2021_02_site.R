# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format ACCS Alphabet Hills Site data to match minimum standards.
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2022-10-13
# Usage: Script should be executed in R 4.0.0+.
# Description: "Format ACCS Alphabet Hills Site data to match minimum standards" renames fields and creates additional fields on the ACCS Alphabet Hills dataset.
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
input_template <- file.path(root_folder, "Data/Data_Entry","02_Site.xlsx")

# Define outputs ----
output_site <- file.path(project_folder, "temp_files","02_accs_alphabethills.csv")

# Read in data ----
site_data <- read_xlsx(path=input_site,sheet="Site")
template <- colnames(read_xlsx(path=input_template))

# Format data ----
site_formatted <- site_data %>% 
  rename(h_datum = datum,
         latitude_dd = latitude,
         longitude_dd = longitude,
         h_error_m = error,
         plot_dimensions_m = `plot dimensions`,
         location_type = `location type`) %>% 
  mutate(site_code = paste("ALPH",site_data$site, sep="_"),
         establishing_project_code = "accs_alphabethills",
         perspective = "ground",
         cover_method = "line-point intercept",
         positional_accuracy = "consumer grade GPS") %>% 
  select(all_of(template))

# Remove 'm' from plot dimensions
site_formatted$plot_dimensions_m <- str_replace_all(site_formatted$plot_dimensions_m,pattern = "m",replacement = "")
site_formatted$plot_dimensions_m <- str_squish(site_formatted$plot_dimensions_m)

# Export CSV ----
write_csv(site_formatted,output_site)