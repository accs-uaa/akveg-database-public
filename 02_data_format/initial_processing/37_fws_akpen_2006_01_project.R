# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Project table for USFWS Alaska Peninsula 2006 data.
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-08-07
# Usage: Code chunks must be executed sequentially in R 4.4.1+.
# Description: "Format Project Table for USFWS Alaska Peninsula 2006 data" enters and formats project-level information for ingestion into the AKVEG Database. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data', 'Data_Plots', '37_fws_alaskapeninsula_2006')
template_folder = path(project_folder, "Data/Data_Entry")

# Define inputs ----
template_input = path(template_folder,"01_Project.xlsx")

# Define outputs ----
project_output = path(plot_folder, "01_project_fwsakpen2006.csv")

# Read in data ----
template = read_xlsx(path=template_input, col_types = "text")

# Format data ----
project_data = template %>%
  add_row %>% 
  mutate(project_code = "fws_akpen_2006",
         project_name = "Alaska Peninsula Earth Cover",
         originator = "DU",
         funder = "USFWS",
         manager = "Dan Fehringer",
         completion = "finished",
         year_start = 2005,
         year_end = 2006,
         project_description = "Aerial plots collected for the creation of an earth cover map for the Alaska Peninsula region. Project was a multipartner effort among U.S. Fish and Wildlife Service, Ducks Unlimited Inc., and U.S. Department of Defense.",
         private = FALSE)

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(project_data, is.na)
    , sum))

# Export as CSV ----
write_csv(project_data, project_output)

# Clear workspace ----
rm(list=ls())
