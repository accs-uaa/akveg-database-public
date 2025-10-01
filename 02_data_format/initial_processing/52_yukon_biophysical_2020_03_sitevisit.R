# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Visit Table for Yukon Biophysical Inventory System Plots"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-09-30
# Usage: Must be executed in R version 4.5.1+.
# Description: "Format Site Visit Table for Yukon Biophysical Inventory System Plots" formats information about site visits for ingestion into the AKVEG Database. The script formats dates, creates site visit codes, parses personnel names, re-classifies structural class data, and populates required metadata. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr, warn.conflicts = FALSE)
library(fs)
library(lubridate, warn.conflicts = FALSE)
library(readr)
library(readxl)
library(stringr)
library(tidyr)

# Define directories ----

# Set root directory
drive <- "C:"
root_folder <- "ACCS_Work"

# Define input folders
project_folder <- path(drive, root_folder, "OneDrive - University of Alaska", "ACCS_Teams", "Vegetation", "AKVEG_Database", "Data")
plot_folder <- path(project_folder, "Data_Plots", "52_yukon_biophysical_2020")
source_folder <- path(plot_folder, "source", "ECLDataForAlaska_20240919", "YBIS_Data")
template_folder <- path(project_folder, "Data_Entry")

# Define datasets ----

# Define inputs
visit_input <- path(source_folder, "Plot_2024Apr09.xlsx")
site_input <- path(plot_folder, "02_site_yukonbiophysical2020.csv")
template_input <- path(template_folder, "03_site_visit.xlsx")

# Define output
visit_output <- path(plot_folder, "03_sitevisit_yukonbiophysical2020.csv")

# Read in data ----
site_original <- read_csv(site_input, col_select = c("site_code", "establishing_project_code", "perspective"))
visit_original <- read_xlsx(visit_input, range = "A1:AH15124")
template <- colnames(read_xlsx(path = template_input))

# Format site code ----
## Mirror formatting used in processed site table
visit_data <- visit_original %>%
  mutate(site_code = str_c(`Project ID`, `Plot ID`, sep = "_")) %>%
  right_join(site_original, by = "site_code") ## Drop sites that aren't included in site table

## Ensure all site codes have a match in the site table
print(which(!(visit_data$site_code %in% site_original$site_code)))

# Format date & site visit code ----

# Format date
visit_data <- visit_data %>%
  mutate(observe_date = as.Date(visit_data$`Survey date`, format = "%Y %b %d"))

# Ensure date range is reasonable
summary(visit_data$observe_date)
unique(month(visit_data$observe_date))

# Create site visit code
visit_data <- visit_data %>%
  mutate(
    observe_date = as.character(observe_date),
    date_string = str_replace_all(observe_date, "-", ""),
    site_visit_code = paste(site_code, date_string, sep = "_")
  )

head(visit_data$site_visit_code)

# Populate remaining columns ----
visit_final <- visit_data %>%
  rename(project_code = establishing_project_code) %>%
  mutate(
    data_tier = "map development & verification",
    veg_observer = "unknown",
    veg_recorder = "unknown",
    env_observer = "unknown",
    soils_observer = case_when(grepl("Soil", Observers) ~ "unknown",
      .default = "none"
    ),
    scope_vascular = case_when(
      perspective == "ground" ~ "exhaustive",
      perspective == "aerial" ~ "top canopy"
    ),
    scope_bryophyte = case_when(
      perspective == "ground" ~ "common species",
      perspective == "aerial" ~ "category"
    ),
    scope_lichen = case_when(
      perspective == "ground" ~ "common species",
      perspective == "aerial" ~ "category"
    ),
    homogeneous = "TRUE",
    structural_class = case_when(`Vegetation Structure` == "2b" ~ "bryoid herbaceous",
      `Vegetation Structure` == "3a" & !is.na(`Wetland kind level`) ~ "forb emergent",
      `Vegetation Structure` == "3b" & !is.na(`Wetland kind level`) ~ "graminoid emergent",
      `Vegetation Structure` == "3c" ~ "aquatic forb",
      `Vegetation Structure` == "4a" ~ "tall shrub",
      `Vegetation Structure` == "4b" ~ "low shrub",
      .default = "not available"
    )
  ) %>%
  select(all_of(template))

# Do any of the columns have null values that need to be addressed?
print(cbind(
  lapply(
    lapply(visit_final, is.na),
    sum
  )
))

# Export as CSV ----
write_csv(visit_final, visit_output)

# Clear workspace ----
rm(list = ls())
