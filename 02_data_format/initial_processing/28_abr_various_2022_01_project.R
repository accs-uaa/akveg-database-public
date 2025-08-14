# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Project Table for ABR 2022 data.
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2022-11-02
# Usage: Code chunks must be executed sequentially in R version 4.2.1+.
# Description: "Format Project Table for ABR 2022 data" formats project names, parses start and end dates, and creates additional columns to match the "Project" table in the AKVEG database.
# ---------------------------------------------------------------------------

rm(list=ls())

# Load packages ----
library(readxl)
library(tidyverse)
library(lubridate)

# Define directories ----
drive <- "F:"
root_folder <- file.path(drive,"ACCS_Work/Projects/AKVEG_Database")
data_folder <- file.path(root_folder, "Data/Data_Plots")
project_folder <- file.path(data_folder, "27_ABR_2022")

# Define inputs ----
input_veg <- file.path(project_folder, "export_tables","tnawrocki_deliverable_two_veg.xlsx")
input_project <- file.path(project_folder, "export_tables", "tnawrocki_deliverable_two_ref_project.xlsx")
input_template <- file.path(root_folder, "Data/Data_Entry","01_Project.xlsx")

# Define outputs ----
output_project <- file.path(project_folder, "temp_files","01_abr_2022.csv")

# Read in data ----
veg_data <- read_xlsx(path=input_veg,na="NULL")
project <- read_xlsx(path=input_project)
template <- colnames(read_xlsx(path=input_template))

# Create project names ----
project <- project %>% 
  rename(project_name = title) %>% 
  mutate(project_name = case_when(grepl(project_name, pattern="Drill Site 3S") ~ "Drill Site 3S (Palm) Development Area",
                                  project$project_id=="17-107" ~ "2017 Willow Integrated Terrain Unit",
                                  project$project_id=="18-167" ~ "2018 Willow Integrated Terrain Unit",
                                  grepl(project_name, pattern="Reconnaissance survey") ~ "Reconnaissance Survey of Seismic Trail Impacts on the Colville River Delta",
                                  TRUE ~ project_name))

project$project_name <- project$project_name %>%
  str_replace_all(c("ITU" = "Integrated Terrain Unit", 
                    "NPRA" = "National Petroleum Reserve-Alaska", 
                    "ANWR" = "Arctic National Wildlife Refuge"))

# Obtain dates ----
# Parse year for veg_data
# Date/time is wrong for one of the plots (per the notes and also b/c it just doesn't make sense) - drop entire row
project_years <- veg_data %>% 
  filter(!(veg_field_start_ts == "1969-12-31 14:18:40.931-10")) %>% 
  mutate(year = year(as.Date(veg_field_start_ts))) %>% 
  group_by(project_id) %>% 
  summarize(year_start = min(year, na.rm=TRUE),
            year_end = max(year, na.rm=TRUE))

# Two projects span multiple years: project_id 02-166 and 736. 02-166 has multiple entries for both years so I'm assuming that checks out. I'm *assuming* 736 also checks out based on this citation by Jorgenson et al. 2001: Jorgenson, M.T., and J.E. Roth. 2001. Reconnaissance Survey and Monitoring of Seismic Trail Impacts on the Colville River Delta, 1997-1998. Prepared for Phillips Alaska Inc., Anchorage, AK, by ABR, Inc. Fairbanks, AK. (which matches up w/ coordinates)

# Populate remaining columns ----
project <- project %>% 
  mutate(originator = "ABR, Inc.",
         funder = case_when(client=="CPAI" ~ "ConocoPhillips Alaska Inc.",
                            grepl(project$client, pattern="Hilcorp") ~ "Hilcorp Alaska, LLC",
                            client=="AEA" ~ "Alaska Energy Authority",
                            client=="USFWS" ~ "U.S. Fish and Wildlife Service"),
         manager = "unknown",
         completion = "finished",
         year_start = project_years$year_start,
         year_end = project_years$year_end,
         project_code = project_id) %>% 
  select(all_of(template))

# Export CSV ----
write_csv(project,output_project)
