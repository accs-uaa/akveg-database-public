# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Project Table for BLM AIM data.
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2022-11-02
# Usage: Code chunks must be executed sequentially in R version 4.2.1+.
# Description: "Format Project Table for BLM AIM data" selects which projects to keep, formats project names, parses start and end dates, and creates additional columns to match the "Project" table in the AKVEG database.
# ---------------------------------------------------------------------------

rm(list=ls())

# Load packages ----
library(tidyverse)
library(lubridate)

# Define directories ----
drive <- "F:"
root_folder <- file.path(drive, "ACCS_Work/Projects/AKVEG_Database")
data_folder <- file.path(root_folder, "Data/Data_Plots")
project_folder <- file.path(data_folder, "28_aimBLM_2021")

# Define inputs ----
input_site <- file.path(project_folder, "export_tables", "export_tbl_terradat_AKonly.csv")
input_template <- file.path(root_folder, "Data/Data_Entry","01_Project.xlsx")

# Define outputs ----
file_name <- paste0("01_aim_blm_",str_replace_all(Sys.Date(),pattern="-",replacement=""),".csv")
output_project <- file.path(project_folder, "temp_files",file_name)

# Read in data ----
site <- read_csv(file=input_site, col_select = c("PrimaryKey","ProjectName","DateEstablished"))
template <- colnames(read_xlsx(path=input_template))

# Simplify datasets ----

# Select projects to keep
projects_to_include <- c("Alaska Anchorage FO 2018","ALASKA_GMT2_2021","KobukSeward-WEST 2020","KobukSeward-NORTHEAST 2021")

site <- site %>%
  filter(ProjectName %in% projects_to_include) %>% 
  mutate(DateEstablished = as.Date(DateEstablished, format="%m/%d/%Y"))

project <- site %>% 
  group_by(ProjectName) %>% 
  summarise(year_start = year(min(DateEstablished)),
            year_end = year(max(DateEstablished))) %>% 
  rename(project_name = ProjectName) %>% 
  mutate(project_name = c("Anchorage Field Office Assessment, Inventory, and Monitoring", 
                          "GMT-2 Assessment, Inventory, and Monitoring",
                          "Kobuk-Seward Northeast Assessment, Inventory, and Monitoring",
                          "Kobuk-Seward West Assessment, Inventory, and Monitoring"),
    project_code = c("AIM Anchorage FO", "AIM GMT-2","AIM Kobuk-Seward NE", "AIM Kobuk-Seward W"),
    originator = c("Alaska Center for Conservation Science", "Alaska Center for Conservation Science", "ABR, Inc.","ABR, Inc."),
         funder = "Bureau of Land Management",
    manager = c("Jeanne Osnas", "Anjanette Steer", "Aaron F. Wells", "Aaron F. Wells"),
    completion = "finished") %>% 
  select(all_of(template))

# Export as CSV ----
write_csv(project,output_project,na="")