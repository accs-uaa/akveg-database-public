# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Site and Site Visit Tables for BLM AIM data.
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-01-19
# Usage: Code chunks must be executed sequentially in R version 4.2.1+.
# Description: "Format Site and Site Visit Tables for BLM AIM data" selects projects to keep, renames columns, adds required metadata, and selects specific columns to match the Site and Site Visit tables in the AKVEG Database.
# ---------------------------------------------------------------------------

rm(list=ls())

# Load packages ----
library(tidyverse)

# Define directories ----
drive = "F:"
root_folder = "ACCS_Work/Projects/AKVEG_Database"
data_folder = file.path(drive, root_folder, "Data/Data_Plots")
project_folder = file.path(data_folder, "32_aim_various_2021")

# Define inputs ----
input_site = file.path(project_folder, "export_tables", "export_tbl_terradat_AKonly.csv")
input_lpi = file.path(project_folder, "export_tables", "export_tbl_LPI_Header.csv")
input_soils = file.path(project_folder, "export_tables", "export_tbl_Soil_Pits.csv")
input_project = file.path(project_folder,"01_aim_blm.xlsx")

# Define outputs ----
file_name_site = paste0("02_aim_blm_",str_replace_all(Sys.Date(),pattern="-",replacement=""),".csv")
file_name_visit = paste0("03_aim_blm_",str_replace_all(Sys.Date(),pattern="-",replacement=""),".csv")
output_site = file.path(project_folder, "temp_files",file_name_site)
output_visit = file.path(project_folder, "temp_files",file_name_visit)
output_reckey = file.path(project_folder, "temp_files","site_visit_to_reckey.csv")

# Read in data ----
site = read_csv(file=input_site)
lpi = read_csv(file=input_lpi)
soils = read_csv(file=input_soils)
project = read_xlsx(input_project)

# Simplify datasets ----
# Select columns to keep
keep_site = c("PrimaryKey", "PlotID","ProjectName","EcologicalSiteId","Latitude_NAD83","Longitude_NAD83","DateEstablished","DateVisited","Design")
keep_lpi = c("Observer","Recorder","PrimaryKey","RecKey")
keep_soils = c("Observer","PrimaryKey")

# Select projects to keep
projects_to_include = c("Alaska Anchorage FO 2018","ALASKA_GMT2_2021","KobukSeward-WEST 2020","KobukSeward-NORTHEAST 2021")

site = site %>% 
  select(all_of(keep_site)) %>% 
  filter(ProjectName %in% projects_to_include)

lpi = lpi %>% 
  select(all_of(keep_lpi)) %>% 
  rename(veg_observer = Observer,
         veg_recorder = Recorder)

soils = soils %>% 
  select(all_of(keep_soils)) %>% 
  rename(soils_observer = Observer)

site = left_join(site, lpi, by="PrimaryKey")
site = left_join(site, soils,by="PrimaryKey")

# For GMT-2, mirror formatting of site code for other plots listed in AKVEG
site$PlotID = str_replace_all(string=site$PlotID,pattern=" ", replacement="")

site = site %>% 
  mutate(PlotID = if_else(site$ProjectName=="ALASKA_GMT2_2021",str_pad(PlotID,3,side="left",pad="0"),site$PlotID),
         site_code = if_else(site$ProjectName=="ALASKA_GMT2_2021",paste("GMT2",site$PlotID,sep="-"),site$PlotID))

# Prepare Site data (table #2) ----

# Add missing columns, rename existing one
aim_site = site %>% 
  rename(latitude_dd = Latitude_NAD83,
         longitude_dd = Longitude_NAD83,
         location_type = Design) %>% 
  mutate(cover_method = "line-point intercept",
         h_datum = "NAD83",
         h_error_m = 3,
         positional_accuracy = "consumer grade GPS",
         plot_dimensions = "30",
         scope_vascular = "exhaustive",
         scope_bryophyte = "non-trace species",
         scope_lichen = "non-trace species") %>% 
  select(site_code,cover_method,h_datum,latitude_dd,longitude_dd,h_error_m,positional_accuracy, plot_dimensions, scope_vascular, scope_bryophyte, scope_lichen, location_type)

# Change location_type to lowercase
aim_site$location_type = str_to_lower(aim_site$location_type)

# Prepare Site Visit data (table #3) -----

# Format ProjectName to match the name that's in the Project datasheet
project_codes = project$project_code
project_names = unique(site$ProjectName)

aim_visit = site %>% 
  mutate(site_visit_id = "",
         project_code = case_when(site$ProjectName == project_names[1] ~ project_codes[1],
                                                 site$ProjectName == project_names[2] ~ project_codes[2],
                                                 site$ProjectName == project_names[3] ~ project_codes[3],
                                                 site$ProjectName == project_names[4] ~ project_codes[4]),
         data_tier = if_else(project_code=="AIM Anchorage FO","map development & verification",
                             "ecological land classification"),
         observe_date = if_else(project_code=="AIM Anchorage FO", 
                                as.character(as.Date(DateEstablished, format="%m/%d/%Y")), 
                                as.character(as.Date(DateVisited, format="%m/%d/%Y"))),
         env_observer = soils_observer,
         structural_class = "n/a") %>% 
  select(site_visit_id,project_code, site_code, data_tier, observe_date, veg_observer, veg_recorder, env_observer, soils_observer, structural_class,RecKey)

# Populate site_visit_id
visit_dates = str_replace_all(aim_visit$observe_date, pattern="-", replacement="")
aim_visit$site_visit_id =paste(aim_visit$site_code,visit_dates,sep="_")

# Create site_visit_id to RecKey reference
visit_to_reckey = aim_visit %>% 
  select(site_visit_id,RecKey)

aim_visit = aim_visit %>% select(-RecKey)

# Export as CSV ----
# For aim_visit: In Excel, import the CSV as text and then specify 'text' (instead of General) for the observe_date column.
write_csv(aim_site,output_site,na="")
write_csv(aim_visit,output_visit,na="")
write_csv(visit_to_reckey,output_reckey,na="")