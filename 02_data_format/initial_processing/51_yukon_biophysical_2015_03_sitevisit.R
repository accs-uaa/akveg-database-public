# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Site Visit Table for Yukon Biophysical Plots data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-08-07
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Site Visit Table for Yukon Biophysical Plots data" formats information about site visits for ingestion into the AKVEG Database. The script creates site visit codes, parses personnel names, and populates required columns. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(stringr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
plot_folder = path(project_folder, 'Data/Data_Plots/51_yukon_biophysical_2015')
source_folder = path(plot_folder, 'source')
template_folder = path(project_folder, "Data/Data_Entry")

# Define input datasets
site_visit_input = path(source_folder, 'yukon_pft_2021', 'raw_data_yukon_biophysical', 'YukonPlotsSince2000.xlsx')
site_input = path(plot_folder, '02_site_yukonbiophysical2015.csv')
template_input = path(template_folder, "03_site_visit.xlsx")

# Define output dataset
site_visit_output = path(plot_folder, '03_sitevisit_yukonbiophysical2015.csv')

# Read in data ----
site_visit_original = read_xlsx(site_visit_input,
                               sheet = 'Env')
site_original = read_csv(site_input)
template = colnames(read_xlsx(path=template_input))

# Restrict to sites in site table ----
site_visit = site_visit_original %>% 
  semi_join(site_original, join_by("PlotNumber" == "site_code"))

# Format date & site visit code ----
summary(site_visit$Date) # No NAs, properly formatted
hist(site_visit$Date, breaks = "year")

site_visit = site_visit %>% 
  mutate(observe_date = as.character(Date),
         date_string = str_replace_all(observe_date, "-", ""),
         site_visit_code = paste(PlotNumber, date_string, sep = "_"))

# Format veg observer names ----
unique(site_visit$SiteSurveyor)
unique(site_visit$VegSurveyor)

# Use VegSurveyor column to parse veg_observer
site_visit_staff = site_visit %>%
  mutate(veg_observer = case_when(!is.na(VegSurveyor) ~ str_split_i(VegSurveyor, 
                                                                    pattern = ",", 
                                                                    i = 1),
                                  is.na(VegSurveyor) & (SiteSurveyor == "Scott Smith" | SiteSurveyor == "Catherine Kennedy") ~ SiteSurveyor,
                                  is.na(VegSurveyor) & grepl("Veg", SiteSurveyor) ~ str_split_i(SiteSurveyor, "Veg\\(", i = 2),
                                  is.na(VegSurveyor) & grepl("Plot", SiteSurveyor) ~ str_split_i(str_split_i(SiteSurveyor, pattern = "Plot\\(", i = 2), pattern = ",", i = 1),
                                  .default = "unknown")) %>% 
  mutate(veg_observer = case_when(grepl("Soil", veg_observer) ~ str_split_i(veg_observer, " Soil", i = 1),
                                  .default = veg_observer)) %>% 
  mutate(veg_observer = str_replace_all(veg_observer, "Catherine E. Kennedy", "Catherine Kennedy")) %>% 
  mutate(veg_observer = str_replace_all(veg_observer, "\\)", ""))

# Ensure veg_observer was populated as expected
print(site_visit_staff %>% 
        filter(!is.na(SiteSurveyor) & is.na(VegSurveyor)) %>% 
        distinct(SiteSurveyor, veg_observer), n = 100)

site_visit_staff %>% 
  filter(!is.na(VegSurveyor)) %>% 
  distinct(VegSurveyor, veg_observer)

site_visit_staff %>% 
  filter(is.na(SiteSurveyor) & is.na(VegSurveyor)) %>% 
  distinct(veg_observer) # Should only return unknown

site_visit_staff %>% 
  filter(!is.na(SiteSurveyor) | !is.na(VegSurveyor)) %>% 
  filter(veg_observer=="unknown") # Should not return any matches

# Format veg recorder names ----
site_visit_staff = site_visit_staff %>% 
  mutate(veg_recorder = case_when(veg_observer == "unknown" ~ "unknown",
                                  !is.na(VegSurveyor) & grepl(",", VegSurveyor) ~ 
                                    str_split_i(VegSurveyor, pattern = ", ", 
                                                                    i = 2),
                                  grepl("Veg\\(\\w{3},", SiteSurveyor) ~ str_split_i(str_split_i(SiteSurveyor, "\\)", i=2), ",", 2),
                                  grepl("Plot\\(\\w{3},", SiteSurveyor) & !grepl("Veg",SiteSurveyor) ~ str_split_i(SiteSurveyor,pattern=",",i=2),
                                  grepl("Plot", SiteSurveyor) ~ str_replace(str_split_i(SiteSurveyor, "\\)", i=1), "Plot\\(", ""),
                                  veg_observer != "unknown" ~ veg_observer,
                                  .default = "unknown")) %>% 
  mutate(veg_recorder = case_when(grepl(",", veg_recorder) ~ str_split_i(veg_recorder, ",", i = 1),
                                  .default = veg_recorder)) %>% 
  mutate(veg_recorder = case_when(grepl("\\)", veg_recorder) ~ str_split_i(veg_recorder, "\\)", i = 1),
                                  .default = veg_recorder))

# Verify values
unique(site_visit_staff$veg_recorder)
site_visit_staff %>% filter(veg_observer == "unknown") %>% distinct(veg_recorder) # Should only be unknown

# Format environment observer name
# Obtain from SiteSurveyor column
# Select the second personnel listed in either "Plot", "Veg", or "Soil"
site_visit_staff = site_visit_staff %>%
  mutate(env_observer = case_when(
    grepl("Plot\\([A-Z]+,", SiteSurveyor) ~ str_split_i(str_split_i(SiteSurveyor,
                                                                    pattern="Plot\\([A-Z]+,",
                                                                    i=2),
                                                        pattern="\\)",
                                                        i=1),
    grepl("Veg\\([A-Z]+,", SiteSurveyor) ~ str_split_i(str_split_i(SiteSurveyor,
                                                              pattern="Veg\\([A-Z]+,",
                                                              i=2),
                                                  pattern="\\)",
                                                  i=1),
    grepl("Soil\\([A-Z]+,", SiteSurveyor) ~ str_split_i(str_split_i(SiteSurveyor,
                                                             pattern="Soil\\([A-Z]+,",
                                                             i=2),
                                                 pattern="\\)",
                                                 i=1),
    .default = "unknown")) %>% 
  mutate(env_observer = case_when(grepl(",", env_observer) ~ str_split_i(env_observer, 
                                                                             ",", 
                                                                             i = 1),
                                    .default = env_observer)) %>% 
  mutate(env_observer = str_replace(env_observer, pattern = "^FE$", "FED"), # Correct typos
         env_observer = str_replace(env_observer, pattern = "^CK$", "CEK"),
         env_observer = str_replace(env_observer, pattern = "^R$", "RJM"),
         env_observer = str_replace(env_observer, pattern = "^RJ$", "RJM"),
         env_observer = str_replace(env_observer, pattern = "^KM$", "KMM")
         )

# Verify values
table(site_visit_staff$env_observer)

# Format soil surveyor name
site_visit_staff = site_visit_staff %>% 
  mutate(soils_observer = case_when(!is.na(SoilSurveyor) ~ str_split_i(SoilSurveyor, ",", i = 1),
                                    is.na(SoilSurveyor) & grepl("Soil", SiteSurveyor, ignore.case = TRUE) ~ str_split_i(SiteSurveyor, "Soil\\(", i=2),
                                    .default = "unknown")) %>% 
  mutate(soils_observer = case_when(grepl(",", soils_observer) ~ str_split_i(soils_observer, 
                                                                             ",", 
                                                                             i = 1),
                                    .default = soils_observer)) %>% 
  mutate(soils_observer = str_replace_all(soils_observer, "\\)", "")) %>% 
  mutate(soils_observer = str_replace(soils_observer, pattern = "^R$", "RJM"), # Correct incorrect initials (typo or max field length met?)
         soils_observer = str_replace(soils_observer, pattern = "^C$", "CASS"),
         soils_observer = str_replace(soils_observer, pattern = "^CA$", "CASS"),
         soils_observer = str_replace(soils_observer, pattern = "^NJ$", "NJF"))
         

# Verify values
table(site_visit_staff$soils_observer)

# Convert staff initials to full names ----
# Awaiting feedback from YK team
staff_full_names = data.frame("initials" = c("BAB", 
                                             "BB",
                                             "CASS",
                                             "CEK",
                                             "CK",
                                             "GB",
                                             "JAS",
                                             "JCM",
                                             "KMM",
                                             "MBW",
                                             "NS",
                                             "PSH",
                                             "WHM"), 
                              "full_name" = c("Bruce Bennett", 
                                              "Bruce Bennett",
                                              "C.A.S. Smith",
                                              "Catherine Kennedy",
                                              "Catherine Kennedy",
                                              "Greg Brunner",
                                              "Jennifer Staniforth",
                                              "John Meikle",
                                              "Karen McKenna",
                                              "Marcus Waterreus",
                                              "Nancy Steffen",
                                              "Pippa Seccombe-Hett",
                                              "William MacKenzie"
                                              ))

site_visit_staff = site_visit_staff %>% 
  left_join(staff_full_names, join_by("veg_observer" == "initials")) %>% 
  mutate(full_name = case_when(is.na(full_name) ~ veg_observer,
                               .default = full_name)) %>% 
  select(-veg_observer) %>% 
  rename(veg_observer = full_name) %>% 
  left_join(staff_full_names, join_by("veg_recorder" == "initials")) %>% 
  mutate(full_name = case_when(is.na(full_name) ~ veg_recorder,
                               .default = full_name)) %>% 
  select(-veg_recorder) %>% 
  rename(veg_recorder = full_name) %>% 
  left_join(staff_full_names, join_by("env_observer" == "initials")) %>% 
  mutate(full_name = case_when(is.na(full_name) ~ env_observer,
                               .default = full_name)) %>% 
  select(-env_observer) %>% 
  rename(env_observer = full_name)  %>% 
  left_join(staff_full_names, join_by("soils_observer" == "initials")) %>% 
  mutate(full_name = case_when(is.na(full_name) ~ soils_observer,
                               .default = full_name)) %>% 
  select(-soils_observer) %>% 
  rename(soils_observer = full_name) 

# For now, convert all initials to unknown
site_visit_staff = site_visit_staff %>% 
  mutate(veg_observer = case_when(grepl("^[A-Z][A-Z]+", veg_observer) ~ "unknown",
                                  .default = veg_observer),
         veg_recorder = case_when(grepl("^[A-Z][A-Z]+", veg_recorder) ~ "unknown",
                                  .default = veg_recorder),
         env_observer = case_when(grepl("^[A-Z][A-Z]+", env_observer) ~ "unknown",
                                  .default = env_observer),
         soils_observer = case_when(grepl("^[A-Z][A-Z]+", soils_observer) ~ "unknown",
                                    .default = soils_observer))

# Populate remaining columns ----
site_visit_final = site_visit_staff %>% 
  rename(site_code = PlotNumber) %>% 
  mutate(project_code = "yukon_biophysical_2015",
         data_tier = "map development & verification",
         structural_class = "not available",
         scope_vascular = "exhaustive", # Cover data includes trace species
         scope_bryophyte = "common species",
         scope_lichen = "common species",
         homogenous = "TRUE") %>% 
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(site_visit_final, is.na)
    , sum))

# Verify personnel names
unique(site_visit_final$veg_observer)
unique(site_visit_final$veg_recorder)
unique(site_visit_final$env_observer)
unique(site_visit_final$soils_observer)

# Export as CSV ----
write_csv(site_visit_final, site_visit_output)

# Clear workspace ----
rm(list=ls())
