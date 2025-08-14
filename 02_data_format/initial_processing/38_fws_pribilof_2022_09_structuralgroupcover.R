# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Structural Group Cover Table for USFWS Pribilof Islands 2022 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-08-09
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Structural Group Cover Table for USFWS Pribilof Islands 2022 data" formats structural group cover data for ingestion into the AKVEG Database. The script ensures values match constrained values and adds missing structural groups. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
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
plot_folder = path(project_folder, 'Data/Data_Plots/38_fws_pribilof_2022')
source_folder = path(plot_folder, 'source')
template_folder = path(project_folder, "Data/Data_Entry")

# Define datasets ----

# Define input datasets
structural_cover_input = path(source_folder,'pribilofs2022.xlsx')
site_visit_input = path(plot_folder, '03_sitevisit_fwspribilof2022.csv')
template_input = path(template_folder, "09_structural_group_cover.xlsx")
structural_group_input = path(project_folder, 'Data', 'Tables_Metadata', 'csv', 'structural_group.csv')

# Define output datasets
structural_cover_output = path(plot_folder, '09_structuralgroupcover_fwspribilof2022.csv')

# Read in data ----
structural_cover_original = read_xlsx(structural_cover_input)
site_visit_original = read_csv(site_visit_input)
template = colnames(read_xlsx(path=template_input))
structural_group_list = read_csv(structural_group_input)

# Append site visit code ----
site_visit_data = site_visit_original %>% 
  select(site_code, site_visit_code)

structural_cover = structural_cover_original %>% 
  mutate(site_code = str_replace_all(`Releve number`, "-", "_")) %>% # Convert dashes to underscore
  right_join(site_visit_data, by = "site_code") # Use right join to drop 6 anthropogenic sites

# Ensure all entries have a site visit code
structural_cover %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Explore data ----

# Ensure all sites (n=95) are present in the dataset
length(unique(structural_cover$site_visit_code))

# Select only relevant columns
structural_cover = structural_cover %>%
  select(site_visit_code, `Cov_dwarf shrub`:`Cov_other lichens`)

# Convert to long format ----
structural_cover_long = structural_cover %>%
  pivot_longer(`Cov_dwarf shrub`:`Cov_other lichens`, 
               names_to="structural_group", 
               values_to="structural_cover_percent")

# Rename structural group elements ----
unique(structural_cover_long$structural_group)

structural_cover_long = structural_cover_long %>% 
  mutate(structural_group = str_remove_all(structural_group, "Cov_"),
         structural_group = case_when(structural_group == "grass" ~ "grasses",
                                      structural_group == "forb" ~ "forbs",
                                      structural_group == "sedge" ~ "sedges and rushes",
                                      structural_group == "sphagnum" ~ "Sphagnum mosses",
                                      structural_group == "feather moss" ~ "feathermosses",
                                      structural_group == "other moss" ~ "other bryophytes",
                                      structural_group == "light lichens" ~ "light macrolichens",
                                      .default = structural_group))

# Ensure all ground elements correspond with an element in the 'structural group list'
which(!(unique(structural_cover_long$structural_group) %in% structural_group_list$structural_group))

# Add structural group elements with 0% cover ----
for (i in 1:length(unique(structural_cover_long$site_visit_code))) {
  site_code = unique(structural_cover_long$site_visit_code)[i]
  subset_cover = structural_cover_long %>% filter(site_visit_code == site_code)
  
  # Determine which structural elements are not listed at that site
  missing_elements = structural_group_list %>% filter(!(structural_group %in% subset_cover$structural_group))
  
  # Append missing elements to existing ground cover data
  missing_elements = missing_elements %>%
    mutate(structural_cover_percent = 0,
           site_visit_code = site_code) %>% 
    select(site_visit_code, structural_group, structural_cover_percent)
  
  if (i==1) {
    structural_cover_complete = bind_rows(subset_cover, missing_elements)
  } else {
    structural_cover_complete = bind_rows(structural_cover_complete, 
                                      subset_cover, missing_elements)
  }
}

# QA/QC ----

# Ensure only required columns are included in output
structural_cover_complete = structural_cover_complete %>% 
  mutate(structural_cover_type = "absolute canopy cover") %>% 
  select(all_of(template)) %>% 
  arrange(site_visit_code, structural_group, .locale="en") # Specify locale so that capitalized 'Sphagnum' doesn't get listed before everything else

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(structural_cover_complete, is.na)
    , sum)
)

# Do the group cover values match constrained values?
which(!(unique(structural_cover_complete$structural_group) %in% structural_group_list$structural_group))

# Does every site have an entry for each ground element?
structural_cover_complete %>% 
  group_by(site_visit_code) %>% 
  summarize(rows = n()) %>% 
  filter(rows!=nrow(structural_group_list))

# Does every structural group element have an entry for each site?
structural_cover_complete %>% 
  group_by(structural_group) %>% 
  summarize(rows = n()) %>% 
  filter(rows!=length(unique(site_visit_data$site_code)))

# Export as CSV ----
write_csv(structural_cover_complete, structural_cover_output)

# Clear workspace ----
rm(list=ls())
