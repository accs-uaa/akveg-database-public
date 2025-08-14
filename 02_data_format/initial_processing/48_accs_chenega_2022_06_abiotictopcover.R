# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Abiotic Top Cover Table for ACCS Chenega Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-03-26
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Abiotic Top Cover Table for ACCS Chenega Data" formats abiotic top cover data collected and entered by ACCS for ingestion into the AKVEG Database. The script ensures values match constrained values, adds missing abiotic elements, and ensures values are within reasonable range. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
plot_folder = path(project_folder, 'Data/Data_Plots/48_accs_chenega_2022')
template_folder = path(project_folder, "Data/Data_Entry")
source_folder = path(plot_folder, 'source')

# Define datasets ----

# Define input datasets
abiotic_cover_input = path(source_folder,'06_Abiotic_Top_Cover_Chenega_2022.xlsx')
site_visit_input = path(plot_folder, '03_sitevisit_accschenega2022.csv')
vegetation_cover_input = path(plot_folder, '05_vegetationcover_accschenega2022.csv')
template_input = path(template_folder, "06_abiotic_top_cover.xlsx")
abiotic_elements_input = path(project_folder, 'Data', 'Tables_Metadata', 'csv', 'ground_element.csv')

# Define output datasets
abiotic_top_cover_output = path(plot_folder, '06_abiotictopcover_accschenega2022.csv')

# Read in data ----
abiotic_cover_original = read_xlsx(abiotic_cover_input,
                                  range="A1:C26")
site_visit_original = read_csv(site_visit_input)
vegetation_cover_original = read_csv(vegetation_cover_input)
template = colnames(read_xlsx(path=template_input))
abiotic_elements_list = read_csv(abiotic_elements_input)

# Format abiotic elements list ----
# Remove entries that refer to ground cover table only
elements_to_exclude = c("boulder","cobble","gravel","stone","organic soil","mineral soil", "biotic")
abiotic_elements_list = abiotic_elements_list %>% 
  filter(!(ground_element %in% elements_to_exclude))

# Obtain site visit code ----

# 4 sites do not have an entry in the abiotic top cover table
length(unique(abiotic_cover_original$site_visit_id))

# Format site code to match new convention
abiotic_top_cover = abiotic_cover_original %>%
  mutate(site_code = str_replace_all(site_visit_id, "CHE_GRN_", "CHEN_")) %>% 
  select(-site_visit_id)

# Join with Site Visit table to obtain site visit code
abiotic_top_cover = site_visit_original %>% 
  select(site_code, site_visit_code) %>% 
  left_join(abiotic_top_cover, by = "site_code") %>% 
  select(-site_code)

# Ensure all entries have a site visit code
abiotic_top_cover %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Ensure all sites (n=18) are present in the dataset
length(unique(abiotic_top_cover$site_visit_code))

# Rename abiotic elements ----
abiotic_top_cover = abiotic_top_cover %>%
  mutate(abiotic_element = str_replace_all(abiotic_element, "_", " "),
         abiotic_element = case_when(abiotic_element == "bedrock" ~ "bedrock (exposed)",
                                     abiotic_element == "dead down wood" ~ "dead down wood (≥ 2 mm)",
                                     abiotic_element == "litter" ~ "litter (< 2 mm)",
                                     abiotic_element == "dead standing" ~ "dead standing woody vegetation",
                                     abiotic_element == "rock" ~ "rock fragments",
                                    .default = abiotic_element))

# Add abiotic elements with 0% cover ----
for (i in 1:length(unique(abiotic_top_cover$site_visit_code))) {
  site_code = unique(abiotic_top_cover$site_visit_code)[i]
  subset_top_cover = abiotic_top_cover %>% filter(site_visit_code == site_code)
  
  # Determine which abiotic elements are not listed at that site
  missing_elements = abiotic_elements_list %>% filter(!(ground_element %in% subset_top_cover$abiotic_element))
  
  # Append missing elements to existing abiotic top cover data
  missing_elements = missing_elements %>% 
    rename(abiotic_element = ground_element) %>% 
    mutate(abiotic_top_cover_percent = 0,
           site_visit_code = site_code) %>% 
    select(site_visit_code, abiotic_element, abiotic_top_cover_percent)
  
  if (i==1) {
    complete_abiotic_cover = bind_rows(subset_top_cover, missing_elements)
  } else {
    complete_abiotic_cover = bind_rows(complete_abiotic_cover, 
                                       subset_top_cover, missing_elements)
  }
}

rm(i, subset_top_cover, site_code, missing_elements, abiotic_top_cover)

# Remove NA that were inserted during join process for sites not originally included in the datasheet (sites for which all abiotic top cover was 0%)
complete_abiotic_cover = complete_abiotic_cover %>% filter(!is.na(abiotic_element))

# Check to see that results are what would you expect
table(complete_abiotic_cover$site_visit_code) # 8 entries for each site (total number of abiotic elements)
table(complete_abiotic_cover$abiotic_element) # 18 entries for each abiotic element (total number of sites)

# Restrict to required columns ----
abiotic_cover_final = complete_abiotic_cover %>% 
  select(all_of(template))

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(abiotic_cover_final, is.na)
    , sum)
)

# Does every site have an entry for each abiotic element?
abiotic_cover_final %>% 
  group_by(site_visit_code) %>% 
  summarize(rows = n()) %>% 
  filter(rows!=nrow(abiotic_elements_list))

# Ensure that sum of abiotic top cover does not exceed 100%
abiotic_cover_final %>% 
  group_by(site_visit_code) %>% 
  summarize(total_abiotic_cover = sum(abiotic_top_cover_percent)) %>% 
  mutate(total_abiotic_cover = round(total_abiotic_cover, digits = 0)) %>% 
  filter(total_abiotic_cover > 100)

# Export as CSV ----
write_csv(abiotic_cover_final, abiotic_top_cover_output)

# Clear workspace ----
rm(list=ls())
