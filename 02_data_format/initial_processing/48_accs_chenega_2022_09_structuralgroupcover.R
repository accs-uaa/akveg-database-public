# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Structural Group Cover Table for ACCS Chenega Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-23
# Usage: Must be executed in R version 4.4.0+.
# Description: "Format Structural Group Cover Table for ACCS Chenega Data" formats structural group cover data collected and entered by ACCS for ingestion into the AKVEG Database. The script renames values to match constrained values, adds missing structural groups, and corrects estimates where total cover (ground cover less biotic + structural group cover) exceeds 105%. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
structural_cover_input = path(source_folder,'Chenega Appendices 28Apr2023.xlsx')
site_visit_input = path(plot_folder, '03_sitevisit_accschenega2022.csv')
ground_cover_input = path(plot_folder, '08_groundcover_accschenega2022.csv')
template_input = path(template_folder, "09_structural_group_cover.xlsx")
structural_group_input = path(project_folder, 'Data', 'Tables_Metadata', 'csv', 'structural_group.csv')

# Define output datasets
structural_cover_output = path(plot_folder, '09_structuralgroupcover_accschenega2022.csv')

# Read in data ----
structural_cover_original = read_xlsx(structural_cover_input, 
                                  sheet="group_structural_4")
site_visit_original = read_csv(site_visit_input)
ground_cover_original = read_csv(ground_cover_input)
template = colnames(read_xlsx(path=template_input))
structural_group_list = read_csv(structural_group_input)

# Obtain site visit code ----

# Format site code to match new convention
structural_cover = structural_cover_original %>%
  mutate(Site_code = str_replace_all(Site_code, "CHE_GRN_", "CHEN_"))

# Join with Site Visit table to obtain site visit code
structural_cover = site_visit_original %>% 
  select(site_code, site_visit_code) %>% 
  left_join(structural_cover, by = c("site_code" = "Site_code"))

# Ensure all sites have a site visit code
structural_cover %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

length(unique(structural_cover$site_visit_code))

# Add cover type ----
structural_cover = structural_cover %>% 
  mutate(structural_cover_type = "absolute foliar cover")

# Rename structural group elements ----
unique(structural_cover$`Structural Group`)

structural_cover = structural_cover %>%
  rename(structural_cover_percent = `Structural Percent Cover`) %>% 
  mutate(structural_group = str_replace_all(`Structural Group`, pattern = "_", replacement = " "),
         structural_group = case_when(structural_group == "fern and allies" ~ "spore-bearing plant",
                                      structural_group == "Spaghnum mosses" ~ "Sphagnum mosses",
                                      .default = structural_group)) %>% 
  select(all_of(template)) # Ensure only required columns are included in output

# Add structural group elements with 0% cover ----
for (i in 1:length(unique(structural_cover$site_visit_code))) {
  site_code = unique(structural_cover$site_visit_code)[i]
  subset_cover = structural_cover %>% filter(site_visit_code == site_code)
  
  # Determine which structural elements are not listed at that site
  missing_elements = structural_group_list %>% filter(!(structural_group %in% subset_cover$structural_group))
  
  # Append missing elements to existing ground cover data
  missing_elements = missing_elements %>%
    mutate(structural_cover_percent = 0,
           site_visit_code = site_code,
           structural_cover_type = "absolute foliar cover") %>% 
    select(all_of(template))
  
  if (i==1) {
    structural_cover_complete = bind_rows(subset_cover, missing_elements)
  } else {
    structural_cover_complete = bind_rows(structural_cover_complete, 
                                          subset_cover, missing_elements)
  }
}

# Arrange list of structural group classes alphabetically
structural_cover_complete = structural_cover_complete %>% 
  arrange(site_visit_code, structural_group, .locale="en") # Specify locale so that capitalized 'Sphagnum' doesn't get listed before everything else

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(structural_cover_complete, is.na)
    , sum)
)

# Do the group cover values match constrained values?
which(!(unique(structural_cover_complete$structural_group) %in% structural_group_list$structural_group))

# Does every site visit code have an entry for each structural group?
structural_cover_complete %>% 
  group_by(site_visit_code) %>% 
  count() %>% 
  ungroup() %>% 
  distinct(n) == nrow(structural_group_list)

# Does total % cover add up to 100 for each site?
# Total % cover: structural cover percent + ground cover percent - biotic percent (since biotic percent is sum of all structural group classes)
ground_cover = ground_cover_original %>%
  filter(ground_element != "biotic") %>% 
  group_by(site_visit_code) %>% 
  summarize(total_ground_cover = sum(ground_cover_percent))

total_cover = structural_cover %>%
  group_by(site_visit_code) %>% 
  summarise(total_structural_cover = sum(structural_cover_percent)) %>% 
  full_join(ground_cover, by="site_visit_code") %>% 
  group_by(site_visit_code) %>% 
  mutate(total_cover_sum = total_structural_cover + total_ground_cover) %>% 
  arrange(-total_cover_sum)

# Correct structural group estimates ----
# Calculate percent reduction in structural group estimates required to achieve 100% total cover.
# Only applies to sites where total % cover is > 105%. All other sites are within the margin of error and do not need a correction factor.
# Add 0.01 to correction factor to address rounding errors leading to 99% total cover
total_cover = total_cover %>% 
  mutate(correction_factor = case_when(total_cover_sum > 105 ~ (100 - total_ground_cover) / total_structural_cover + 0.01,
                                       .default = 1)) %>% 
  select(site_visit_code, correction_factor)

# Append with structural group df
# Calculate new cover percent
corrected_structural_cover = left_join(structural_cover_complete, total_cover, by = "site_visit_code") %>% 
  mutate(corrected_cover_percent = round(structural_cover_percent * correction_factor))

# Repeat total cover check
corrected_total_cover = corrected_structural_cover %>%
  group_by(site_visit_code) %>% 
  summarise(total_structural_cover = sum(corrected_cover_percent)) %>% 
  full_join(ground_cover, by="site_visit_code") %>% 
  group_by(site_visit_code) %>% 
  mutate(total_cover_sum = total_structural_cover + total_ground_cover) %>% 
  arrange(-total_cover_sum)

# Drop uncorrected columns
corrected_structural_cover = corrected_structural_cover %>% 
  select(-structural_cover_percent) %>% 
  rename(structural_cover_percent = corrected_cover_percent) %>% 
  select(all_of(template))

# Export as CSV ----
write_csv(corrected_structural_cover, structural_cover_output)

# Clear workspace ----
rm(list=ls())
