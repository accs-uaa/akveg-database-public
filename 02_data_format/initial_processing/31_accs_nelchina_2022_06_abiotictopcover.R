# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Abiotic Top Cover for ACCS Nelchina data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-03-27
# Usage: Must be executed in R version 4.4.3+.
# Description: "Format Abiotic Top Cover for ACCS Nelchina data" calculates abiotic top cover from line-point intercept data, appends unique site visit codes, and renames columns to match the AKVEG template. The script also ensures values match constrained values, adds missing abiotic elements, and ensures values are within reasonable range. The output is a CSV table that can be converted and included in a SQL INSERT statement. 
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(tidyr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = path(project_folder, 'Data', 'Data_Plots', '31_accs_nelchina_2022')
template_folder = path(project_folder, "Data", "Data_Entry")
source_folder = path(plot_folder, 'source')

# Define datasets ----

# Define input datasets
lpi_cover_input = path(plot_folder,"source","05_accs_nelchina_lpi_surveys.xlsx")
extra_site_input = file.path(plot_folder, "source","06_accs_nelchina.xlsx")
site_visit_input = path(plot_folder, "03_sitevisit_accsnelchina2022.csv")
abiotic_list_input = path(project_folder, 'Data', 'Tables_Metadata', 'csv', 'ground_element.csv')
template_input = path(template_folder, "06_abiotic_top_cover.xlsx")

# Define outputs
abiotic_cover_output = path(plot_folder, "06_abiotictopcover_accsnelchina2022.csv")

# Read in data ----
lpi_cover_original = read_xlsx(path=lpi_cover_input, col_types=c("text","numeric","numeric",
                                                      "text","text","text",
                                                      "text","text","text",
                                                      "text","text"))
site_visit_original = read_csv(site_visit_input, col_select=c('site_visit_id', 'site_code'))
extra_site_original = read_xlsx(extra_site_input)
abiotic_list_original = read_csv(abiotic_list_input)
template = colnames(read_xlsx(path=template_input))

# Format abiotic elements list ----
# Remove entries that refer to ground cover table only
abiotic_list = abiotic_list_original %>% 
  filter(ground_element != 'biotic') %>% 
  mutate(abiotic_element = case_when(ground_element %in% c("boulder","cobble","gravel","stone") ~ 'rock fragments',
                                     ground_element %in% c("organic soil","mineral soil") ~ 'soil',
                                     .default = ground_element))

# Append site_visit_id ----
cover_data_wide = left_join(lpi_cover_original, site_visit_original, by="site_code")
extra_site = left_join(extra_site_original,site_visit_original,by="site_code")

# Ensure all sites have an associated site visit id
cover_data_wide %>% filter(is.na(site_visit_id))
extra_site %>% filter(is.na(site_visit_id))
cover_data_wide %>% distinct(site_visit_id) %>% nrow() + 1 == nrow(site_visit_original)

# Convert cover data to long format ----
cover_data_long = pivot_longer(cover_data_wide, 
                         cols = layer_1:layer_8,
                         names_to = "strata",
                         names_prefix = "layer_",
                         values_to = "code",
                         values_drop_na = TRUE)

# Correct abiotic codes ----
# To match formatting used in data dictionary
cover_data_long = cover_data_long %>% 
  mutate(ground_element_code = case_when(code == 'O' ~ 'OS',
                                         code == 'HL' ~ 'L',
                                         code == 'S' ~ 'MS',
                                         code == 'W' ~ 'WA',
                                         .default = code))

# Restrict cover data to abiotic elements ----
abiotic_cover = cover_data_long %>% 
  filter(ground_element_code %in% abiotic_list$ground_element_code) %>% 
  left_join(abiotic_list, by="ground_element_code")

# Create biotic_data for QA/QC purposes ----
# Abiotic + biotic top cover must sum to 100%
biotic_cover = cover_data_long %>% 
  filter(!(ground_element_code %in% abiotic_list$ground_element_code))

# Ensure all cover data are included in abiotic + biotic cover data
nrow(abiotic_cover) + nrow(biotic_cover) == nrow(cover_data_long)

# Format extra site ----
# Site did not have line-point intercept data
extra_site = extra_site %>% 
  rename(site_visit_code = site_visit_id) %>% 
  select(all_of(template))

# Calculate abiotic top cover percent ----
abiotic_top_cover = abiotic_cover %>%
  filter(strata == 1) %>% # Only hits on first stratum counts toward 'top' cover
  mutate(hits = 1) %>% # Add counter
  group_by(site_visit_id, abiotic_element) %>% 
  summarize(total_hits = sum(hits)) %>% # Total number of top cover hits on a line
  mutate(abiotic_top_cover_percent = total_hits/120*100) %>% # Maximum number of points on a line is 120
  rename(site_visit_code = site_visit_id) %>% 
  mutate(abiotic_top_cover_percent = round(abiotic_top_cover_percent, digits = 3)) %>% # Round percent cover to 3 decimal points
  select(all_of(template)) %>% 
  bind_rows(extra_site) # Add extra site

# Add abiotic elements with 0% cover ----
# Use original site visit dataframe. Some sites may be missing from abiotic cover df if there was no abiotic top cover 
for (i in 1:length(unique(site_visit_original$site_visit_id))) {
  site_code = unique(site_visit_original$site_visit_id)[i]
  top_cover = abiotic_top_cover %>% filter(site_visit_code == site_code)
  
  # Determine which abiotic elements are not listed at that site
  missing_elements = abiotic_list %>% 
    filter(!(abiotic_element %in% top_cover$abiotic_element)) %>% 
    select(abiotic_element) %>% 
    distinct()

  # Append missing elements to existing abiotic top cover data
  missing_elements = missing_elements %>%
    mutate(abiotic_top_cover_percent = 0,
           site_visit_code = site_code) %>% 
    select(all_of(template)) %>% 
    bind_rows(top_cover) %>% 
    arrange(abiotic_element)
  
  # Populate dataframe
  if (i==1) {
    abiotic_cover_final = missing_elements
  } else {
    abiotic_cover_final = bind_rows(abiotic_cover_final, 
                                       missing_elements)
  }
}

rm(i, top_cover, site_code, missing_elements)

# Verify results
table(abiotic_cover_final$site_visit_code) # 8 entries for each site (total number of abiotic elements)
table(abiotic_cover_final$abiotic_element) # 22 entries for each abiotic element (total number of sites)

# Calculate biotic top cover percent ----
# For QA/QC
biotic_top_cover = biotic_cover %>% 
  filter(strata == 1) %>% 
  mutate(hits = 1) %>% 
  group_by(site_visit_id,code) %>% 
  summarize(total_hits = sum(hits)) %>% 
  mutate(abiotic_top_cover_percent = total_hits/120*100) %>% 
  rename(abiotic_element = code) %>% 
  ungroup() %>% 
  rename(site_visit_code = site_visit_id) %>% 
  select(all_of(template))

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(abiotic_cover_final, is.na)
    , sum)
)

# Are the correct number of sites included?
abiotic_cover_final %>% 
  distinct(site_visit_code) %>% 
  nrow() == nrow(site_visit_original)

# Are values for abiotic top cover reasonable?
summary(abiotic_cover_final$abiotic_top_cover_percent)

# Calculate total cover (abiotic + biotic percent)
total_cover = rbind(abiotic_cover_final, biotic_top_cover)

# Does the combined top cover equal to 100%?
total_cover %>% 
  group_by(site_visit_code) %>% 
  summarize(total_top_cover = sum(abiotic_top_cover_percent)) %>% 
  mutate(total_top_cover = round(total_top_cover, digits = 0)) %>% 
  filter(total_top_cover != 100)

# Export data ----
write_csv(abiotic_cover_final, file=abiotic_cover_output)

# Clear workspace ----
rm(list=ls())
