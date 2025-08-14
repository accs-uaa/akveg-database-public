# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Ground Cover Table for USFWS Pribilof Islands 2022 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-23
# Usage: Must be executed in R version 4.4.1+.
# Description: "Format Ground Cover Table for USFWS Pribilof Islands 2022 data" formats ground cover data for ingestion into the AKVEG Database. The script re-classifies ground elements to ensure values match constrained values, adds missing ground elements, and ensures values are within reasonable range. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
ground_cover_input = path(source_folder,'pribilofs2022.xlsx')
site_visit_input = path(plot_folder, '03_site_visit_fwspribilof2022.csv')
template_input = path(template_folder, "08_ground_cover.xlsx")
ground_elements_input = path(project_folder, 'Data', 'Tables_Metadata', 'csv', 'ground_element.csv')

# Define output datasets
ground_cover_output = path(plot_folder, '08_groundcover_fwspribilof2022.csv')

# Read in data ----
ground_cover_original = read_xlsx(ground_cover_input)
site_visit_original = read_csv(site_visit_input)
template = colnames(read_xlsx(path=template_input))
ground_elements_list = read_csv(ground_elements_input)

# Format ground elements list ----
# Remove entries that refer to ground cover table only
elements_to_exclude = c("rock fragments", "soil")
ground_elements_list = ground_elements_list %>% 
  filter(!(ground_element %in% elements_to_exclude))

# Append site visit code ----
site_visit_data = site_visit_original %>% 
  select(site_code, site_visit_code)

ground_cover = ground_cover_original %>% 
  mutate(site_code = str_replace_all(`Releve number`, "-", "_")) %>% # Convert dashes to underscore
  right_join(site_visit_data, by = "site_code") # Use right join to drop 6 anthropogenic sites

# Ensure all entries have a site visit code
ground_cover %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Explore data ----

# Ensure all sites (n=95) are present in the dataset
length(unique(ground_cover$site_visit_code))

# Select only relevant columns
ground_cover = ground_cover %>%
  select(site_visit_code, `Cover litter layer (%)`:`Cov_scree < 20 cm`) %>% 
  select(-Cov_wtuss) # Not included as a ground element

# Convert to long format ----
ground_cover_long = ground_cover %>%
  mutate(`Cover litter layer (%)` = as.numeric(`Cover litter layer (%)`),
         `Cover open water (%)` = as.numeric(`Cover open water (%)`)) %>% 
  pivot_longer(`Cover litter layer (%)`:`Cov_scree < 20 cm`, 
               names_to="ground_element", values_to="ground_cover_percent")

# Rename ground elements ----
# Ground elements for rock fragment categories do not exactly match with the size classes in this dataset. Notably, the 'scree' class (<200 mm) encompasses both the 'gravel' (2mm - 76mm) and 'cobble' (76-250mm) classes in AKVEG. I assumed most of the rocks in the 'scree' category were 'cobble'-sized.
unique(ground_cover_long$ground_element)

ground_cover_long = ground_cover_long %>%
  mutate(ground_element = case_when(ground_element == "Cover litter layer (%)" ~ "litter (< 2 mm)",
                                     ground_element == "Cover open water (%)" ~ "water",
                                     ground_element == "Cov_dwarf shrub" | ground_element == "Cov_grass" | ground_element == "Cov_forb" | ground_element == "Cov_sedge" | ground_element == "Cov_sphagnum" | ground_element == "Cov_feather moss" | ground_element == "Cov_other moss" | ground_element == "Cov_light lichens" | ground_element == "Cov_other lichens" | ground_element == "Cov_biocrust" ~ "biotic",
                                    ground_element == "Cov_organic soil" ~ "organic soil",
                                    ground_element == "Cov_mineral soil" | ground_element == "Cov_sand" ~ "mineral soil",
                                    ground_element == "Cov_dead down wood (drift wood)" ~ "dead down wood (≥ 2 mm)",
                                    ground_element == "Cov_bed drock" ~ "bedrock (exposed)",
                                    ground_element == "Cov_boulder > 100 cm" ~ "boulder",
                                    ground_element == "Cov_talus 20 - 100 cm" ~ "stone",
                                    ground_element == "Cov_scree < 20 cm" ~ "cobble",
                                    .default = ground_element))

# Ensure all ground elements correspond with an element in the 'ground elements list'
which(!(unique(ground_cover_long$ground_element) %in% ground_elements_list$ground_element))

# Add ground elements with 0% cover ----
for (i in 1:length(unique(ground_cover_long$site_visit_code))) {
  site_code = unique(ground_cover_long$site_visit_code)[i]
  subset_ground_cover = ground_cover_long %>% filter(site_visit_code == site_code)
  
  # Determine which ground elements are not listed at that site
  missing_elements = ground_elements_list %>% filter(!(ground_element %in% subset_ground_cover$ground_element))
  
  # Append missing elements to existing ground cover data
  missing_elements = missing_elements %>%
    mutate(ground_cover_percent = 0,
           site_visit_code = site_code) %>% 
    select(site_visit_code, ground_element, ground_cover_percent)
  
  if (i==1) {
    ground_cover_complete = bind_rows(subset_ground_cover, missing_elements)
  } else {
    ground_cover_complete = bind_rows(ground_cover_complete, 
                                       subset_ground_cover, missing_elements)
  }
}

rm(i, subset_ground_cover, site_code, missing_elements, ground_cover_long, ground_cover)

# Summarize ground cover ----
# Group all biotic entries and all mineral soil entries together for each site visit code
ground_cover_final = ground_cover_complete %>% 
  group_by(site_visit_code, ground_element) %>% 
  mutate(ground_cover_percent = sum(ground_cover_percent)) %>% 
  distinct(site_visit_code, ground_element, ground_cover_percent) %>% 
  arrange(site_visit_code, ground_element) %>% 
  select(all_of(template))

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(ground_cover_final, is.na)
    , sum)
)

# Does every site have an entry for each ground element?
ground_cover_final %>% 
  group_by(site_visit_code) %>% 
  summarize(rows = n()) %>% 
  filter(rows!=nrow(ground_elements_list))

# Does every ground element have an entry for each site?
ground_cover_final %>% 
  group_by(ground_element) %>% 
  summarize(rows = n()) %>% 
  filter(rows!=length(unique(site_visit_data$site_code)))

# Does range of values make sense? Ground cover does not have an upper limit, but minimum values should be zero
summary(ground_cover_final$ground_cover_percent)
hist(ground_cover_final$ground_cover_percent)

# Export as CSV ----
write_csv(ground_cover_final, ground_cover_output)

# Clear workspace ----
rm(list=ls())