# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Ground Cover Table for ACCS Chenega Data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-07-02
# Usage: Must be executed in R version 4.4.0+.
# Description: "Format Ground Cover Table for ACCS Chenega Data" formats ground cover data collected and entered by ACCS for ingestion into the AKVEG Database. The script ensures values match constrained values, adds missing ground elements, and ensures that total percent cover sums to 100%. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
ground_cover_input = path(source_folder,'Chenega Appendices 28Apr2023.xlsx')
site_visit_input = path(plot_folder, '03_sitevisit_accschenega2022.csv')
template_input = path(template_folder, "08_ground_cover.xlsx")
ground_elements_input = path(project_folder, 'Data', 'Tables_Metadata', 'csv', 'ground_element.csv')

# Define output datasets
ground_cover_output = path(plot_folder, '08_groundcover_accschenega2022.csv')

# Read in data ----
ground_cover_original = read_xlsx(ground_cover_input, 
                                  sheet="App IV Ground Cover",
                                  range="A2:K20")
site_visit_original = read_csv(site_visit_input)
template = colnames(read_xlsx(path=template_input))
ground_elements_list = read_csv(ground_elements_input)

# Format ground elements list ----
# Remove 2 entries that refer to abiotic top cover table only
ground_elements_list = ground_elements_list %>% 
  filter(ground_element != "rock fragments" & ground_element != "soil")

# Obtain site visit code ----

# Format site code to match new convention
ground_cover = ground_cover_original %>%
  mutate(Plot = str_replace_all(Plot, "CHE_GRN_", "CHEN_"))

# Join with Site Visit table to obtain site visit code
ground_cover = site_visit_original %>% 
  select(site_code, site_visit_code) %>% 
  left_join(ground_cover, by = c("site_code" = "Plot"))

# Ensure all sites have a site visit code (n=18)
ground_cover %>% 
  filter(!is.na(site_visit_code)) %>% 
  nrow()

# Convert to long format ----
ground_cover = ground_cover %>% 
  pivot_longer(cols = "Standing Dead":"Boulder",
              names_to = "ground_element",
              values_to = "ground_cover_percent")

# Rename ground elements ----
ground_cover = ground_cover %>% 
  mutate(ground_element = str_to_lower(ground_element),
         ground_element = case_when(ground_element == "bedrock" ~ "bedrock (exposed)",
                                    ground_element == "dead and downed wood" ~ "dead down wood (≥ 2 mm)",
                                    ground_element == "litter" ~ "litter (< 2 mm)",
                                    ground_element == "standing dead" ~ "dead standing woody vegetation",
                                    .default = ground_element))

# Add ground elements with 0% that are missing from dataset 
extra_elements_list = ground_elements_list[which(!(ground_elements_list$ground_element %in% unique(ground_cover$ground_element))),]$ground_element

extra_elements = data.frame("site_code" = rep(unique(ground_cover$site_code)),
                            "site_visit_code" = rep(unique(ground_cover$site_visit_code)),
                            "ground_element" = rep(extra_elements_list, 
                                                   times=1,
                                                   each=18),
                            "ground_cover_percent" = 0)

# Check to see that results are what would you expect
table(extra_elements$site_code) # 3 entries for each site (number of missing elements)
table(extra_elements$ground_element) # 18 entries for each ground element (number of sites)

# Add extra elements to primary dataframe
ground_cover = ground_cover %>% 
  bind_rows(extra_elements)

# Ensure every element is included at each site (even if cover percent is 0%)
table(ground_cover$ground_element)
length(unique(ground_cover$ground_element)) == nrow(ground_elements_list)

# Restrict to required columns ----
ground_cover = ground_cover %>% 
  select(all_of(template))

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(ground_cover, is.na)
    , sum)
)

# Does the total % ground cover add up to 100 for each site?
ground_cover %>% 
  group_by(site_visit_code) %>% 
  summarize(total_cover = round(sum(ground_cover_percent),digits=1)) %>% 
  filter(total_cover != 100)

# Does every site have an entry for each ground element?
ground_cover %>% 
  group_by(site_visit_code) %>% 
  summarize(rows = n()) %>% 
  filter(rows!=nrow(ground_elements_list))

# Export as CSV ----
write_csv(ground_cover, ground_cover_output)

# Clear workspace ----
rm(list=ls())