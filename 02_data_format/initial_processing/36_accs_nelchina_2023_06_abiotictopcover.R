# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Abiotic Top Cover for ACCS Nelchina 2023 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2023-10-27
# Usage: Must be executed in R version 4.3.1+.
# Description: "Calculate Abiotic Top Cover for ACCS Nelchina 2023 data" uses data from line-point intercept surveys to calculate plot-level percent abiotic top cover for each abiotic element. The script also appends unique site visit identifiers, codes all non-existing entries as 0%, performs QA/QC checks to ensure values are within a reasonable range, and enforces formatting to match the AKVEG template. Note: If the first hit on a line is an abiotic_element, cover_percent is calculated and an entry is recorded. If the first hit is not an abiotic_element, there is no abiotic top cover for that line.
# ---------------------------------------------------------------------------

rm(list=ls())

# Load packages ----
library(dplyr)
library(readr)
library(readxl)
library(stringr)
library(tidyr)

# Define directories ----
drive = "D:"
project_folder = file.path(drive,"ACCS_Work/Projects")
db_folder = file.path(project_folder,"AKVEG_Database", "Data")
template_folder = file.path(db_folder, "Data_Entry")
data_folder = file.path(project_folder, "Caribou_Nelchina","Data")
output_folder = file.path(db_folder,"Data_Plots","36_accs_nelchina_2023")

# Define inputs ----
input_lpi = file.path(data_folder, "Summer 2023", "05_accs_nelchina_lpi_2023.xlsx")
input_visual = file.path(data_folder, "Summer 2023", "06_accs_nelchina_abiotic_2023.xlsx")
input_elements = file.path(data_folder,"abiotic_ground_elements.xlsx")
input_template = file.path(template_folder, "06_Abiotic_Top_Cover.xlsx")
input_site_visit = file.path(output_folder, "03_accs_nelchina_2023.csv")

# Define outputs ----
output_abiotic = file.path(output_folder, "06_accs_nelchina_2023.csv")

# Read in data ----
lpi_data = read_xlsx(path=input_lpi, col_types=c("text","numeric","numeric",
                                                  "text","text","text",
                                                  "text","text","text",
                                                  "text","text"))
visual_data = read_xlsx(path=input_visual)
element_codes = read_xlsx (path=input_elements)
template = colnames(read_xlsx(path=input_template))
site_visit = read_csv(input_site_visit, col_select=c(site_code, site_visit_code))

# Format elements code list ----
element_codes <- element_codes %>% 
  mutate(code = str_to_lower(code)) %>% 
  filter(abiotic_element != FALSE)

abiotic_elements = unique(element_codes$abiotic_element)

# Format visual cover data ----
visual_data = visual_data %>%
  add_row(site_code = "NLC_185", 
          abiotic_element = "soil",
          abiotic_top_cover_percent = 4) %>%
  mutate(site_code = str_replace_all(site_code, 
                                     pattern ="NLC", replacement = "NLC2023")) %>%
  left_join(site_visit,by="site_code") %>%
  filter(abiotic_element %in% abiotic_elements & abiotic_top_cover_percent!=0) %>% 
  select(-site_code)

# Format LPI cover data ----
abiotic_data = lpi_data %>%
  mutate(site_code = str_replace_all(site_code, 
                                     pattern ="NLC", replacement = "NLC2023")) %>% 
  left_join(site_visit,by="site_code") %>% # Append site visit code
  pivot_longer(cols = layer_1:layer_8, # Convert to long format
               names_to = "strata",
               names_prefix = "layer_",
               values_to = "code",
               values_drop_na = TRUE) %>% 
  mutate(code = str_to_lower(code)) %>% # Convert to lowercase
  filter((code %in% element_codes$code)) %>% # Include only abiotic codes
  left_join(element_codes, by="code")

# Calculate top cover percent ----
# Only include the first (topmost) strata
# Each abiotic_element can appear a maximum of 120 times per plot
abiotic_data = abiotic_data %>% 
  filter(strata == 1) %>% 
  mutate(hits = 1) %>% 
  group_by(site_visit_code, abiotic_element) %>% 
  summarize(total_hits = sum(hits)) %>% 
  mutate(abiotic_top_cover_percent = round(total_hits/120*100, digits=3)) %>% 
  ungroup %>% 
  select(all_of(template))

# Append data from visual estimate site
abiotic_data = bind_rows(abiotic_data, visual_data)

# Add abiotic elements with 0% cover ----
sites = unique(site_visit$site_visit_code) # Do not use abiotic_data df in case one site has no abiotic top cover hits

for (i in 1:length(sites)) {
  unique_visit = sites[i]
  
  if (unique_visit %in% abiotic_data$site_visit_code){
    abiotic_subset = abiotic_data %>% 
      filter(site_visit_code == unique_visit)
    
    missing_elements = subset(abiotic_elements, !(abiotic_elements %in% abiotic_subset$abiotic_element))
    
    missing_df = data.frame(site_visit_code = unique_visit, 
                            abiotic_element = missing_elements,
                            abiotic_top_cover_percent = 0)
  } else {
    missing_df = data.frame(site_visit_code = unique_visit, 
                            abiotic_element = abiotic_elements,
                            abiotic_top_cover_percent = 0)
  }
  abiotic_data = bind_rows(abiotic_data,missing_df)
}

# Arrange by site_visit_code & ground_element
abiotic_data = abiotic_data %>% 
  arrange(site_visit_code,abiotic_element)


# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(abiotic_data, is.na)
    , sum)
)

# Are the list of sites the same?
abiotic_data$site_visit_code[!(abiotic_data$site_visit_code %in% site_visit$site_visit_code)] # Missing from site visit data
site_visit$site_visit_code[!(site_visit$site_visit_code %in% abiotic_data$site_visit_code)] # Missing from abiotic data

# Does every site have an entry for each abiotic element?
abiotic_data %>% 
  group_by(site_visit_code) %>% 
  summarize(rows = n()) %>% 
  filter(rows!=6)

# Does the sum of the abiotic + biotic elements equal 100%?
biotic_data = lpi_data %>%
  mutate(site_code = str_replace_all(site_code, 
                                     pattern ="NLC", replacement = "NLC2023")) %>% 
  left_join(site_visit,by="site_code") %>% # Append site visit code
  pivot_longer(cols = layer_1:layer_8, # Convert to long format
               names_to = "strata",
               names_prefix = "layer_",
               values_to = "code",
               values_drop_na = TRUE) %>% 
  mutate(code = str_to_lower(code)) %>% # Convert to lowercase
  filter(!(code %in% element_codes$code)) %>% # Include only biotic codes
  filter(strata == 1) %>% 
  mutate(hits = 1) %>% 
  group_by(site_visit_code,code) %>% 
  summarize(total_hits = sum(hits)) %>% 
  mutate(abiotic_top_cover_percent = round(total_hits/120*100,digits=3)) %>% 
  rename(abiotic_element = code) %>% 
  ungroup %>% 
  select(all_of(template))

# Combine abiotic and biotic top cover
total_cover <- bind_rows(abiotic_data,biotic_data)

total_cover %>% 
  group_by(site_visit_code) %>% 
  summarize(total_top_cover = round(sum(abiotic_top_cover_percent),digits=1)) %>% 
  filter(total_top_cover != 100) # Returns the single visual estimate site, which is not included in the LPI data

# Export data ----
write_csv(abiotic_data,file=output_abiotic)

# Clear workspace ----
rm(list=ls())