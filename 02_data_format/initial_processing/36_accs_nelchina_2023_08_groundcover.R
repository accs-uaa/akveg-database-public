# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Ground Cover for ACCS Nelchina 2023 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2023-10-27
# Usage: Must be executed in R version 4.3.1+.
# Description: "Calculate Ground Cover for ACCS Nelchina 2023 data" uses data from line-point intercept surveys to calculate plot-level percent ground cover for each ground element. The script also appends unique site visit identifiers, performs QA/QC checks to ensure values are within a reasonable range, and enforces formatting to match the AKVEG template.
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
input_visual = file.path(data_folder, "Summer 2023", "08_accs_nelchina_ground_2023.xlsx")
input_elements = file.path(data_folder,"abiotic_ground_elements.xlsx")
input_template = file.path(template_folder, "08_Ground_Cover.xlsx")
input_site_visit = file.path(output_folder, "03_accs_nelchina_2023.csv")

# Define outputs ----
output_ground = file.path(output_folder, "08_accs_nelchina_2023.csv")

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
  filter(ground_element != FALSE)

ground_elements = unique(element_codes$ground_element)

# Format visual data ----
visual_data = visual_data %>% 
  mutate(site_code = str_replace_all(site_code, 
                                     pattern ="NLC", replacement = "NLC2023"),
         ground_element = case_when(ground_element == "moss" ~ "biotic",
                                    .default = ground_element)) %>% # Ensure categories reflect accepted AKVEG terms
  left_join(site_visit,by="site_code") %>% # Append site visit code
  filter(ground_element %in% ground_elements & ground_cover_percent!=0) %>% 
  select(-site_code)

# Format LPI cover data ----
lpi_data = lpi_data %>%
  mutate(site_code = str_replace_all(site_code, 
                                     pattern ="NLC", replacement = "NLC2023")) %>% 
  left_join(site_visit,by="site_code") %>% # Append site visit code
  pivot_longer(cols = layer_1:layer_8, # Convert to long format
               names_to = "strata",
               names_prefix = "layer_",
               values_to = "code",
               values_drop_na = TRUE) %>% 
  mutate(code = str_to_lower(code))

# Restrict to last stratum ----
# For each point on a line, only consider the last strata (i.e., ground-level)
ground_data <- lpi_data %>% 
  group_by(site_visit_code, line, point) %>% 
  mutate(last_strata = max(strata)) %>% 
  filter(strata == last_strata) %>% 
  ungroup()

# QA/QC ----
# Are there any ground hits that are neither basal hits nor listed as a ground_element?
# Basal hits are the only time a vascular plant gets counted as ‘biotic’ ground cover.
ground_data %>% 
  mutate(basal_hit = if_else(grepl(pattern="-b",x=ground_data$code),
                             "TRUE",
                             "FALSE")) %>% 
  filter( !((code %in% element_codes$code) | basal_hit == TRUE))

# Calculate cover percent ----
# Each ground_element can appear a maximum of 120 times per plot
# Classify all basal hits as "biotic" ground elements
ground_data <- ground_data %>% 
  left_join(element_codes, by="code") %>% 
  mutate(ground_element = if_else(is.na(ground_element), 
                                  "biotic", ground_element)) %>% 
  group_by(site_visit_code, ground_element) %>% 
  mutate(hits = 1) %>% 
  summarize(total_hits = sum(hits)) %>% 
  mutate(ground_cover_percent = round(total_hits/120*100,digits=3)) %>% 
  ungroup()  %>%
  select(all_of(template))

# Append data from visual estimate site
ground_data = bind_rows(ground_data, visual_data)

# Add ground elements with 0% cover ----
sites = unique(site_visit$site_visit_code)

for (i in 1:length(sites)) {
  unique_visit = sites[i]
  
  if (unique_visit %in% ground_data$site_visit_code){
    ground_subset = ground_data %>% 
      filter(site_visit_code == unique_visit)
    
    missing_elements = subset(ground_elements, !(ground_elements %in% ground_subset$ground_element))
    
    missing_df = data.frame(site_visit_code = unique_visit, 
                            ground_element = missing_elements,
                            ground_cover_percent = 0)
  } else {
    missing_df = data.frame(site_visit_code = unique_visit, 
                            ground_element = ground_elements,
                            ground_cover_percent = 0)
  }
  ground_data = bind_rows(ground_data,missing_df)
}

# Arrange by site_visit_code & ground_element
ground_data = ground_data %>% 
  arrange(site_visit_code,ground_element)

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(ground_data, is.na)
    , sum)
)

# Are the list of sites the same?
ground_data$site_visit_code[!(ground_data$site_visit_code %in% site_visit$site_visit_code)] # Missing from site visit data
site_visit$site_visit_code[!(site_visit$site_visit_code %in% ground_data$site_visit_code)] # Missing from ground data

# Does the total % ground cover add up to 100 for each site?
ground_data %>% 
  group_by(site_visit_code) %>% 
  summarize(total_cover = round(sum(ground_cover_percent),digits=1)) %>% 
  filter(total_cover != 100)

# Does every site have an entry for each ground element?
ground_data %>% 
  group_by(site_visit_code) %>% 
  summarize(rows = n()) %>% 
  filter(rows!=length(ground_elements))

# Export data ----
write_csv(ground_data,file=output_ground)

# Clear workspace ----
rm(list=ls())