# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Soil Metrics for ACCS Nelchina 2023 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2023-10-24
# Usage: Must be executed in R version 4.3.1+.
# Description: "Format Soil Metrics for ACCS Nelchina 2023 data" appends unique site visit identifier, ensures formatting matches the AKVEG template, and performs QA/QC checks to ensure values match constrained fields and are within a reasonable range.
# ---------------------------------------------------------------------------

rm(list=ls())

# Load packages ----
library(dplyr)
library(ggplot2)
library(readr)
library(readxl)
library(stringr)
library(tidyr)

# Define directories ----
drive <- "D:"
project_folder <- file.path(drive,"ACCS_Work/Projects")
db_folder <- file.path(project_folder,"AKVEG_Database", "Data")
template_folder <- file.path(db_folder, "Data_Entry")
data_folder <- file.path(project_folder, "Caribou_Nelchina","Data", "Summer 2023")
output_folder <- file.path(db_folder,"Data_Plots","36_accs_nelchina_2023")

# Define inputs ----
input_soils <- file.path(data_folder, "13_accs_nelchina_soils_2023.xlsx")
input_template <- file.path(template_folder, "13_soil_metrics.xlsx")
input_site_visit <- file.path(output_folder, "03_accs_nelchina_2023.csv")

# Define outputs ----
output_soils <- file.path(output_folder, "13_accs_nelchina_2023.csv")

# Read in data ----
soils_data <- read_xlsx(path=input_soils)
template <- colnames(read_xlsx(path=input_template))
site_visit <- read_csv(input_site_visit)

# Format site visit data ----
site_visit <- site_visit %>% 
  select(site_code, site_visit_code)

# Format environment data ----
soils_data <- soils_data %>%
  select(-site_visit_code) %>% 
  mutate(site_code = str_replace_all(site_code, 
                                     pattern ="NLC", replacement = "NLC2023")) %>% 
  left_join(site_visit,by="site_code") %>% # Append site visit code
  select(all_of(template)) # Keep only required columns

# QA/QC ----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(soils_data, is.na)
    , sum)
)

# Are the list of sites the same?
soils_data <- soils_data %>% arrange(site_visit_code)
site_visit <- site_visit %>% arrange(site_visit_code)

which(soils_data$site_visit_code != site_visit$site_visit_code)

# Verify values for boolean column
table(soils_data$water_measurement)

# Verify values for numerical data
# Measurement depth - most values should be at 10 cm
soils_data %>%
  ggplot(aes(x=measure_depth_cm)) + 
  geom_histogram(color="black", fill="white", binwidth = 2) +
  theme_classic()

# pH
soils_data %>% filter(ph == -999) %>% nrow() # One null value

soils_data %>% filter(ph != -999) %>% 
  ggplot(aes(x=ph)) + 
  geom_histogram(color="black", fill="#4477AA", binwidth = 0.5) +
  xlim(4,8) +
  theme_classic()

# Conductivity
soils_data %>% filter(conductivity_mus == -999) %>% nrow() # No null values

soils_data %>%
  ggplot(aes(x=conductivity_mus)) + 
  geom_histogram(color="black", fill="#228833", binwidth = 15) +
  theme_classic()
# Cross-referencing with environment data: Three highest conductivity values are in floodplains; lowest valleys are on alpine mountains with shallow soils.

# Temperature
soils_data %>% filter(temperature_deg_c == -999) %>% nrow() # One null value

soils_data %>% filter(temperature_deg_c != -999) %>%
  ggplot(aes(x=temperature_deg_c)) + 
  geom_histogram(color="black", fill="#CCBB44", binwidth = 2) +
  theme_classic()

# Export data ----
write_csv(soils_data,file=output_soils)

# Clean workspace ----
rm(list=ls())