# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Vegetation Cover for BLM AIM Various 2022 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-08-06
# Usage: Must be executed in R version 4.4.1+.
# Description: "Calculate Vegetation Cover for BLM AIM Various 2022 data" uses data from visual estimate surveys to calculate site-level percent foliar cover for each recorded species. It also appends unique site visit identifiers, corrects taxonomic names using the AKVEG database as a taxonomic standard, populates required metadata fields, and performs QA/QC checks. The script depends on the output from the 44_aim_various_2022_05_extract_veg_data.py script in the /datum_conversion subfolder. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(RPostgres)
library(stringr)
library(tidyr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database')
data_folder = path(project_folder, "Data")
plot_folder = path(data_folder, 'Data_Plots', '44_aim_various_2022')

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_public')

# Define datasets ----

# Define input datasets
veg_cover_input = 'C:/ACCS_Work/OneDrive - University of Alaska/ACCS_Teams/Vegetation/AKVEG_Database/Data/Data_Plots/44_aim_various_2022/working/veg_cover_data_export.csv'
site_visit_input = path(plot_folder, paste0("03_site_visit_", 'aimvarious2022', ".csv"))
template_input = path(data_folder, "Data_Entry", "05_vegetation_cover.xlsx")
plant_codes_input = path(data_folder,"Tables_Taxonomy","USDA_Plants","USDA_Plants_20240301.csv")

# Define output dataset
veg_cover_output = path(plot_folder, paste0("05_vegetation_cover_", 'aimvarious2022', ".csv"))

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public.csv')

# Read in data ----
veg_cover_original = read_csv(veg_cover_input)
site_visit_original = read_csv(site_visit_input)
template = colnames(read_xlsx(path=template_input))
usda_codes_original = read_csv(plant_codes_input, col_select = c("Symbol","Synonym Symbol", "Scientific Name with Author"))

# Query AKVEG database ----

# Import database connection function
connection_script = path(repository_folder,
                         'package_DataProcessing',
                         'connect_database_postgresql.R')
source(connection_script)

# Connect to the AKVEG PostgreSQL database
akveg_connection = connect_database_postgresql(authentication)

# Define query
query_taxa = "SELECT taxon_all.taxon_name as taxon_name
, taxon_accepted_name.taxon_name as taxon_name_accepted
FROM taxon_all
  LEFT JOIN taxon_accepted ON taxon_all.taxon_accepted_code = taxon_accepted.taxon_accepted_code
  LEFT JOIN taxon_all taxon_accepted_name ON taxon_all.taxon_accepted_code = taxon_accepted_name.taxon_code
ORDER BY taxon_name_accepted, taxon_name;"

# Read SQL table as dataframe
taxa_all = as_tibble(dbGetQuery(akveg_connection, query_taxa))

# Format USDA taxonomy ----
usda_codes = usda_codes_original %>% 
  mutate(symbol = case_when(!is.na(`Synonym Symbol`) ~ NA,
                            .default = Symbol)) %>% 
  select(-Symbol) %>% 
  pivot_longer(cols = c(symbol, `Synonym Symbol`),
               names_to = "code_status",
               values_to = "usda_code",
               values_drop_na = TRUE)

# Format site codes ----
# Remove 'AK' prefix
# Remove "CYFO' and 'EIFO' prefixes (project_code already includes this info)
# Remove date string appended at the end
veg_cover_data = veg_cover_original %>% 
  mutate(site_code = str_remove_all(EvaluationID, c("AK-|CYFO-|EIFO-")),
         site_code = str_remove(site_code, "_\\d{4}-\\d{2}-\\d{2}"),
         site_code = str_replace_all(site_code, "-", "_"))

# Append site visit code ----
site_visit_data = site_visit_original %>% 
  select(site_code, site_visit_code)

veg_cover_data = veg_cover_data %>%
  right_join(site_visit_data, by = "site_code") # Use right join to drop any excluded plots

# Explore cover data ----

# Ensure all entries have a site visit code
veg_cover_data %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Ensure that all sites in Site Visit table are associated with an entry in the veg cover data
which(!site_visit_data$site_visit_code %in% unique(veg_cover_data$site_visit_code))

# Ensure that all lines are the standard 25m length
unique(veg_cover_data$LineLength)

# Create a unique point number that is a combination of point number + line number
veg_cover_data = veg_cover_data %>% 
  group_by(site_visit_code) %>% 
  arrange(LineNumber, PointNbr) %>% 
  mutate(point_number = row_number())

# Format dead status ---- 
# Thank you to user Ronak-Shah for the rename_with solution: https://stackoverflow.com/questions/57609974/rename-all-variables-that-contai-a-particular-string-and-add-a-sequencial-number

# Rename 'dead_status' columns
veg_cover_data = veg_cover_data %>% 
  rename_with(~paste0("dead", sub("ChkboxLower_*", "", .)), 
              +                    starts_with('ChkboxLower')) %>% 
  rename(dead0 = ChkboxTop,
         dead8 = ChkboxBasal)

# Convert L (live) and D (dead) to Boolean, where FALSE = live vegetation and TRUE = dead
veg_cover_data = veg_cover_data %>% 
  mutate(across(starts_with("dead"), ~if_else(.x == "L", "FALSE", "TRUE")))

# Mini check to make sure that worked
table(veg_cover_original$ChkboxBasal)
table(veg_cover_data$dead8)

# Format canopy layer columns ----
veg_cover_data = veg_cover_data %>% 
  rename_with(~paste0("species", sub("Lower_*", "", .)), 
              +                    starts_with('Lower')) %>% 
  rename(species0 = TopCanopy,
         species8 = codebasal) # Use this column instead of SoilSurface, as SoilSurface includes abiotic elements as well

# Select relevant columns ----
cols_to_keep = c("site_visit_code","point_number",
                 str_c("species", seq(0,8,1)), # strata/canopy layer columns
                 str_c("dead", seq(0,8,1))) # dead status columns

veg_cover_simple = veg_cover_data %>% 
  select(all_of(cols_to_keep))

# Convert to long format ----
veg_cover_long = pivot_longer(veg_cover_simple,
                    cols = species0:species8,
                    names_to = "strata",
                    names_prefix = "([A-Za-z]+)",
                    values_to = "species_code",
                    values_drop_na = TRUE) %>% 
  pivot_longer(cols = dead0:dead8,
               names_to = "dead_strata",
               names_prefix = "([A-Za-z]+)",
               values_to = "dead_status",
               values_drop_na = FALSE) %>% # Do not drop NA because a few hits do not have anything entered for dead status
  mutate(keep_row = case_when(strata == dead_strata ~ 1,
                          .default = 0)) %>% 
  filter(keep_row == 1) %>% 
  ungroup()

# Drop abiotic elements ----
abiotic_elements = c("HL", "N", "DL", "NL", "WL", "W", "TH", "AE") # Are some of these (TH, AE) typos?

veg_cover_long = veg_cover_long %>% 
  filter(!(species_code %in% abiotic_elements))

# Correct dead status ----
# For entries that do not have anything written (n=3). Assume plant is alive i.e., dead status FALSE
veg_cover_long %>% 
  filter(is.na(dead_status))

veg_cover_long = veg_cover_long %>% 
  mutate(dead_status = case_when(is.na(dead_status) ~ "FALSE",
                                 .default = dead_status))

# Obtain accepted taxonomic name ----

# Join with USDA codes
veg_cover_taxa = veg_cover_long %>% 
  left_join(usda_codes, join_by("species_code"=="usda_code"))

# Explore codes with no matches
veg_cover_taxa %>% 
  filter(is.na(`Scientific Name with Author`)) %>% 
  distinct(species_code)

# All unknown codes end in '86' and seem to only be found in AK plots...? 
# Drop entries (n=256) for now until we can reconcile the codes 
veg_cover_taxa = veg_cover_taxa %>% 
  filter(!is.na(`Scientific Name with Author`))

# Remove author names from species name string
names_no_author = veg_cover_taxa %>% 
  distinct(`Scientific Name with Author`) %>% 
  mutate(name_original = str_remove_all(`Scientific Name with Author`, "\\("),
         name_original = str_remove_all(name_original, "\\)"),
         name_original = case_when(grepl("^[a-zA-Z]+ [a-z]+ [A-Z]+", name_original) ~ str_c(str_split_i(name_original, boundary("word"), i=1),
                                                                                           str_split_i(name_original, boundary("word"), i=2), 
                                                                                           sep=" "),
                                   
                                   grepl("^[a-zA-Z]+ [A-Z]", name_original) ~ str_split_i(name_original, boundary("word"), i=1),
                                        grepl("^[a-zA-Z]+ [a-z]+-[a-z]+ \\(", `Scientific Name with Author`) ~ str_split_i(`Scientific Name with Author`, " \\(", i=1),
                                        grepl("^[a-zA-Z]+ [a-z]+-[a-z]+ [A-Z]\\.", name_original) ~ str_remove(name_original, " [A-Z]\\."),
                                        .default = NA),
         name_original = case_when(`Scientific Name with Author` == "Betula ×dugleana Lepage" ~ "Betula × dugleana", 
                                   `Scientific Name with Author` == "Dryas octopetala L. ssp. octopetala" ~ "Dryas octopetala ssp. octopetala",
                                   `Scientific Name with Author` == "Dryas octopetala L. ssp. alaskensis (A.E. Porsild) Hultén" ~ "Dryas octopetala ssp. alaskensis",
                                   `Scientific Name with Author` == "Polygonum bistorta L. var. plumosum (Small) B. Boivin" ~ "Polygonum bistorta var. plumosum",
                                   .default = name_original))

# Ensure all cases have been addressed
names_no_author %>% filter(is.na(name_original))

# Join with veg cover data
veg_cover_taxa = veg_cover_taxa %>% 
  left_join(names_no_author, join_by(`Scientific Name with Author`))

# Join with AKVEG comprehensive checklist
veg_cover_taxa = veg_cover_taxa %>%
  left_join(taxa_all,join_by("name_original" == "taxon_name")) %>% 
  rename(name_adjudicated = taxon_name_accepted)

# Manually correct remaining unmatched codes
veg_cover_taxa = veg_cover_taxa %>% 
  mutate(name_adjudicated = case_when(name_original == "Cephalozia loitlesbergeri" ~ "Cephalozia",
                                      name_original == "Vaccinium oxycoccos" ~ "Oxycoccus microcarpus",
                                      name_original == "Betula × dugleana" ~ "Betula cf. occidentalis",
                                      name_original == "Polygonum bistorta" ~ "Bistorta plumosa",
                                      name_original == "Dryas octopetala" ~ "Dryas ajanensis ssp. beringensis",
                                      
                                      name_original == "Saxifraga bronchialis" ~ "Saxifraga funstonii",
                                      .default = name_original))

# Ensure that there are no other entries w/o a name_adjudicated
veg_cover_taxa %>% 
  filter(is.na(name_adjudicated)) %>% 
  distinct(name_original) 

# Calculate percent cover ---- 

# Calculate number of points per plot
# Plots should have 150 points (3 transects * 50 points per transects), but a handful have fewer than that
number_of_points = veg_cover_taxa %>% 
  group_by(site_visit_code) %>%
  summarize(max_hits = max(point_number)) %>% 
  select(site_visit_code, max_hits)

# Calculate total hits per species per plot
# Group by point_number first: even if a species occurs in multiple strata on a point, it only counts as 1 hit.
veg_cover_percent = veg_cover_taxa %>% 
  group_by(site_visit_code, point_number, name_original, name_adjudicated, dead_status) %>%
  summarize(hits = 1) %>% 
  group_by(site_visit_code, name_original, name_adjudicated, dead_status) %>% 
  summarize(total_hits = sum(hits)) %>% 
  left_join(number_of_points,by="site_visit_code") %>% # Join to obtain max number of points per plot
  mutate(cover_percent = signif((total_hits / max_hits * 100), digits = 3)) %>% # round to 3 decimal places
  ungroup()

# Populate remaining columns ----
veg_cover_summary = veg_cover_percent %>%
  mutate(cover_type = "absolute foliar cover") %>%
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(veg_cover_summary, is.na)
    , sum))

# Are the values for percent cover between 0% and 100%?
summary(veg_cover_summary$cover_percent)

# Are the correct number of sites included?
length(unique(site_visit_original$site_visit_code)) == length(unique(veg_cover_summary$site_visit_code))

# Export data ----
write_csv(veg_cover_summary, veg_cover_output)

# Clean workspace ----
rm(list=ls())
