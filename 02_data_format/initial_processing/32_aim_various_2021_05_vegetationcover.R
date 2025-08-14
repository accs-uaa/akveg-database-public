# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Vegetation Cover for AIM Various 2021 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2024-11-12
# Usage: Must be executed in R version 4.4.1+.
# Description: "Calculate Vegetation Cover for AIM Various 2021" uses data from visual estimate surveys to calculate site-level percent foliar cover for each recorded species. It also appends unique site visit identifiers, corrects taxonomic names using the AKVEG database as a taxonomic standard, populates required metadata fields, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
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
plot_folder = path(project_folder, 'Data/Data_Plots/32_aim_various_2021')
template_folder = path(project_folder, "Data/Data_Entry")

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(project_folder, 'Credentials', 'akveg_public')

# Define datasets ----

# Define input datasets
veg_cover_input = path(plot_folder, 'working', 'AIM_Terrestrial_Alaska_LPI.csv')
plant_codes_input = path(plot_folder, 'source', "tblStateSpecies.csv")
site_visit_input = path(plot_folder, "03_sitevisit_aimvarious2021.csv")
template_input = path(template_folder, "05_vegetation_cover.xlsx")

# Define output dataset
veg_cover_output = path(plot_folder, "05_vegetationcover_aimvarious2021.csv")

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public.csv')

# Read in data ----
veg_cover_original = read_csv(veg_cover_input)
site_visit_original = read_csv(site_visit_input)
plant_codes_original = read_csv(plant_codes_input, col_select=c('SpeciesCode', 'ScientificName'))
template = colnames(read_xlsx(path=template_input))

# Query AKVEG database ----

# Import database connection function
connection_script = path(repository_folder,
                         'package_DataProcessing',
                         'connect_database_postgresql.R')
source(connection_script)

# Connect to the AKVEG PostgreSQL database
akveg_connection = connect_database_postgresql(authentication)

# Define query
query_taxa = "SELECT taxon_all.taxon_code as taxon_code
, taxon_all.taxon_name as taxon_name
, taxon_status.taxon_status as taxon_status
, taxon_accepted_name.taxon_name as taxon_name_accepted
FROM taxon_all
  LEFT JOIN taxon_accepted ON taxon_all.taxon_accepted_code = taxon_accepted.taxon_accepted_code
  LEFT JOIN taxon_all taxon_accepted_name ON taxon_all.taxon_accepted_code = taxon_accepted_name.taxon_code
  LEFT JOIN taxon_status ON taxon_all.taxon_status_id = taxon_status.taxon_status_id
ORDER BY taxon_name_accepted, taxon_status, taxon_name;"

# Read SQL table as dataframe
taxa_all = as_tibble(dbGetQuery(akveg_connection, query_taxa))

# Restrict projects ----
projects_to_include = c("Alaska Anchorage FO 2018","ALASKA_GMT2_2021","KobukSeward-WEST 2020","KobukSeward-NORTHEAST 2021")

veg_cover = veg_cover_original %>% 
  filter(ProjectName %in% projects_to_include)

# Format site code ----
# To match site visit table
veg_cover = veg_cover %>% 
  mutate(site_code = case_when(grepl('P-38_EXCLOSURE', PlotID) ~ str_remove_all(PlotID, ' '),
                               ProjectName=="ALASKA_GMT2_2021" ~ str_pad(PlotID,3,side="left",pad="0"),
                               .default = PlotID),
         site_code = case_when(ProjectName=="ALASKA_GMT2_2021" ~ str_c("GMT2",site_code,sep="-"),
                               .default = site_code))

# Append site visit code ----
site_visit = site_visit_original %>% 
  select(site_code, site_visit_id)

veg_cover = veg_cover %>%
  right_join(site_visit, by="site_code") %>% 
  rename(site_visit_code = site_visit_id)

# Explore cover data ----

# Ensure all entries have a site visit code
veg_cover %>% 
  filter(is.na(site_visit_code)) %>% 
  nrow()

# Ensure that all sites in Site Visit table are associated with an entry in the veg cover data
which(!site_visit$site_visit_id %in% unique(veg_cover$site_visit_code))

# Drop extraneous columns ----

# 1) Select columns to keep. I didn't keep "ChkboxLower5", "ChkboxLower6", and "ChkboxLower7" since there aren't any hits for those strata (Lower5, Lower6, and Lower7 columns are all NAs). I'm assuming the zeroes were meant to be NAs...??
# 2) Change "0" and "1" in Chkbox columns to "TRUE" and "FALSE"
# 3) Rename columns w/ species hits so that each column name has information on the stratum

columns_to_keep = c("site_visit_code",'RecKey',"PointNbr","species0","species1","species2","species3","species4","species5","dead0","dead1","dead2","dead3","dead4","dead5")

veg_cover = veg_cover %>% 
  mutate(dead0 = if_else(ChkboxTop == 0, "FALSE","TRUE"),
         dead1 = if_else(ChkboxLower1 == 0, "FALSE","TRUE"),
         dead2 = if_else(ChkboxLower2 == 0, "FALSE","TRUE"),
         dead3 = if_else(ChkboxLower3 == 0, "FALSE","TRUE"),
         dead4 = if_else(ChkboxLower4 == 0, "FALSE","TRUE"),
         dead5 = if_else(ChkboxSoil == 0, "FALSE","TRUE")) %>% 
  rename(species0 = TopCanopy,
         species1 = Lower1,
         species2 = Lower2,
         species3 = Lower3,
         species4 = Lower4,
         species5 = SoilSurface) %>% 
  select(all_of(columns_to_keep))

# Convert to long format ----
veg_cover_long = pivot_longer(veg_cover, 
                           cols = species0:species5,
                           names_to = "strata",
                           names_prefix = "([A-Za-z]+)",
                           values_to = "code",
                           values_drop_na = TRUE)

veg_cover_long = pivot_longer(veg_cover_long, 
                              cols = dead0:dead5,
                              names_to = "strata_dead",
                              names_prefix = "([A-Za-z]+)",
                              values_to = "dead_status",
                              values_drop_na = TRUE) %>% 
  mutate(rows_to_keep = case_when(strata == strata_dead ~ 'TRUE',
                                  .default = 'FALSE')) %>% 
  filter(rows_to_keep == 'TRUE') %>% 
  select(-c(strata_dead, rows_to_keep))

# Calculate percent cover ---- 

# Calculate maximum number of hits per plot i.e., what is the maximum number of times a species could have been recorded, if it occurred on every line and every point along that line. Each plot should have 150 points, but some might have a little less or a little more.
hits_per_plot = veg_cover_long %>% 
  group_by(site_visit_code, RecKey) %>%
  summarize(hits_per_line = max(PointNbr)) %>% 
  group_by(site_visit_code) %>% 
  summarize(max_hits = sum(hits_per_line))

summary(hits_per_plot$max_hits)

# Calculate percent cover
# First, calculate total number of hits per species per plot. Group by PointNbr: even if a species occurs in multiple strata on a point, it only counts as 1 hit.
veg_cover_summary = veg_cover_long %>% 
  group_by(site_visit_code, PointNbr, code, dead_status) %>%
  summarize(hits = 1) %>% 
  group_by(site_visit_code, code, dead_status) %>% 
  summarize(total_hits = sum(hits)) %>% 
  # Add maximum number of hits
  left_join(hits_per_plot, by="site_visit_code") %>% 
  mutate(cover_percent = total_hits / max_hits * 100) %>% 
  ungroup()

# Obtain accepted taxonomic name ----

# Cross-reference species code to BLM species list
veg_cover_taxa = veg_cover_summary %>% 
  left_join(plant_codes_original, join_by("code" == "SpeciesCode"))

# Add missing names to BLM species list
veg_cover_taxa = veg_cover_taxa %>% 
  mutate(ScientificName = case_when(code == '2ALGA' ~ 'algae',
                                    code == '2FUNGI' ~ 'fungus',
                                    code == '2LICHN' ~ 'lichen',
                                    code == '2LVRWRT' ~ 'liverwort',
                                    code == '2LW' ~ 'liverwort',
                                    code == '2MOSS' ~ 'moss',
                                    code == 'ALBO86' ~ 'Alopecurus borealis',
                                    code == 'ANPI7' ~ 'Aneura pinguis',
                                    code == 'ASALA9' ~ 'Astragalus alpinus ssp. arcticus',
                                    code == 'BRMI5' ~ 'Brachythecium mildeanum',
                                    code == 'CILA70' ~ 'Cinclidium latifolium',
                                    code == 'CEISI' ~ 'Cetraria islandica ssp. islandica',
                                    code == 'CLAL11' ~ 'Cladonia albonigra',
                                    code == 'CEPHA6' ~ 'Cephaloziella',
                                    code == 'CALU27' ~ 'Carex lugens', # Likely a mistake in the code, CALU2 not CALU27
                                    code == 'DESUO86' ~ 'Deschampsia sukatschewii ssp. orientalis',
                                    code == 'DRAR86' ~ 'Drepanocladus arcticus',
                                    code == 'OROB86' ~ 'Orthilia obtusata',
                                    code == 'SPCO14' ~ 'Sphagnum contortum',

                                    code == 'POALA2' ~ 'Polytrichastrum alpinum',
                                    code == 'MOSS' ~ 'moss',
                                    code == 'SPAL86' ~ 'Sphagnum alaskense',
                                    .default = ScientificName))

# Examine which codes did not return a match, but restrict to only those codes that start with 4 letters. All others are unknown or abiotic elements
veg_cover_taxa %>% 
  filter(is.na(ScientificName) & grepl("[A-Z]{4}", code)) %>% 
  distinct(code, ScientificName) %>% 
  arrange(code)

# OK to drop all unmatched codes
veg_cover_taxa = veg_cover_taxa %>% 
  filter(!is.na(ScientificName))

# Join with AKVEG comprehensive checklist
veg_cover_taxa = veg_cover_taxa %>%
  left_join(taxa_all,by=c("ScientificName" = "taxon_name")) %>% 
  rename(name_original = 'ScientificName',
         name_adjudicated = taxon_name_accepted)

# Correct remaining codes
veg_cover_taxa = veg_cover_taxa %>%
  mutate(name_adjudicated = case_when(name_original == 'Astragalus alpinus var. alpinus' ~ 'Astragalus alpinus',
                                       name_original == 'Bromus inermis ssp. pumpellianus var. arcticus' ~ 'Bromus pumpellianus var. arcticus',
                                       name_original == 'Cerastium beeringianum ssp. beeringianum var. beeringianum' ~ 'Cerastium beeringianum var. beeringianum',
                                       name_original == 'Cerastium beeringianum ssp. beeringianum var. grandiflorum' ~ 'Cerastium beeringianum var. grandiflorum',
                                       name_original == 'Vaccinium oxycoccos' ~ 'Oxycoccus microcarpus',
                                       .default = name_adjudicated))

# Ensure that there are no other entries w/o a name_adjudicated
veg_cover_taxa %>% 
  filter(is.na(name_adjudicated)) %>% 
  distinct(name_original) 

# Ensure that all accepted names are in the AKVEG checklist
which(!(unique(veg_cover_taxa$name_adjudicated) %in% unique(taxa_all$taxon_name_accepted)))

# Summarize percent cover ---
veg_cover_final = veg_cover_taxa %>% 
  group_by(site_visit_code, name_original, name_adjudicated, dead_status) %>%
  summarize(cover_percent = sum(cover_percent))

# Populate remaining columns ----
veg_cover_final = veg_cover_final %>%
  mutate(cover_type = "absolute foliar cover",
         cover_percent = signif(cover_percent, digits = 3)) %>%
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(veg_cover_final, is.na)
    , sum))

# Are the values for percent cover between 0% and 100%?
summary(veg_cover_final$cover_percent)

# Are values for dead status boolean?
table(veg_cover_final$dead_status)

# Are the correct number of sites included?
length(unique(site_visit_original$site_visit_id)) == length(unique(veg_cover_final$site_visit_code))

# Are there any duplicates?
# Consider entries with different percent cover as well (indicates that summary step was not completed)
veg_cover_final %>% 
  distinct(site_visit_code, name_adjudicated, dead_status) %>% 
  nrow() == nrow(veg_cover_final) # If TRUE, no duplicates to address

# Export data ----
write_csv(veg_cover_final, veg_cover_output)

# Clean workspace ----
rm(list=ls())
