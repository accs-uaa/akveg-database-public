# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Calculate Vegetation Cover for ACCS Nelchina 2022 data"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-03-28
# Usage: Must be executed in R version 4.4.1+.
# Description: "Calculate Vegtation Cover for ACCS Nelchina 2022 data" uses data from line-point surveys to calculate plot-level percent foliar cover for each recorded species. It also appends unique site visit identifiers, corrects taxonomic names using the AKVEG database as a taxonomic standard, populates required metadata fields, and performs QA/QC checks. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr)
library(fs)
library(readr)
library(readxl)
library(RPostgres)
library(tidyr)
library(stringr)

# Define directories ----

# Set root directory
drive = 'C:'
root_folder = 'ACCS_Work'

# Define input folders
project_folder = path(drive, root_folder, 'OneDrive - University of Alaska', 'ACCS_Teams', 'Vegetation', 'AKVEG_Database')
plot_folder = path(project_folder, 'Data', 'Data_Plots', '31_accs_nelchina_2022')
template_folder = path(project_folder, "Data", "Data_Entry")
source_folder = path(plot_folder, 'source')

# Set repository directory
repository_folder = path(drive,root_folder,'Repositories', 'akveg-database')

# Set credentials directory
credential_folder = path(drive,root_folder, 'Servers_Websites', 'Credentials', 'akveg_public_read')

# Define datasets ----

# Define input datasets
trace_cover_input = path(plot_folder, "source","05_accs_nelchina_additional_species.xlsx")
lpi_cover_input = path(plot_folder,"source","05_accs_nelchina_lpi_surveys.xlsx")
site_visit_input = path(plot_folder, "03_sitevisit_accsnelchina2022.csv")
template_input = path(template_folder, "05_vegetation_cover.xlsx")

# Define output dataset
veg_cover_output = path(plot_folder, '05_vegetationcover_accsnelchina2022.csv')

# Define credentials file
authentication =  path(credential_folder, 'authentication_akveg_public_read.csv')

# Read in data ----
trace_cover_original = read_xlsx(path=trace_cover_input)
lpi_cover_original = read_xlsx(path=lpi_cover_input, col_types=c("text","numeric","numeric",
                                                                 "text","text","text",
                                                                 "text","text","text",
                                                                 "text","text"))
site_visit_original = read_csv(site_visit_input, col_select=c('site_visit_id', 'site_code'))
abiotic_list_input = path(project_folder, 'Data', 'Tables_Metadata', 'csv', 'ground_element.csv')
template = colnames(read_xlsx(path=template_input))

# Query AKVEG database ----

# Import database connection function
connection_script = path(repository_folder,
                         'package_DataProcessing',
                         'connect_database_postgresql.R')
source(connection_script)

# Connect to the AKVEG PostgreSQL database
akveg_connection = connect_database_postgresql(authentication)

# Define taxa query
query_taxa = "SELECT taxon_all.taxon_code as taxon_code
, taxon_all.taxon_name as taxon_name
, taxon_status.taxon_status as taxon_status
, taxon_accepted_name.taxon_name as taxon_name_accepted
FROM taxon_all
  LEFT JOIN taxon_accepted ON taxon_all.taxon_accepted_code = taxon_accepted.taxon_accepted_code
  LEFT JOIN taxon_all taxon_accepted_name ON taxon_all.taxon_accepted_code = taxon_accepted_name.taxon_code
  LEFT JOIN taxon_status ON taxon_all.taxon_status_id = taxon_status.taxon_status_id
ORDER BY taxon_name_accepted, taxon_status, taxon_name;"

# Define abiotic/ground element query
query_abiotic = "SELECT * FROM ground_element"

# Read SQL table as dataframe
taxa_all = as_tibble(dbGetQuery(akveg_connection, query_taxa))
abiotic_list = as_tibble(dbGetQuery(akveg_connection, query_abiotic))

# Append site visit code ----
lpi_cover = left_join(lpi_cover_original, site_visit_original, by="site_code")
trace_cover = left_join(trace_cover_original, site_visit_original, by="site_code")

# Ensure all entries have a site visit code
lpi_cover %>% 
  filter(is.na(site_visit_id)) %>% 
  nrow()

trace_cover %>% 
  filter(is.na(site_visit_id)) %>% 
  nrow()

# Ensure that all sites in Site Visit table are associated with an entry in the veg cover data
which(!site_visit_original$site_visit_id %in% unique(lpi_cover$site_visit_id)) # OK to proceed. 'Missing' site NLS3_999 is included in 'trace cover' instead because there was no LPI at this site (visual estimate only)
which(!site_visit_original$site_visit_id %in% unique(trace_cover$site_visit_id)) # Included in LPI, looks like a low diversity site - assume no trace species detected

# Convert to long format ----
lpi_cover_long = pivot_longer(lpi_cover, 
                              cols = layer_1:layer_8,
                              names_to = "strata",
                              names_prefix = "layer_",
                              values_to = "code",
                              values_drop_na = TRUE)

# Create dead_status ----
# Remove dead status + basal designation from code
lpi_cover_long = lpi_cover_long %>% 
  mutate(dead_status = case_when(grepl(pattern="-D", code) ~ "TRUE",
                                 .default = "FALSE"),
         code = str_replace_all(code,
                                pattern=c("-D"="", "-B"="")))

# Correct abiotic codes ----
# To match formatting used in data dictionary
lpi_cover_long = lpi_cover_long %>% 
  mutate(code = case_when(code == 'O' ~ 'OS',
                          code == 'HL' ~ 'L',
                          code == 'S' ~ 'MS',
                          code == 'W' ~ 'WA',
                          .default = code))

# Drop abiotic elements ----
lpi_cover_long = lpi_cover_long %>% 
  filter(!(code %in% abiotic_list$ground_element_code))

# Calculate cover percent ----
# Group by line & point first to make sure species don't get counted twice per point, then group by plot
# Each species can appear a maximum of 120 times per plot
veg_cover_data = lpi_cover_long %>% 
  group_by(site_visit_id, line, point, code, dead_status) %>% 
  summarize(hits = 1) %>% 
  group_by(site_visit_id, code, dead_status) %>% 
  summarize(total_hits = sum(hits)) %>% 
  mutate(cover_percent = total_hits/120*100,
         cover_type = "absolute foliar cover") %>% 
  select(-total_hits) %>% 
  ungroup()

veg_cover_data$dead_status = as.logical(veg_cover_data$dead_status)

# Append trace species data ----

# Create unique site code - taxon name - dead status id to compare both datasets
veg_cover_data = veg_cover_data %>% 
  mutate(site_taxon_id = paste(site_visit_id,
                               code,
                               dead_status,
                               sep="_"))

trace_cover = trace_cover %>% 
  mutate(site_taxon_id = paste(site_visit_id,
                               name_original,
                               dead_status,sep="_"))

# Are there any duplicated entries within the trace data itself?
# These are worth checking manually to see if that is a field error or a data entry error, and if the cover percentages are the same
which(duplicated(trace_cover$site_taxon_id))

# Are there any duplicated entries between the LPI and the trace data?
# These can usually be removed from the trace data since the observer likely forgot they had already hit that species
trace_cover = trace_cover %>% 
  mutate(duplicate_entry = site_taxon_id %in% veg_cover_data$site_taxon_id) %>% 
  filter(duplicate_entry==FALSE) %>% 
  select(-duplicate_entry)

# Format trace_species data to match veg/lpi cover data
trace_cover = trace_cover %>% 
  rename(code = name_original) %>% 
  select(site_visit_id, code, cover_type, cover_percent,dead_status)

veg_cover_data = bind_rows(veg_cover_data,trace_cover)

veg_cover_data = veg_cover_data %>% 
  arrange(site_visit_id, code, dead_status)

# Obtain accepted taxonomic name ----
# Convert cover_data code to lowercase to match formatting in taxonomic tables
veg_cover_taxa = veg_cover_data %>% 
  mutate(code = str_to_lower(code))

# Partial merge with AKVEG Checklist to obtain 'original' taxon name
veg_cover_taxa = taxa_all %>% 
  select(taxon_code, taxon_name) %>% 
  right_join(veg_cover_taxa, join_by(taxon_code == code))

# Address codes that did not match with a name in the checklist
# Drop PYRFRI and CHEGRA
veg_cover_taxa %>% 
  filter(is.na(taxon_name)) %>% 
  distinct(taxon_code)

veg_cover_taxa = veg_cover_taxa %>% 
  filter(!(taxon_code == "pyrfri" | taxon_code == "chegra")) %>% 
  mutate(taxon_name = case_when(taxon_code == "brachy" ~ "Brachythecium",
                                taxon_code == "callie" ~ "Calliergonella",
                                taxon_code == "carant" ~ "Carex anthoxanthea",
                                taxon_code == "carmic" ~ "Carex microchaeta",
                                taxon_code == "dicran" ~ "Dicranum",
                                taxon_code == "fgcrulic" ~ "crustose lichen (non-orange)",
                                taxon_code == "fgfeamos" ~ "feathermoss (other)",
                                taxon_code == "fgfollic" ~ "foliose lichen (other)",
                                taxon_code == "fgturmos" ~ "turf moss",
                                taxon_code == "poaarc" ~ "Poa arctica",
                                taxon_code == "polytr" ~ "Polytrichum",
                                taxon_code == "salarc" ~ "Salix arctica",
                                taxon_code ==  "silacu" ~ "Silene acaulis",
                                taxon_code == "sphagnum" ~ "Sphagnum",
                                taxon_code == "tepatr" ~ "Tephroseris atropurpurea",
                                .default = taxon_name))

# Join with AKVEG Checklist on taxon name to obtain name adjudicated
veg_cover_taxa = taxa_all %>%
  select(taxon_name, taxon_name_accepted) %>% 
  right_join(veg_cover_taxa, by = "taxon_name") %>% 
  rename(name_adjudicated = taxon_name_accepted,
         name_original = taxon_name)

# Ensure that there are no other entries w/o a name_adjudicated
veg_cover_taxa %>% 
  filter(is.na(name_adjudicated)) %>% 
  distinct(name_original) 

# Ensure that all accepted names are in the AKVEG checklist
which(!(unique(veg_cover_taxa$name_adjudicated) %in% unique(taxa_all$taxon_name_accepted)))

# Match template formatting ----
veg_cover_final = veg_cover_taxa %>% 
  arrange(site_visit_id, name_adjudicated) %>% 
  rename(site_visit_code = site_visit_id) %>% 
  mutate(cover_percent = signif(cover_percent, digits = 3)) %>% 
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(veg_cover_final, is.na)
    , sum))

# Are the values for percent cover between 0% and 100%?
summary(veg_cover_final$cover_percent)

# Are the correct number of sites included?
length(unique(site_visit_original$site_visit_id)) == length(unique(veg_cover_final$site_visit_code))

# Are there any duplicates?
# Consider entries with different percent cover as well (indicates that summary step was not completed)
veg_cover_final %>% 
  distinct(site_visit_code, name_adjudicated, dead_status) %>% 
  nrow() == nrow(veg_cover_final) # If TRUE, no duplicates to address

veg_cover_final %>% 
  distinct(site_visit_code, name_adjudicated, dead_status) %>% 
  nrow() == nrow(veg_cover_final) # If TRUE, no duplicates to address

# Export data ----
write_csv(veg_cover_final,veg_cover_output)

# Clean workspace ----
rm(list=ls())
