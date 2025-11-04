# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# "Format Vegetation Cover for Yukon Biophysical Inventory System Plots"
# Author: Amanda Droghini, Alaska Center for Conservation Science
# Last Updated: 2025-10-02
# Usage: Must be executed in R version 4.5.1+.
# Description: "Format Vegetation Cover for Yukon Biophysical Inventory System Plots" formats vegetation cover data for ingestion into the AKVEG Database. The script appends unique site visit identifiers, corrects taxonomic names using the AKVEG comprehensive checklist, and enforces formatting to match the AKVEG template. The output is a CSV table that can be converted and included in a SQL INSERT statement.
# ---------------------------------------------------------------------------

# Load packages ----
library(dplyr, warn.conflicts = FALSE)
library(fs)
library(readr)
library(readxl)
library(RPostgres)
library(stringr)
library(tidyr)

# Define directories ----

# Set root directory
drive <- "C:"
root_folder <- "ACCS_Work"

# Define input folders
project_folder <- path(drive, root_folder, "OneDrive - University of Alaska", "ACCS_Teams", "Vegetation", "AKVEG_Database")
plot_folder <- path(project_folder, "Data", "Data_Plots", "52_yukon_biophysical_2020")
template_folder <- path(project_folder, "Data", "Data_Entry")
source_folder <- path(plot_folder, "source", "ECLDataForAlaska_20240919", "YBIS_Data")

# Set repository directory
repository_folder <- path(drive, root_folder, "Repositories", "akveg-database-public")

# Set credentials directory
credential_folder <- path(project_folder, "Credentials", "akveg_public_read")

# Define datasets ----

# Define input datasets
visit_input <- path(plot_folder, "03_sitevisit_yukonbiophysical2020.csv")
veg_input <- path(source_folder, "Veg_2024Apr09.xlsx")
template_input <- path(template_folder, "05_vegetation_cover.xlsx")

# Define output dataset
veg_output <- path(plot_folder, "05_vegetationcover_yukonbiophysical2020.csv")

# Define credentials file
authentication <- path(credential_folder, "authentication_akveg_public_read.csv")

# Define query file
taxa_file <- path(repository_folder, "queries", "00_taxonomy.sql")

# Read in data ----
visit_original <- read_csv(visit_input, col_select = c("site_code", "site_visit_code"))
veg_original <- read_xlsx(veg_input, .name_repair = "universal") ## Ignore warnings
template <- colnames(read_xlsx(path = template_input))

# Query AKVEG database ----

# Import database connection function
connection_script <- path(
  repository_folder,
  "pull_functions", "connect_database_postgresql.R"
)
source(connection_script)

# Connect to the AKVEG PostgreSQL database
akveg_connection <- connect_database_postgresql(authentication)

# Define query
query_taxa <- read_file(taxa_file)

# Read SQL table as dataframe
taxa_all <- as_tibble(dbGetQuery(akveg_connection, query_taxa))

# Append site visit code ----
veg_data <- veg_original %>%
  mutate(site_code = str_c(Project.ID, Plot.ID, sep = "_")) %>%
  right_join(visit_original, by = "site_code") %>% ## Drop sites that aren't included in site table
  select(
    site_visit_code, Scientific.name, Veg.cover.pct,
    Needs.checking.flg, Stratum.display.order, Veg.stratum.cd, Access
  )

# Ensure every site visit has at least one entry
print(veg_data %>% filter(is.na(Veg.cover.pct)))

# Standardize scientific names ----

# Correct non-matching codes
veg_taxa <- veg_data %>%
  mutate(
    name_original = str_remove(Scientific.name, " sp\\."),
    name_original = str_remove(name_original, " spp\\.")
  ) %>%
  mutate(name_original = case_when(grepl("Bryophyte", name_original) ~ "bryophyte",
    name_original == "Lichen" ~ "lichen",
    name_original == "Lichen crustose" ~ "crustose lichen",
    name_original == "Liverwort" ~ "liverwort",
    grepl("^Alga", name_original) ~ "algae",
    name_original == "Picea x 3" ~ "Picea",
    name_original == "Cladonia arbuscula ssp mitis" ~ "Cladonia arbuscula ssp. mitis",
    name_original == "Hierochloe hirta" ~ "Hierochloë hirta",
    name_original == "Galium brandegeei" ~ "Galium brandegei",
    name_original == "Carex amblyorhyncha" ~ "Carex amblyorrhyncha",
    name_original == "Deschampsia caespitosa" ~ "Deschampsia cespitosa",
    .default = name_original
  )) %>%
  filter(name_original != "Unspecified" & name_original != "Fungus" & Veg.stratum.cd != "NV" & name_original != "Temporarily unidentified") ##  Remove non-plant codes

# Join with AKVEG comprehensive checklist
veg_taxa <- veg_taxa %>%
  left_join(taxa_all, join_by("name_original" == "taxon_name")) %>%
  rename(name_adjudicated = taxon_accepted)

# Address codes without a match
veg_taxa <- veg_taxa %>%
  mutate(name_adjudicated = case_when(name_original == "Poaceae" ~ "grass (Poaceae)",
    name_original == "Alnus alnobetula ssp. crispa" ~ "Alnus alnobetula ssp. fruticosa",
    grepl("^Alopecurus", Scientific.name) & is.na(name_adjudicated) ~ "Alpinus",
    name_original == "Andromeda polifolia var. polifolia" ~ "Andromeda polifolia",
    name_original == "Artemisia canadensis" ~ "Artemisia borealis",
    name_original == "Artemisia norvegica" ~ "Artemisia arctica",
    name_original == "Artemisia rupestris" ~ "Artemisia woodii",
    name_original == "Astragalus adsurgens" ~ "Astragalus laxmannii",
    name_original == "Astragalus eucosmus ssp. eucosmus" ~ "Astragalus eucosmus",
    name_original == "Betula x dugleana" ~ "Betula cf. occidentalis",
    name_original == "Bromus carinatus" ~ "grass (Poaceaea)",
    name_original == "Cardamine oligosperma" ~ "Cardamine umbellata",
    name_original == "Carex x flavicans" ~ "Carex subspathacea",
    name_original == "Chrysanthemum" ~ "forb",
    name_original == "Cratoneuron arcticum" ~ "Hygroamblystegium varium",
    name_original == "Cryptogramma crispa var." ~ "Cryptogramma acrostichoides",
    name_original == "Draba alpina" ~ "Draba",
    name_original == "Dryas octopetala" ~ "Dryas ajanensis ssp. beringensis",
    grepl("Elyhordeum", Scientific.name) & is.na(name_adjudicated) ~ "grass (Poaceae)",
    name_original == "Eriophorum vaginatum ssp. vaginatum" ~ "Eriophorum vaginatum",
    name_original == "Galium labradoricum" ~ "Galium",
    name_original %in% c("Lagotis glauca ssp. minor", "Lagotis stelleri") ~ "Lagotis glauca ssp. lanceolata",
    name_original == "Minuartia" ~ "forb",
    name_original == "Myriophyllum spicatum" ~ "Myriophyllum sibiricum",
    name_original == "Melandrium apetalum" ~ "Silene uralensis",
    name_original == "Oxytropis borealis var. hudsonica" ~ "Oxytropis borealis",
    name_original == "Oxytropis nigrescens ssp. nigrescens" ~ "Oxytropis bryophila",
    name_original == "Pedicularis sudetica" ~ "Pedicularis",
    name_original == "Poa alpina ssp. vivipara" ~ "Poa alpina var. alpina",
    name_original == "Potentilla egedii" ~ "Potentilla anserina ssp. groenlandica",
    name_original == "Ranunculus gmelinii var. gmelinii" ~ "Ranunculus gmelinii ssp. gmelinii",
    name_original == "Salix arctica ssp. arctica" ~ "Salix arctica",
    name_original == "Salix brachycarpa" ~ "Salix niphoclada",
    name_original == "Saxifraga bronchialis" ~ "Saxifraga funstonii",
    name_original == "Saxifraga davurica" ~ "Micranthes",
    name_original == "Scirpus caespitosus" ~ "Trichophorum cespitosum",
    name_original == "Silene acaulis ssp. acaulis" ~ "Silene acaulis",
    name_original == "Taraxacum lyratum" ~ "Taraxacum scopulorum",
    name_original == "Xanthoria" ~ "lichen",
    name_original == "Vaccinium microcarpum" ~ "Oxycoccus microcarpus",
    name_original == "Vaccinium oxycoccos" ~ "Oxycoccus microcarpus",
    name_original == "Xanthoparmelia chlorochroa" ~ "Xanthoparmelia",
    .default = name_adjudicated
  ))

# Ensure that all codes returned a match
print(veg_taxa %>%
  filter(is.na(name_adjudicated)) %>%
  nrow())

# Ensure that all accepted names are in the AKVEG checklist
print(which(!(veg_taxa$name_adjudicated %in% unique(taxa_all$taxon_accepted))))

# Format dead status ----
# If veg stratum = Snag, set dead_status to TRUE. Assume all other plants are live.
veg_taxa <- veg_taxa %>%
  mutate(dead_status = case_when(Veg.stratum.cd == "SN" ~ "TRUE",
    .default = "FALSE"
  ))

# Summarize percent cover ---
veg_final <- veg_taxa %>%
  group_by(site_visit_code, name_original, name_adjudicated, dead_status) %>%
  summarize(cover_percent = sum(Veg.cover.pct))

# Format remaining columns ----
veg_final <- veg_final %>%
  mutate(
    cover_type = "absolute foliar cover", # Review
    cover_percent = signif(cover_percent, digits = 3)
  ) %>% # Round to 3 decimal places
  select(all_of(template))

# QA/QC -----

# Do any of the columns have null values that need to be addressed?
cbind(
  lapply(
    lapply(veg_final, is.na),
    sum
  )
)

# Are the values for percent cover between 0% and 100%?
temp <- veg_final %>%
  group_by(site_visit_code) %>%
  summarise(total_sum = sum(cover_percent)) %>%
  arrange(-total_sum)

# Are values for dead status Boolean?
print(table(veg_final$dead_status))

# Are the correct number of sites included?
visit_original %>%
  filter(!(site_visit_code %in% veg_final$site_visit_code)) %>%
  select(site_visit_code) # Plots with 100% abiotic cover

# Are there any duplicates?
veg_final %>%
  distinct(site_visit_code, name_adjudicated, dead_status) %>%
  nrow() == nrow(veg_final) # If TRUE, no duplicates to address

# Export data ----
write_csv(veg_final, veg_output)

# Clean workspace ----
rm(list = ls())
