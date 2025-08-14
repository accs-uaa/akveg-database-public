# Calculate cover percent from line-point intercept data for NPS CAKN.
rm(list=ls())

# Load packages ----
library(readxl)
library(tidyverse)

# Define directories ----
drive <- "F:"
root_folder <- "ACCS_Work/Projects/AKVEG_Database"
data_folder <- file.path(drive, root_folder, "Data/Data_Plots")
project_folder <- file.path(data_folder, "26_npsCAKN_2022")

# Define inputs ----
input_survey <- file.path(project_folder, "source", "CAKN_transect_cover_data_first_iteration.xlsx")
input_visit_id <- file.path(project_folder, "temp_files","visit_id_join_table.xlsx")
input_taxa_list <- file.path(project_folder, "source", "ref_taxon.xlsx")
input_codes <- file.path(project_folder, "temp_files","cakn_codes_biotic_vs_abiotic.xlsx")
input_lichen <- file.path(project_folder, "source", "CAKN_lichen_cover_finalized_full.xlsx")

# Define outputs ----
file_name <- paste0("05_nps_cakn_",str_replace_all(Sys.Date(),pattern="-",replacement=""),".csv")
output_cover <- file.path(project_folder, "temp_files",file_name)

# Read in data ----
survey <- read_xlsx(path=input_survey)
visit_id <- read_xlsx(path=input_visit_id)
taxa_list <- read_xlsx(path=input_taxa_list)
codes <- read_xlsx(path=input_codes)
lichen <- read_excel(path=input_lichen)

# Format data ----
# Some species codes are all lowercase, others start with a capital letter. Switch everything to lowercase to avoid join errors later on.
taxa_list$species_code <- str_to_lower(taxa_list$species_code)
survey$species_code <- str_to_lower(survey$species_code)

# Add 'site_visit_id' column to survey df
survey <- left_join(survey, visit_id, by="sample_event_num")

# Calculate cover percent ----

# Calculate maximum possible hits per plot
# First, calculate the length of each transect. Then sum across all 3 transects to arrive at a plot-level value.
hits <- survey %>% 
  group_by(site_visit_id, transect_code) %>% 
  summarize(distance = length(unique(distance_point))) %>% 
  arrange(site_visit_id,-distance) %>% 
  group_by(site_visit_id) %>% 
  summarize(max_hits = sum(distance))

# Calculate number of hits per species per plot
# If a species shows up multiple times on the same point, it still counts as just 1 hit
all_cover <- survey %>% 
  group_by(site_visit_id, transect_code, distance_point, species_code) %>% 
  summarize(hits = 1) %>% 
  group_by(site_visit_id, species_code) %>% 
  summarize(total_hits = length(hits))

all_cover <- left_join(all_cover, hits, by = "site_visit_id")

# Calculate % cover
all_cover <- all_cover %>% 
  mutate(cover_percent = total_hits / max_hits * 100) 

#### Append species names ----
# No reference names for lichen. ref_taxon only includes vascular and abiotic codes
taxa_list <- taxa_list %>% 
  select(species_code,scientific_name)

all_cover <- left_join(all_cover,taxa_list,by="species_code")

# Check that there aren't any unknowns
subset(all_cover,is.na(scientific_name))

#### Restrict to plants & fungi only ----
codes <- codes %>% 
  filter(include_in_cover == TRUE)

veg_cover <- all_cover %>% 
  filter(scientific_name %in% codes$name_original)

##### Match data to entry form ----

# Round cover percent to 3 decimal places
veg_cover$cover_percent <- round(veg_cover$cover_percent, digits = 3)

# Add columns: 'cover type', 'name_adjudicated'
# Change 'scientific_name' to 'name_original'
# If species name was recorded, plant was alive (all dead plants were lumped by functional groups e.g., Standing Dead Forb)
veg_cover <- veg_cover %>%
  mutate(cover_type = "absolute foliar cover",
         name_adjudicated = "",
         dead_status = FALSE) %>% 
  rename(name_original = scientific_name) %>% 
  select(site_visit_id, name_original, name_adjudicated, cover_type, dead_status, cover_percent)

rm(all_cover, taxa_list, survey, visit_id, hits)

# Append lichen data ----

# Convert to long form
lichen <- pivot_longer(lichen,
                       cols = ALAL61:XAPO60,
                       names_to = "name_original",
                       values_to = "cover")

# Remove rows for which cover = 0
lichen <- lichen %>% 
  filter(cover > 0) %>%
  select(-c(actual_latitude,actual_longitude))

# Create site ID that matches cover ID
# Remove -1M_V from 'grid' name and append plot number
lichen$grid <- str_sub(lichen$grid, end=-6)
lichen$site_code <- paste(lichen$grid,lichen$plot,sep="")

# Match data to entry form ----
lichen <- lichen %>% 
  rename(cover_percent = cover) %>% 
  mutate(name_adjudicated = "",
         cover_type = "absolute foliar cover",
         dead_status = FALSE)

# Round cover percent to 3 decimal places
lichen$cover_percent <- round(lichen$cover_percent, digits = 3)

# Merge with vegetation cover data ----

# Create site ID column in cover data 
sites <- as.data.frame(unique(veg_cover$site_visit_id))
colnames(sites) <- "site_visit_id"
sites$site_code <- str_sub(sites$site_visit_id, end=-10)

# Append site_visit_id to lichen data
# Some lichen sites (n=11) do not exist in the veg cover dataset. We are not adding these missing sites.
lichen_subset <- lichen %>% 
  filter(site_code %in% sites$site_code)

lichen_subset <- left_join(lichen_subset,sites,by="site_code")

lichen_subset <- lichen_subset %>% 
  select(site_visit_id,name_original, name_adjudicated, cover_type, dead_status, cover_percent)

veg_cover <- rbind(veg_cover,lichen_subset)
veg_cover <- veg_cover %>% 
  arrange(site_visit_id)

# Remove any 'general lichen' codes for plots that have lichen data
veg_cover <- veg_cover %>% 
  filter(!(site_visit_id %in% lichen_subset$site_visit_id & grepl(pattern="lichen",name_original,ignore.case=TRUE)))

# Export data ----
write_csv(veg_cover,output_cover, na="")