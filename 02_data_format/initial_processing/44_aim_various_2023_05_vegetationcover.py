# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------------
# Format Vegetation Cover Table for BLM AIM Various 2023 data
# Author: Amanda Droghini, Alaska Center for Conservation
# Last Updated: 2025-10-30
# Usage: Must be executed in a Python 3.13+ distribution.
# Description: "Format Vegetation Cover Table for BLM AIM Various 2023 data" uses data from line-point intercept surveys
# to calculate site-level percent foliar cover for each recorded species. It also appends unique site visit
# identifiers, corrects taxonomic names using the AKVEG database as a taxonomic standard, populates required
# metadata fields, and performs QA/QC checks. The script depends on the output from the
# 44_aim_various_2023_00_extract_data.py script. The output is a CSV table that can be converted and included in a
# SQL INSERT statement.
# ---------------------------------------------------------------------------

# Import packages
import polars as pl
from pathlib import Path
from utils import get_taxonomy
from utils import get_usda_codes

# Define directories
drive = Path('C:/')
root_folder = drive / 'ACCS_Work'

# Define folders
project_folder = root_folder / 'OneDrive - University of Alaska' / 'ACCS_Teams' / 'Vegetation' / 'AKVEG_Database' / 'Data'
plot_folder = project_folder / 'Data_Plots' / '44_aim_various_2023'
template_folder = project_folder / 'Data_Entry'

# Define inputs
vegcover_input = plot_folder / 'working' / '44_aim_2023_veg_export.csv'
visit_input = plot_folder / '03_sitevisit_aimvarious2023.csv'
template_input = template_folder / '05_vegetation_cover.xlsx'

# Define output
vegcover_output = plot_folder / '05_vegetationcover_aimvarious2023.csv'

# Read in data
lazy_veg = pl.scan_csv(vegcover_input)
visit_original = pl.read_csv(visit_input, columns=["site_code", "site_visit_code"])
template = pl.read_excel(template_input)

# Obtain taxonomy checklist from the AKVEG Database
taxonomy_checklist = get_taxonomy()

# Load and format vegetation cover data
vegcover = (
    lazy_veg
    .select(
        pl.col(["EvaluationID", "LineLength", "LineNumber", "PointNbr", "ChkboxTop"]),
        pl.col("^ChkboxLower.*$"),  # Use regex to select multiple columns
        pl.col("ChkboxBasal"),
        pl.col("TopCanopy"),
        pl.col("^Lower.*$"),
        pl.col("codebasal")
    )
    # Format site code
    .with_columns(pl.col("EvaluationID")
                  .str.extract(r"^(.*)_")
                  .alias("site_code"))
    # Append site visit code using right join to drop any plots that were excluded from site visit table
    .join(visit_original.lazy(), on="site_code", how="right")
    # Create a sequential row number for each site visit
    ## Solution from Ritchie Vink: https://github.com/pola-rs/polars/issues/2542
    .sort(by=["site_visit_code", "LineNumber", "PointNbr"])
    .with_columns(pl.first()
                  .cum_count()
                  .alias("point_number")
                  .over("site_visit_code")
                  .flatten())

    .collect()
)

# Explore data

## Ensure no nulls
print(vegcover["site_code", "site_visit_code"].null_count())

## Ensure that all lines are the standard 25m length
print(vegcover["LineLength"].unique())

# --- Convert to long format ---

# Identify abiotic element codes (to be excluded from species list)
abiotic_elements = ["HL", "N", "DL", "NL", "WL", "W", "TH"]

# Identify groups of columns
species_cols = vegcover.select(pl.col(["TopCanopy", "^Lower.*$", "codebasal"])).columns
chkbox_cols = vegcover.select(pl.col("^Chkbox.*$")).columns  # Indicate dead status
id_cols = ["site_visit_code", "point_number"]

# Melt species codes columns
species_long = (
    vegcover.lazy()
    .unpivot(
        on=species_cols,
        index=id_cols,
        variable_name="strata",
        value_name="usda_code",
    )
    .filter(pl.col("usda_code").is_not_null()
            .and_(~pl.col("usda_code").is_in(abiotic_elements))
              )
    .collect()
)

# Melt dead status columns
dead_long = (
    vegcover.lazy()
    .unpivot(
        on=chkbox_cols,
        index=id_cols,
        variable_name="strata",
        value_name="dead_status",
    )
    # Convert Live and Dead codes to Boolean
    .with_columns(pl.col("dead_status")
                  .str.replace_many(["D", "L"], ["TRUE", "FALSE"]))

    .collect()
)

# Create a common key
species_long = species_long.with_columns(
    pl.col("strata")
      .str.replace_many(["TopCanopy", "codebasal"], ["Top", "Basal"])
      .alias("strata")
)

dead_long = dead_long.with_columns(
    pl.col("strata")
      .str.strip_prefix("Chkbox")
      .alias("strata")
)

# Join tables
vegcover_long = (species_long.join(
    dead_long,
    on=id_cols + ["strata"],
    how="left" # Use left join to keep only the valid species rows
)
                  .sort(["site_visit_code", "point_number"]))

# Correct entries with null dead_status (n=8)
## Assume all entries should be live (FALSE)
vegcover_long = vegcover_long.with_columns(pl.when(pl.col("dead_status").is_null())
                                           .then(pl.lit("FALSE"))
                                           .otherwise(pl.col("dead_status"))
                                           .alias("dead_status"))

print(vegcover_long["dead_status"].value_counts())  ## Ensure no nulls

# --- Obtain accepted taxonomic names ----

# Format USDA plant codes
usda_codes = get_usda_codes()

# Translate USDA codes to accepted scientific names
vegcover_taxa = (vegcover_long.lazy()

                 # Join veg df to USDA plant codes to obtain scientific names
                 .join(usda_codes.lazy(), how="left", on="usda_code")

                 # Fill in 'taxonomic' names for unknown functional types
                 .with_columns(pl.when(pl.col("usda_code") == "AE")
                               .then(pl.lit("algae"))
                               .when(pl.col("usda_code") == "LI")
                               .then(pl.lit("lichen"))
                               .when(pl.col("usda_code") == "PF")
                               .then(pl.lit("forb"))
                               .otherwise(pl.col("name_original"))

                               .alias("name_original")
                               )

                # Join with AKVEG checklist to obtain accepted names
                .join(taxonomy_checklist.lazy(), how="left", left_on="name_original", right_on="taxon_name")

                 # Manually correct name original with no matches in AKVEG
                 .with_columns(pl.when(pl.col("name_original") == "Cephalozia loitlesbergeri")
                               .then(pl.lit("Cephalozia"))
                               .when(pl.col("name_original") == "Vaccinium oxycoccos")
                               .then(pl.lit("Oxycoccus microcarpus"))
                               .when(pl.col("name_original") == "Betula ×dugleana")
                               .then(pl.lit("Betula cf. occidentalis"))
                               .when(pl.col("name_original") == "Betula ×eastwoodiae")
                               .then(pl.lit("Betula cf. occidentalis"))
                               .when(pl.col("name_original") == "Polygonum bistorta")
                               .then(pl.lit("Bistorta plumosa"))
                               .when(pl.col("name_original") == "Dryas octopetala")
                               .then(pl.lit("Dryas ajanensis ssp. beringensis"))
                               .when(pl.col("name_original") == "Saxifraga bronchialis")
                               .then(pl.lit("Saxifraga funstonii"))
                               .when(pl.col("name_original") == "Carex pyrenaica")
                               .then(pl.lit("Carex micropoda"))
                               .otherwise(pl.col("name_adjudicated"))
                               .alias("name_adjudicated")
                               )

                 .collect()
                 )


# Explore BLM species codes that did not match with USDA codes
## One 2-letter code (HW, n=4 hits) and several codes that end in '86'. Not sure what those might be?
unmatched_codes = (vegcover_taxa
                   .filter(pl.col("name_original").is_null())
                   .unique(subset=["usda_code","name_original"])
                   .select("usda_code")
                   )

## Reconcile entries to unknown for now (n=384)
vegcover_taxa = (vegcover_taxa.with_columns(pl.when(pl.col("name_original").is_null())
                                            .then(pl.lit("unknown"))
                                            .otherwise(pl.col("name_original"))
                                            .alias("name_original"))
                 .with_columns(pl.when(pl.col("name_original") == "unknown")
                               .then(pl.lit("unknown"))
                               .otherwise(pl.col("name_adjudicated"))
                               .alias("name_adjudicated")
                               )
                 )

## Explore USDA scientific names that did not match with AKVEG Checklist
unmatched_sci_names = (vegcover_taxa
                      .filter(pl.col("name_adjudicated").is_null())
                      .unique(subset="name_original")
                      .select("name_original")
                      )  ## All names have been corrected

# --- Calculate percent cover ---

# Define grouping columns
## group_columns_points is used to count the number of unique species observed at each point number
## group_columns_plots is used to summarize the total number of hits per species per plot/site visit
group_columns_points = [
    "site_visit_code",
    "point_number",
    "name_original",
    "name_adjudicated",
    "dead_status"
]

group_columns_plots = [
    "site_visit_code",
    "name_original",
    "name_adjudicated",
    "dead_status"
]

# Calculate number of points per plot
# Plots should have 150 points (3 transects * 50 points per transects), but a handful have fewer
number_of_points = (vegcover_taxa
                    .group_by("site_visit_code")
                    .agg(pl.col("point_number")
                         .max())
                    .rename({"point_number":"max_hits"})
                    )

# Calculate cover percent for each species and site visit
vegcover_final = (vegcover_taxa
                    .lazy()
                    ## Get list of unique species per point
                    .unique(subset=group_columns_points)

                    ## Create constant column with value of 1 to calculate number of times the species was observed
                    # across all points
                    .with_columns(pl.lit(1).alias("observation_marker"))

                    # Calculate total number of hits per species per site visit
                    .group_by(group_columns_plots).agg(pl.col("observation_marker").sum())

                    # Get maximum number of points per plot
                    .join(number_of_points.lazy(), how="left", on="site_visit_code")

                    # Calculate percent cover
                    .with_columns((pl.col("observation_marker") / pl.col("max_hits") * 100)
                                  .round(3)
                                  .alias("cover_percent"))

                    # Populate remaining columns
                    .with_columns(pl.lit("absolute foliar cover").alias("cover_type"))

                  # Sort and select columns
                  .sort(["site_visit_code", "name_original"])
                  .select(template.columns)

                  .collect()
                  )

# QC
print(vegcover_final.describe())  ## Ensure no null values, range of % cover between 0-100%

# Are the correct number of sites included?
set_cover = set(vegcover_final.get_column("site_visit_code").unique().to_list())
set_visit = set(visit_original.get_column("site_visit_code").unique().to_list())
print(set_cover == set_visit)

# Export data
vegcover_final.write_csv(vegcover_output)
