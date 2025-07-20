# Changelog
All notable changes to the AKVEG Database will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and uses a custom versioning system 
(explained at the end of this document). 

## [2.1.0] - 2025-07-19

### Added
- **nrcs_soils_2024** [private]: Replaces
  nrcs_soils_2022.
- **abr_arcticrefuge_2019**: Add Environment, Soil Metrics, and Soil Horizons. 
- **accs_ribdon_2019**: Add Abiotic Top Cover and Ground Cover.
- Add Whole Tussock Cover and Structural Group Cover for several ABR projects:
  - abr_meltwater_2000
  - abr_drillsite3s_2001
  - abr_npra_2003
  - abr_kuparuk_2006
  - abr_milnepoint_2008
  - abr_nuna_2010
  - abr_news_2011 
  - abr_susitna_2013 
  - abr_cd5_2016 
  - abr_willow_2018 
  - abr_stonyhill_2018 
  - abr_colville_1996 
  - abr_tarn_1997 
  - abr_colville_1998

### Removed
- **nrcs_soils_2022**: Replaced by nrcs_soils_2024.

### Fixed
- Adjudicate all entries with *Salix planifolia* as name_original to *Salix pulchra* (#13).

## Versioning System
We use a MAJOR.MINOR.PATCH versioning system where we increment the:
1. MAJOR version when we change the database schema 
2. MINOR version for all other changes, including adding, removing, or modifying datasets. 
3. PATCH version is not currently used, but we include it here to mirror the format that is used in software 
   development.

We began this change log on 2025-07-19. At that date, we were on schema version 2.0. For simplicity's sake, we started 
our 
minor version 
numbering at 1, though there were several changes to the database between the release of schema 2.0 and the updates 
on 2025-07-19.