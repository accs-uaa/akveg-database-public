# Alaska Vegetation Plots Database
Alaska Vegetation Plots Database (AKVEG) scripts to query public data stored in the AKVEG Database.

## Getting Started

These instructions will enable you to query public data in the Alaska Vegetation Plots Database (AKVEG). The AKVEG Database is built in PostgreSQL and hosted on a cloud server that can be queried in numerous ways. This repository provides example scripts to query the database in Python and R.

To query the database, you will need server credentials. Please contact [Amanda Droghini](mailto:adroghini@alaska.edu), the data manager for the AKVEG Database, to request server credentials. The database is public and access is free; the purpose of the server credentials is to prevent excessive loads on the server and to maintain knowledge of how many people are connecting.

### Prerequisites
1. Python 3.9+
   1. psycopg2
   2. pandas
   3. geopandas
   4. os
   5. [akutils](https://github.com/accs-uaa/akutils)

2. R 4.0.0+
   1. dplyr
   2. fs
   3. janitor
   4. lubridate
   5. readr
   6. readxl
   7. RPostgres
   8. sf
   9. stringr
   10. terra
   11. tibble
   12. tidyr


### Preparing credentials

Once you have received server credentials from the data manager, you will need to unzip the credentials in a location accessible to your script environment. The credentials include a csv file. In that csv file, you will need to update the file paths for the following rows: 9 (sslrootcert), 10 (sslcert), and 11 (sslkey).

### Preparing files for example scripts

The example scripts use three input files that are not included in this repository. When requesting credentials from the data manager, we will provide the input files as well.

- domain_input ('region_data/AlaskaYukon_MapDomain_3338.shp'): A shapefile containing the spatial domain of the AKVEG Map, which is the focal area of data integration and taxonomy for the AKVEG Database.
- zone_input ()'region_data/AlaskaYukon_VegetationZones_v1.1_3338.shp'): A shapefile containing features for major biomes and vegetation zones of Alaska & Yukon.
- fireyear_input ('ancillary_data/AlaskaYukon_FireYear_10m_3338.tif'): A 10 m raster dataset of most recent burn year from 1940 to 2023 where zero indicates that no burn was documented during that time frame.

These files are provided for the purpose of providing examples for how to relate the data in the AKVEG Database to other geospatial data. These data are not required to use or access data in the AKVEG Database. To run the example scripts with the example data included, the example data must be unzipped to a location accessible to your script environment. 

## Credits

### Built With
* PostgreSQL 17
* DataGrip 2020.2.1
* R 4.0.2
* RStudio 1.3.1073
* Python 3.12
* PyCharm

### Authors

* **Timm Nawrocki** - *Alaska Center for Conservation Science, University of Alaska Anchorage*
* **Amanda Droghini** - *Alaska Center for Conservation Science, University of Alaska Anchorage*

### Support

The U.S. Fish & Wildlife Service, Bureau of Land Management, National Park Service, U.S. Forest Service, Alaska Department of Fish & Game, and Alaska Department of Natural Resources provided funding in support of the development of the AKVEG Database.

### Usage Requirements

Citing the database is not required to use the data as all data are public. Where a citation is sensible, we would appreciate you citing the AKVEG Database as follows:

*Citation is forthcoming in the future. For now, please contact the data manager for information on citing the AKVEG Database.*

### License

This project is provided under the GNU General Public License v3.0. It is free to use and modify in part or in whole.
