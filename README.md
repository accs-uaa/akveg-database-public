# Alaska Vegetation Plots Database
Alaska Vegetation Plots Database (AKVEG) scripts to compile database from quality controlled source data and enable database replication.

## Getting Started

These instructions will enable you to load a new instance or updated instance of the Alaska Vegetation Plots Database (AKVEG) into a PostgreSQL 12.4+ server. The database was designed for PostgreSQL. Instructions are not provided for loading the database into other SQL platforms; however, advanced users can modify the replication scripts to create the database in other SQL platforms as well. The instructions included in this readme file will also enable you to construct an empty vegetation plots database for management of other data.

### Prerequisites
1. PostgreSQL 12.4+
2. R 4.0.0+

### Installing Database

Install a PostgreSQL 12.4+ server either on localhost or at a remote location that you control. See documentation from database software for installation and set up help.

To build a replicate copy of AKVEG, run all scripts in the **local_replication** folder in the numerical order associated with the script names in a SQL interpreter configured to access your database server. The resulting vegetation database will be identical to the version available online at [https://akveg.uaa.alaska.edu](https://akveg.uaa.alaska.edu). To build an empty version of AKVEG (the database structure with no data loaded into it) run the **Build** scripts located in the **structure_metadata** folder in the numeric order associated with the script names. Building an empty database will enable population with new data. To process new data into the database, use the R scripts in the **data_preparation** folder to create SQL Insert statements from quality controlled source data.

## Usage

AKVEG can be interacted with via SQL queries in a SQL interpreter, through Python or R with PostgreSQL connection packages, or through ArcGIS Pro facilitated by Python. The database schema and data dictionary show users the structure of the database for the purpose of querying data or entering new data.

## Credits

### Built With
* PostgreSQL 12.4
* DataGrip 2020.2.1
* R 4.0.2
* RStudio 1.3.1073

### Authors

* **Timm Nawrocki** - *Alaska Center for Conservation Science, University of Alaska Anchorage*

### Support

The Bureau of Land Management, National Park Service, Alaska Department of Fish & Game, and U.S. Forest Service provided funding in support of the development of the AKVEG database to reconcile multi-format, multi-agency, multi-method vegetation survey and monitoring data.

### Usage Requirements

None. Citing the database is not required to use the data; however, you can cite the AKVEG database as:

Nawrocki, T.W. 2021. Alaska Vegetation Plots Database (AKVEG). Git Repository. Available: https://github.com/accs-uaa/vegetation-plots-database

We recommend citing the individual projects from which data are used.

### License

This project is provided under the GNU General Public License v3.0. It is free to use and modify in part or in whole.
