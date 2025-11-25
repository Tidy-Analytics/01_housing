## ZCTA TO STATE; 
## CORE ADAPTED FROM 'zcta-to-county.r'

library(duckdb)
library(data.table)
library(DBI)
library(dplyr)
#install.packages("reactable")
library(reactable)
library(plotly)
library(classInt)
library(RColorBrewer)
library(dotenv)
library(sf)

setwd("/home/joel")

dotenv::load_dot_env()

### DUCKDB CONNECTION

conh <- dbConnect(duckdb('./data/housing.duckdb'))
dbListTables(conh)

conmeta <- dbConnect(duckdb('./data/duckdb_metadata.duckdb'))
dbListTables(conmeta)

congref <- dbConnect(duckdb('./data/georeference.duckdb'))
dbListTables(congref)

congeo <- dbConnect(
  duckdb::duckdb(), 
  dbdir = "./data/spatial_storage.duckdb",
  read_only = FALSE,
  extensions = c("spatial")
)

metadata <- dbGetQuery(conmeta, "select * from column_metadata")
tables <- dbGetQuery(conmeta, "select * from table_summary")

# county to metro area crosswalk

county_to_cbsa <- dbGetQuery(congref, "select * from county_to_cbsa")

## BASE GEOGRAPHY

hu_cbsa <- as.data.table(dbGetQuery(conh, "select * from hu_cbsa"))

## get comparison geographies

hu_us <- as.data.table(dbGetQuery(conh, "select * from hu_us"))


################################## STATE LEVEL COUSUB ROLLUP #############

hu_cbsa[, matchid := 1]
hu_us[, matchid := 1]

hu_cbsa_us <- as.data.table(
  merge(hu_cbsa, hu_us, by = "matchid", all.x = TRUE)
)

############### RELATIVE INDEX COMPUTATIONS  -- STATE LEVEL

hu_cbsa_us[, idx_20_24_us := (hgi_20_24.x - 1) / (hgi_20_24.y - 1) * 100]
hu_cbsa_us[, idx_20_25_us := (hgi_20_25.x - 1) / (hgi_20_25.y - 1) * 100]
hu_cbsa_us[, idx_25_us := (agr_25.x) / (agr_25.y) * 100]

hu_cbsa_us <- hu_cbsa_us[!is.na(cbsa23), ]


### COMPUTE NATIONAL PERCENTILES FOR CBSAS

# compute percentile column for each .x column in lines 119-121, 
# where the highest percentiles are the highest values
hu_cbsa_us[, pctl_20_24_us := as.integer(
  ceiling(frank(hgi_20_24.x, ties.method = "min", na.last = "keep") / .N * 100)
)]


hu_cbsa_us[, pctl_20_25_us := as.integer(
  ceiling(frank(hgi_20_25.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

hu_cbsa_us[, pctl_25_us := as.integer(
  ceiling(frank(agr_25.x, ties.method = "min", na.last = "keep") / .N * 100)
)]


hu_cbsa_indexes <- hu_cbsa_us[, .(
  cbsa23,
  idx_20_24_us,
  idx_20_25_us,
  idx_25_us,
  pctl_20_24_us,
  pctl_20_25_us,
  pctl_25_us
)]


### WRITE TO DUCKDB

hu_cbsa_indexes <- hu_cbsa_indexes[!is.na(cbsa23), ]

dbWriteTable(conh, "hu_cbsa_indexes", hu_cbsa_indexes, overwrite = TRUE)

dbListTables(conh)


#### PLACE INDEXES COMPLETE ###############################


#######################################

cbsa_names <- as.data.table(dbGetQuery(congeo, "select * from geo_cbsa_23"))
cbsa_hu <- as.data.table(dbGetQuery(conh, "select * from hu_cbsa"))
cbsa_indexes <- as.data.table(dbGetQuery(conh, "select * from hu_cbsa_indexes"))

cbsa_hu <- merge(
  cbsa_names[, .(CBSAFP, NAMELSAD)],
  cbsa_hu,
  by.x = c("CBSAFP"),
  by.y = c("cbsa23"),
  all.x = TRUE
)

cbsa_indexes <- merge(
  cbsa_hu,
  cbsa_indexes,
  by.x = c("CBSAFP"),
  by.y = c("cbsa23"),
  all.x = TRUE
)

cbsa_indexes <- cbsa_indexes[order(HU_20, decreasing = TRUE), ]
cbsa_indexes <- cbsa_indexes[!is.na(HU_20), ]


