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

dbListTables(congeo)

metadata <- dbGetQuery(conmeta, "select * from column_metadata")
tables <- dbGetQuery(conmeta, "select * from table_summary")

# load block groups

hu_block_group <- dbGetQuery(conh, "select * from hu_block_group")
# match block groups to cbsa via county

county_to_cbsa <- fread("./data/county-cbsa-lookup.csv", 
                        colClasses = "character",
                        encoding = "Latin-1")

## write to georeference duckdb

dbWriteTable(congref, "county_to_cbsa", county_to_cbsa, overwrite = TRUE)

### THIS IS IS A 1:1 ZCTA TO STATE CROSSWALK ; WE WILL USE THIS TO ROLLUP 
### FOR BENCHMARKS OF ZCTAS COMPARED TO PARENT STATE, AND THE US

## get hu_state, hu_zcta, 

hu_block_group <- as.data.table(dbGetQuery(conh, "select * from hu_block_group"))
hu_county <- as.data.table(dbGetQuery(conh, "select * from hu_county"))
hu_cbsa <- as.data.table(dbGetQuery(conh, "select * from hu_cbsa"))
hu_state <- as.data.table(dbGetQuery(conh, "select * from hu_state"))
hu_us <- as.data.table(dbGetQuery(conh, "select * from hu_us"))

#################################### COUNTY LEVEL BG ROLLUP #############

county_to_cbsa <- as.data.table(dbGetQuery(congref, "select * from county_to_cbsa"))

## compute county fips
hu_block_group[, county_fips := substr(block_group, 1, 5)]

hu_block_group_cbsa <- merge(hu_block_group, county_to_cbsa, by.x = "county_fips", by.y = "county", all.x = TRUE)

hu_block_group_cbsa <- as.data.table(
  merge(hu_block_group_cbsa, hu_cbsa, by.x = "cbsa23", by.y = "cbsa23", all.x = TRUE)
)

############### RELATIVE INDEX COMPUTATIONS  -- CBSA LEVEL

hu_block_group_cbsa[, idx_20_24_cbsa := (hgi_20_24.x - 1) / (hgi_20_24.y - 1) * 100]
hu_block_group_cbsa[, idx_20_25_cbsa := (hgi_20_25.x - 1) / (hgi_20_25.y - 1) * 100]
hu_block_group_cbsa[, idx_25_cbsa := (agr_25.x) / (agr_25.y) * 100]

## NULL CBSA ARE CONNECTICUT BLOCK GROUPS; CBSA DEFINITIONS BASED ON NEW PLANNING REGION
## COMPONENTS ARE NOT YET DEFINED; WE ARE USING NEW PLANNING REGION IDS IN OUR BLOCK GROUP
## IDENTIFIERS; WE WILL NEED A PLANNING REGION TO CBSA CROSSWALK TO HANDLE THESE; THESE ROWS 
## WOULD REPLACE COUNTY-BASED DEFINITIONS

hu_block_group_cbsa <- hu_block_group_cbsa[!is.na(block_group), ]

hu_block_group_cbsa[is.na(cbsa23), cbsa23 := "CT000"]

## TRIM FILE

hu_block_group_cbsa <- hu_block_group_cbsa[, .(
  block_group,
  cbsa23,
  idx_20_24_cbsa,
  idx_20_25_cbsa,
  idx_25_cbsa
)]

#################################### COUNTY LEVEL BG ROLLUP #############

hu_block_group_county <- as.data.table(
  merge(hu_block_group, hu_county, by.x = "county_fips", by.y = "co_fips", all.x = TRUE)
)

################# RELATIVE INDEX COMPUTATIONS  -- COUNTY LEVEL

hu_block_group_county[, idx_20_24_county := (hgi_20_24.x - 1) / (hgi_20_24.y - 1) * 100]
hu_block_group_county[, idx_20_25_county := (hgi_20_25.x - 1) / (hgi_20_25.y - 1) * 100]
hu_block_group_county[, idx_25_county := (agr_25.x) / (agr_25.y) * 100]

hu_block_group_county <- hu_block_group_county[!is.na(block_group), ]

## TRIM FILE

hu_block_group_county <- hu_block_group_county[, .(
  block_group,
  county_fips,
  idx_20_24_county,
  idx_20_25_county,
  idx_25_county
)]

################################## STATE LEVEL BG ROLLUP #############

hu_block_group[, state_fips := substr(block_group, 1, 2)]

hu_block_group_state <- as.data.table(
  merge(hu_block_group, hu_state, by.x = "state_fips", by.y = "state_code", all.x = TRUE)
)

############### RELATIVE INDEX COMPUTATIONS  -- STATE LEVEL

hu_block_group_state[, idx_20_24_state := (hgi_20_24.x - 1) / (hgi_20_24.y - 1) * 100]
hu_block_group_state[, idx_20_25_state := (hgi_20_25.x - 1) / (hgi_20_25.y - 1) * 100]
hu_block_group_state[, idx_25_state := (agr_25.x) / (agr_25.y) * 100]

hu_block_group_state <- hu_block_group_state[!is.na(block_group), ]

## TRIM FILE

hu_block_group_state <- hu_block_group_state[, .(
  block_group,
  state_fips,
  idx_20_24_state,
  idx_20_25_state,
  idx_25_state
)]

################################## US LEVEL BG ROLLUP #############

hu_block_group[, matchid := 1]
hu_us[, matchid := 1]

hu_block_group_us <- as.data.table(
  merge(hu_block_group, hu_us, by.x = "matchid",  by.y = "matchid", all = TRUE)
)

############# RELATIVE INDEX COMPUTATIONS  -- US LEVEL

hu_block_group_us[, idx_20_24_us := (hgi_20_24.x - 1) / (hgi_20_24.y - 1) * 100]
hu_block_group_us[, idx_20_25_us := (hgi_20_25.x - 1) / (hgi_20_25.y - 1) * 100]
hu_block_group_us[, idx_25_us := (agr_25.x) / (agr_25.y) * 100]


### FINALLY : COMPUTE NATIONAL PERCENTILES FOR BLOCK GROUPS

# compute percentile column for each .x column in lines 119-121, 
# where the highest percentiles are the highest values
hu_block_group_us[, pctl_20_24_us := as.integer(
  ceiling(frank(hgi_20_24.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

hu_block_group_us[, pctl_20_25_us := as.integer(
  ceiling(frank(hgi_20_25.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

hu_block_group_us[, pctl_25_us := as.integer(
  ceiling(frank(agr_25.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

hu_block_group_us <- hu_block_group_us[, .(
  block_group,
  idx_20_24_us,
  idx_20_25_us,
  idx_25_us,
  pctl_20_24_us,
  pctl_20_25_us,
  pctl_25_us
)]


### WRITE TO DUCKDB

hu_block_group_indexes <- Reduce(function(x, y) merge(x, y, by = "block_group", all = TRUE), 
                                 list(hu_block_group_cbsa, hu_block_group_county, hu_block_group_state, hu_block_group_us)
)

hu_block_group_indexes <- hu_block_group_indexes[!is.na(block_group), ]

dbWriteTable(conh, "hu_block_group_indexes", hu_block_group_indexes, overwrite = TRUE)

#### BLOCK GROUP INDEXES COMPLETE ###############################

dbListTables(congeo)

block_group_names <- as.data.table(dbGetQuery(congeo, "select * from geo_block_group"))
block_group_hu <- as.data.table(dbGetQuery(conh, "select * from hu_block_group"))
block_group_indexes <- as.data.table(dbGetQuery(conh, "select * from hu_block_group_indexes"))

block_group_hu <- merge(
  block_group_names[, .(GEOID, NAMELSAD, STATEFP)],
  block_group_hu,
  by.x = c("GEOID"),
  by.y = c("block_group"),
  all.x = TRUE
)

block_group_indexes <- merge(
  block_group_hu,
  block_group_indexes,
  by.x = c("GEOID"),
  by.y = c("block_group"),
  all.x = TRUE
)

#cbsa_indexes <- cbsa_indexes[order(HU_20, decreasing = TRUE), ]
block_group_indexes <- block_group_indexes[!is.na(HU_20), ]

block_group_hu_indexes[STATEFP == '27', ]
