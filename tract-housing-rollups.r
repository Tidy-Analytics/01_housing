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

metadata <- dbGetQuery(conmeta, "select * from column_metadata")
tables <- dbGetQuery(conmeta, "select * from table_summary")

# load tracts and cbsa reference
hu_tract <- as.data.table(dbGetQuery(conh, "select * from hu_tract"))

## get indexes for baseline geographies

hu_county <- as.data.table(dbGetQuery(conh, "select * from hu_county"))
hu_cbsa <- as.data.table(dbGetQuery(conh, "select * from hu_cbsa"))
hu_state <- as.data.table(dbGetQuery(conh, "select * from hu_state"))
hu_us <- as.data.table(dbGetQuery(conh, "select * from hu_us"))

#################################### COUNTY LEVEL TRACT ROLLUP #############

county_to_cbsa <- as.data.table(dbGetQuery(congref, "select * from county_to_cbsa"))

## compute county fips
hu_tract[, county_fips := substr(tract, 1, 5)]

hu_tract_cbsa <- merge(hu_tract, county_to_cbsa, by.x = "county_fips", by.y = "county", all.x = TRUE)

hu_tract_cbsa <- as.data.table(
  merge(hu_tract_cbsa, hu_cbsa, by.x = "cbsa23", by.y = "cbsa23", all.x = TRUE)
)

############### RELATIVE INDEX COMPUTATIONS  -- CBSA LEVEL

hu_tract_cbsa[, idx_20_24_cbsa := (hgi_20_24.x - 1) / (hgi_20_24.y - 1) * 100]
hu_tract_cbsa[, idx_20_25_cbsa := (hgi_20_25.x - 1) / (hgi_20_25.y - 1) * 100]
hu_tract_cbsa[, idx_25_cbsa := (agr_25.x) / (agr_25.y) * 100]

## NULL CBSA ARE CONNECTICUT BLOCK GROUPS; CBSA DEFINITIONS BASED ON NEW PLANNING REGION
## COMPONENTS ARE NOT YET DEFINED; WE ARE USING NEW PLANNING REGION IDS IN OUR BLOCK GROUP
## IDENTIFIERS; WE WILL NEED A PLANNING REGION TO CBSA CROSSWALK TO HANDLE THESE; THESE ROWS 
## WOULD REPLACE COUNTY-BASED DEFINITIONS

hu_tract_cbsa <- hu_tract_cbsa[!is.na(tract), ]

hu_tract_cbsa[is.na(cbsa23), cbsa23 := "CT000"]

## TRIM FILE

hu_tract_cbsa <- hu_tract_cbsa[, .(
  tract,
  cbsa23,
  idx_20_24_cbsa,
  idx_20_25_cbsa,
  idx_25_cbsa
)]

#################################### COUNTY LEVEL TRACT ROLLUP #############

hu_tract_county <- as.data.table(
  merge(hu_tract, hu_county, by.x = "county_fips", by.y = "co_fips", all.x = TRUE)
)

################# RELATIVE INDEX COMPUTATIONS  -- COUNTY LEVEL

hu_tract_county[, idx_20_24_county := (hgi_20_24.x - 1) / (hgi_20_24.y - 1) * 100]
hu_tract_county[, idx_20_25_county := (hgi_20_25.x - 1) / (hgi_20_25.y - 1) * 100]
hu_tract_county[, idx_25_county := (agr_25.x) / (agr_25.y) * 100]

hu_tract_county <- hu_tract_county[!is.na(tract), ]

## TRIM FILE

hu_tract_county <- hu_tract_county[, .(
  tract,
  county_fips,
  idx_20_24_county,
  idx_20_25_county,
  idx_25_county
)]

################################## STATE LEVEL TRACT ROLLUP #############

hu_tract[, state_fips := substr(tract, 1, 2)]

hu_tract_state <- as.data.table(
  merge(hu_tract, hu_state, by.x = "state_fips", by.y = "state_code", all.x = TRUE)
)

############### RELATIVE INDEX COMPUTATIONS  -- STATE LEVEL

hu_tract_state[, idx_20_24_state := (hgi_20_24.x - 1) / (hgi_20_24.y - 1) * 100]
hu_tract_state[, idx_20_25_state := (hgi_20_25.x - 1) / (hgi_20_25.y - 1) * 100]
hu_tract_state[, idx_25_state := (agr_25.x) / (agr_25.y) * 100]

hu_tract_state <- hu_tract_state[!is.na(tract), ]

## TRIM FILE

hu_tract_state <- hu_tract_state[, .(
  tract,
  state_fips,
  idx_20_24_state,
  idx_20_25_state,
  idx_25_state
)]

################################## US LEVEL TRACT ROLLUP #############

hu_tract[, matchid := 1]
hu_us[, matchid := 1]

hu_tract_us <- as.data.table(
  merge(hu_tract, hu_us, by.x = "matchid",  by.y = "matchid", all = TRUE)
)

############# RELATIVE INDEX COMPUTATIONS  -- US LEVEL

hu_tract_us[, idx_20_24_us := (hgi_20_24.x - 1) / (hgi_20_24.y - 1) * 100]
hu_tract_us[, idx_20_25_us := (hgi_20_25.x - 1) / (hgi_20_25.y - 1) * 100]
hu_tract_us[, idx_25_us := (agr_25.x) / (agr_25.y) * 100]


### FINALLY : COMPUTE NATIONAL PERCENTILES FOR BLOCK GROUPS

# compute percentile column for each .x column in lines 119-121, 
# where the highest percentiles are the highest values
hu_tract_us[, pctl_20_24_us := as.integer(
  ceiling(frank(hgi_20_24.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

hu_tract_us[, pctl_20_25_us := as.integer(
  ceiling(frank(hgi_20_25.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

hu_tract_us[, pctl_25_us := as.integer(
  ceiling(frank(agr_25.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

hu_tract_us <- hu_tract_us[, .(
  tract,
  idx_20_24_us,
  idx_20_25_us,
  idx_25_us,
  pctl_20_24_us,
  pctl_20_25_us,
  pctl_25_us
)]


### WRITE TO DUCKDB

hu_tract_indexes <- Reduce(function(x, y) merge(x, y, by = "tract", all = TRUE), 
                                 list(hu_tract_cbsa, hu_tract_county, hu_tract_state, hu_tract_us)
)

hu_tract_indexes <- hu_tract_indexes[!is.na(tract), ]

dbWriteTable(conh, "hu_tract_indexes", hu_tract_indexes, overwrite = TRUE)

#### TRACT INDEXES COMPLETE ###############################
