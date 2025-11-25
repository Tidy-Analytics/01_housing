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

# load counties
hu_county <- as.data.table(dbGetQuery(conh, "select * from hu_county"))

## get indexes for baseline geographies

hu_cbsa <- as.data.table(dbGetQuery(conh, "select * from hu_cbsa"))
hu_state <- as.data.table(dbGetQuery(conh, "select * from hu_state"))
hu_us <- as.data.table(dbGetQuery(conh, "select * from hu_us"))

#################################### COUNTY LEVEL TRACT ROLLUPS ############

### COUNTY TO CBSA

county_to_cbsa <- as.data.table(dbGetQuery(congref, "select * from county_to_cbsa"))

hu_county_cbsa <- merge(hu_county, county_to_cbsa, by.x = "co_fips", by.y = "county", all.x = TRUE)

hu_county_cbsa <- as.data.table(
  merge(hu_county_cbsa, hu_cbsa, by.x = "cbsa23", by.y = "cbsa23", all.x = TRUE)
)

############### RELATIVE INDEX COMPUTATIONS  -- CBSA LEVEL

hu_county_cbsa[, idx_20_24_cbsa := (hgi_20_24.x - 1) / (hgi_20_24.y - 1) * 100]
hu_county_cbsa[, idx_20_25_cbsa := (hgi_20_25.x - 1) / (hgi_20_25.y - 1) * 100]
hu_county_cbsa[, idx_25_cbsa := (agr_25.x) / (agr_25.y) * 100]

## NULL CBSA ARE CONNECTICUT BLOCK GROUPS; CBSA DEFINITIONS BASED ON NEW PLANNING REGION
## COMPONENTS ARE NOT YET DEFINED; WE ARE USING NEW PLANNING REGION IDS IN OUR BLOCK GROUP
## IDENTIFIERS; WE WILL NEED A PLANNING REGION TO CBSA CROSSWALK TO HANDLE THESE; THESE ROWS 
## WOULD REPLACE COUNTY-BASED DEFINITIONS

hu_county_cbsa <- hu_county_cbsa[!is.na(co_fips), ]

hu_county_cbsa[is.na(cbsa23), cbsa23 := "CT000"]

## TRIM FILE

hu_county_cbsa <- hu_county_cbsa[, .(
  co_fips,
  cbsa23,
  idx_20_24_cbsa,
  idx_20_25_cbsa,
  idx_25_cbsa
)]


#### COUNTY TO STATE ################################

hu_county[, state_fips := substr(co_fips, 1, 2)]

hu_county_state <- as.data.table(
  merge(hu_county, hu_state, by.x = "state_fips", by.y = "state_code", all.x = TRUE)
)

############### RELATIVE INDEX COMPUTATIONS  -- STATE LEVEL

hu_county_state[, idx_20_24_state := (hgi_20_24.x - 1) / (hgi_20_24.y - 1) * 100]
hu_county_state[, idx_20_25_state := (hgi_20_25.x - 1) / (hgi_20_25.y - 1) * 100]
hu_county_state[, idx_25_state := (agr_25.x) / (agr_25.y) * 100]

hu_county_state <- hu_county_state[!is.na(co_fips), ]

## TRIM FILE

hu_county_state <- hu_county_state[, .(
  co_fips,
  state_fips,
  idx_20_24_state,
  idx_20_25_state,
  idx_25_state
)]

########### COUNTY TO US #############################################

hu_county[, matchid := 1]
hu_us[, matchid := 1]

hu_county_us <- as.data.table(
  merge(hu_county, hu_us, by.x = "matchid",  by.y = "matchid", all = TRUE)
)

############# RELATIVE INDEX COMPUTATIONS  -- US LEVEL

hu_county_us[, idx_20_24_us := (hgi_20_24.x - 1) / (hgi_20_24.y - 1) * 100]
hu_county_us[, idx_20_25_us := (hgi_20_25.x - 1) / (hgi_20_25.y - 1) * 100]
hu_county_us[, idx_25_us := (agr_25.x) / (agr_25.y) * 100]


### FINALLY : COMPUTE NATIONAL PERCENTILES FOR BLOCK GROUPS

# compute percentile column for each .x column in lines 119-121, 
# where the highest percentiles are the highest values
hu_county_us[, pctl_20_24_us := as.integer(
  ceiling(frank(hgi_20_24.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

hu_county_us[, pctl_20_25_us := as.integer(
  ceiling(frank(hgi_20_25.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

hu_county_us[, pctl_25_us := as.integer(
  ceiling(frank(agr_25.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

hu_county_us <- hu_county_us[, .(
  co_fips,
  idx_20_24_us,
  idx_20_25_us,
  idx_25_us,
  pctl_20_24_us,
  pctl_20_25_us,
  pctl_25_us
)]


### COMPILE INDEX SETS AND WRITE TO DUCKDB

hu_county_indexes <- Reduce(fu                                                                                                                                                                                                  nction(x, y) merge(x, y, by = "co_fips", all = TRUE), 
                                 list(hu_county_cbsa, hu_county_state, hu_county_us)
)

hu_county_indexes <- hu_county_indexes[!is.na(co_fips), ]

dbWriteTable(conh, "hu_county_indexes", hu_county_indexes, overwrite = TRUE)

#### COUNTY INDEXES COMPLETE ###############################

bg_all_data <- as.data.table(
  dbGetQuery(
    conh, "select a.*, b.* from hu_block_group a JOIN hu_block_group_indexes b ON a.block_group = b.block_group"
    )
)