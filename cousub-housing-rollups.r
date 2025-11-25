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

hu_cousub <- as.data.table(dbGetQuery(conh, "select * from hu_cousub"))

## get comparison geographies

hu_state <- as.data.table(dbGetQuery(conh, "select * from hu_state"))
hu_us <- as.data.table(dbGetQuery(conh, "select * from hu_us"))


################################## STATE LEVEL COUSUB ROLLUP #############

hu_cousub_state <- as.data.table(
  merge(hu_cousub, hu_state, by = "state_code", all.x = TRUE)
)

############### RELATIVE INDEX COMPUTATIONS  -- STATE LEVEL

hu_cousub_state[, idx_20_24_state := (hgi_20_24.x - 1) / (hgi_20_24.y - 1) * 100]
hu_cousub_state[, idx_20_25_state := (hgi_20_25.x - 1) / (hgi_20_25.y - 1) * 100]
hu_cousub_state[, idx_25_state := (agr_25.x) / (agr_25.y) * 100]

hu_cousub_state <- hu_cousub_state[!is.na(cousub), ]

## TRIM FILE

hu_cousub_state <- hu_cousub_state[, .(
  cousub,
  state_code,
  idx_20_24_state,
  idx_20_25_state,
  idx_25_state
)]

################################## US LEVEL BG ROLLUP #############

hu_cousub[, matchid := 1]
hu_us[, matchid := 1]

hu_cousub_us <- as.data.table(
  merge(hu_cousub, hu_us, by.x = "matchid",  by.y = "matchid", all = TRUE)
)

############# RELATIVE INDEX COMPUTATIONS  -- US LEVEL

hu_cousub_us[, idx_20_24_us := (hgi_20_24.x - 1) / (hgi_20_24.y - 1) * 100]
hu_cousub_us[, idx_20_25_us := (hgi_20_25.x - 1) / (hgi_20_25.y - 1) * 100]
hu_cousub_us[, idx_25_us := (agr_25.x) / (agr_25.y) * 100]


### FINALLY : COMPUTE NATIONAL PERCENTILES FOR BLOCK GROUPS

# compute percentile column for each .x column in lines 119-121, 
# where the highest percentiles are the highest values
hu_cousub_us[, pctl_20_24_us := as.integer(
  ceiling(frank(hgi_20_24.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

hu_cousub_us[, pctl_20_25_us := as.integer(
  ceiling(frank(hgi_20_25.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

hu_cousub_us[, pctl_25_us := as.integer(
  ceiling(frank(agr_25.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

hu_cousub_us <- hu_cousub_us[, .(
  cousub,
  state_code,
  idx_20_24_us,
  idx_20_25_us,
  idx_25_us,
  pctl_20_24_us,
  pctl_20_25_us,
  pctl_25_us
)]


### WRITE TO DUCKDB

hu_cousub_indexes <- Reduce(function(x, y) merge(x, y, by = c("cousub", "state_code"), all = TRUE), 
                                 list(hu_cousub_state, hu_cousub_us)
)

hu_cousub_indexes <- hu_cousub_indexes[!is.na(cousub), ]

dbWriteTable(conh, "hu_cousub_indexes", hu_cousub_indexes, overwrite = TRUE)

#### BLOCK GROUP INDEXES COMPLETE ###############################

cousub_names <- as.data.table(dbGetQuery(congeo, "select * from geo_cosub_23"))

hu_cousub_indexes <- merge(
  hu_cousub_indexes,
  cousub_names[, .(COUSUBFP, STATEFP, NAMELSAD, NAMELSADCO, STUSPS)],
  by.x = c("cousub", "state_code"),
  by.y = c("COUSUBFP", "STATEFP"),
  all.x = TRUE
)