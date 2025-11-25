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

# LOAD REFERENCE DATA INTO DUCKDB '/home/joel/data/tab20_zcta520_county20_natl.txt'

zcta_to_county <- data.table(
  dbGetQuery(
    congref,
    "select * from read_csv_auto(
     './data/tab20_zcta520_county20_natl.txt',
     header=True, normalize_names=True
     );"
  )
)

## write to georeference duckdb

dbWriteTable(congref, "zcta_to_county", zcta_to_county, overwrite = TRUE)

# trim file

zcta_to_county <- zcta_to_county[
  !is.na(geoid_zcta5_20) & !is.na(geoid_county_20),
  .(
    geoid_zcta5_20,
    arealand_zcta5_20,
    geoid_county_20, 
    arealand_county_20, 
    arealand_part
  )
]

## COMPUTE SCORE FOR PRIORITIZING ZCTA TO COUNTY ASSIGNMENT;
## THE GOAL IS TO HAVE A UNIQUE ASSIGNMENT OF COUNTY FOR EACH ZCTA
## THE SCORE IS BASED ON THE OVERLAP BETWEEN THE ZCTA AND COUNTY
## 2 PARTS: WHAT SHARE OF THE ZCTA IS IN THE COUNTY, AND WHAT SHARE
## OF THE COUNTY IS COMES FROM THE ZCTA; THESE TWO SCORES ARE MULTIPLIED
## AND WE ASSIGN THE MAX SCORE COUNTY TO THE ZCTA

zcta_to_county[, area_share_pct := arealand_part / sum(arealand_part), by = geoid_zcta5_20]

zcta_to_county[, area_county_pct := arealand_part / arealand_county_20]

zcta_to_county[, score := area_share_pct * area_county_pct]

# create a ranking of score within groups defined by ZCTA

zcta_to_county <- zcta_to_county[, rank := frank(-score), by = geoid_zcta5_20][order(geoid_zcta5_20, rank)]

# select only records with rank 1
# this will assure that a ZCTA is assigned to only one county AND state
# we will rollup to state, and then US

zcta_to_county <- zcta_to_county[rank == 1]

# select only relevant columns

zcta_to_county <- zcta_to_county[, .(geoid_zcta5_20, geoid_county_20)]

zcta_to_state <-  unique(zcta_to_county[, .(geoid_zcta5_20, state_fips = substr(geoid_county_20, 1, 2))])


## RETRIEVE STATE NAMES FROM TIGRIS
states <- data.table(tigris::states())
zcta_to_state <- merge(zcta_to_state, states[, .(GEOID, NAME)], by.x = "state_fips", by.y = "GEOID", all.x = TRUE)

dbWriteTable(congref, "zcta_to_state", zcta_to_state, overwrite = TRUE)

### THIS IS IS A 1:1 ZCTA TO STATE CROSSWALK ; WE WILL USE THIS TO ROLLUP 
### FOR BENCHMARKS OF ZCTAS COMPARED TO PARENT STATE, AND THE US

## get hu_state, hu_zcta, 

hu_zcta <- dbGetQuery(conh, "select * from hu_zcta")
hu_state <- dbGetQuery(conh, "select * from hu_state")
hu_us <- dbGetQuery(conh, "select * from hu_us")

zcta_to_state <- dbGetQuery(congref, "select * from zcta_to_state")

hu_zcta_state <- merge(hu_zcta, zcta_to_state, by.x = "zcta_20", by.y = "geoid_zcta5_20", all.x = TRUE)

hu_zcta_state <- as.data.table(
  merge(hu_zcta_state, hu_state, by.x = "state_fips", by.y = "state_code", all.x = TRUE)
)

*** RELATIVE INDEX COMPUTATIONS  -- STATE LEVEL

hu_zcta_state[, idx_20_24_st := (hgi_20_24.x - 1) / (hgi_20_24.y - 1) * 100]
hu_zcta_state[, idx_20_25_st := (hgi_20_25.x - 1) / (hgi_20_25.y - 1) * 100]
hu_zcta_state[, idx_25_st := (agr_25.x) / (agr_25.y) * 100]

## TRIM FILE

hu_zcta_state <- hu_zcta_state[, .(
  zcta_20,
  idx_20_24_st,
  idx_20_25_st,
  idx_25_st
)]


*** RELATIVE INDEX COMPUTATIONS  -- US LEVEL

hu_zcta_us <- as.data.table(
  merge(hu_zcta, hu_us, by = NULL, all = TRUE)
)

hu_zcta_us[, idx_20_24_us := (hgi_20_24.x - 1) / (hgi_20_24.y - 1) * 100]
hu_zcta_us[, idx_20_25_us := (hgi_20_25.x - 1) / (hgi_20_25.y - 1) * 100]
hu_zcta_us[, idx_25_us := (agr_25.x) / (agr_25.y) * 100]

# compute percentile column for each .x column in lines 119-121, 
# where the highest percentiles are the highest values
hu_zcta_us[, pctl_20_24_us := as.integer(
  ceiling(frank(hgi_20_24.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

hu_zcta_us[, pctl_20_25_us := as.integer(
  ceiling(frank(hgi_20_25.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
)]

hu_zcta_us[, pctl_25_us := as.integer(
  ceiling(frank(agr_25.x, ties.method = "min", na.last = "keep") / .N * 100)
)]


hu_zcta_us <- hu_zcta_us[, .(
  zcta_20,
  idx_20_24_us,
  idx_20_25_us,
  idx_25_us,
  pctl_20_24_us,
  pctl_20_25_us,
  pctl_25_us
)]

## MERGE STATE AND US ROLLUPS INTO hu_zcta

hu_zcta_indexes <- as.data.table(
  merge(hu_zcta_state, hu_zcta_us, by = "zcta_20", all.x = TRUE)
)

dbWriteTable(conh, "hu_zcta_indexes", hu_zcta_indexes, overwrite = TRUE)


trajectory_up <- hu_zcta_indexes[idx_20_24_us < 100 & idx_25_us > 100, ]
trajectory_down <- hu_zcta_indexes[idx_20_24_us > 100 & idx_25_us < 100, ]