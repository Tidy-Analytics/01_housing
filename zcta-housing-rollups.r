## ZCTA TO GEOGRAPHIC ROLLUPS
## CORE ADAPTED FROM 'bg-housing-rollups.r'

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

############### RELATIVE INDEX COMPUTATIONS  -- STATE LEVEL

# hgi_* indexes (housing growth index)
hu_zcta_state[, idx_state_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_zcta_state[, idx_state_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_zcta_state[, idx_state_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_zcta_state[, idx_state_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_zcta_state[, idx_state_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_zcta_state[, idx_state_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_zcta_state[, idx_state_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_zcta_state[, idx_state_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_zcta_state[, idx_state_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_zcta_state[, idx_state_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_zcta_state[, idx_state_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_zcta_state[, idx_state_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]

## TRIM FILE

hu_zcta_state <- hu_zcta_state[, .(
  zcta_20,
  state_fips,
  idx_state_hgi_20_apr_24_jul,
  idx_state_hgi_24_jul_25_jul,
  idx_state_hgi_24_jul_25_nov,
  idx_state_hgi_25_jul_25_nov,
  idx_state_hgi_20_apr_25_jul,
  idx_state_hgi_20_apr_25_nov,
  idx_state_cagr_20_apr_24_jul,
  idx_state_cagr_24_jul_25_nov,
  idx_state_cagr_25_jul_25_nov,
  idx_state_cagr_20_apr_25_jul,
  idx_state_cagr_20_apr_25_nov,
  idx_state_agr_24_jul_25_jul
)]


############### RELATIVE INDEX COMPUTATIONS  -- US LEVEL

hu_zcta_us <- as.data.table(
  merge(hu_zcta, hu_us, by = NULL, all = TRUE)
)

# hgi_* indexes (housing growth index)
hu_zcta_us[, idx_us_hgi_20_apr_24_jul := (hgi_20_apr_24_jul.x - 1) / (hgi_20_apr_24_jul.y - 1) * 100]
hu_zcta_us[, idx_us_hgi_24_jul_25_jul := (hgi_24_jul_25_jul.x - 1) / (hgi_24_jul_25_jul.y - 1) * 100]
hu_zcta_us[, idx_us_hgi_24_jul_25_nov := (hgi_24_jul_25_nov.x - 1) / (hgi_24_jul_25_nov.y - 1) * 100]
hu_zcta_us[, idx_us_hgi_25_jul_25_nov := (hgi_25_jul_25_nov.x - 1) / (hgi_25_jul_25_nov.y - 1) * 100]
hu_zcta_us[, idx_us_hgi_20_apr_25_jul := (hgi_20_apr_25_jul.x - 1) / (hgi_20_apr_25_jul.y - 1) * 100]
hu_zcta_us[, idx_us_hgi_20_apr_25_nov := (hgi_20_apr_25_nov.x - 1) / (hgi_20_apr_25_nov.y - 1) * 100]

# cagr_* indexes (compound annual growth rate)
hu_zcta_us[, idx_us_cagr_20_apr_24_jul := (cagr_20_apr_24_jul.x) / (cagr_20_apr_24_jul.y) * 100]
hu_zcta_us[, idx_us_cagr_24_jul_25_nov := (cagr_24_jul_25_nov.x) / (cagr_24_jul_25_nov.y) * 100]
hu_zcta_us[, idx_us_cagr_25_jul_25_nov := (cagr_25_jul_25_nov.x) / (cagr_25_jul_25_nov.y) * 100]
hu_zcta_us[, idx_us_cagr_20_apr_25_jul := (cagr_20_apr_25_jul.x) / (cagr_20_apr_25_jul.y) * 100]
hu_zcta_us[, idx_us_cagr_20_apr_25_nov := (cagr_20_apr_25_nov.x) / (cagr_20_apr_25_nov.y) * 100]

# agr_* indexes (annual growth rate)
hu_zcta_us[, idx_us_agr_24_jul_25_jul := (agr_24_jul_25_jul.x) / (agr_24_jul_25_jul.y) * 100]


### COMPUTE NATIONAL PERCENTILES FOR ZCTAs

# Percentiles for hgi_* metrics
hu_zcta_us[, pctl_us_hgi_20_apr_24_jul := as.integer(
  ceiling(frank(hgi_20_apr_24_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_zcta_us[, pctl_us_hgi_24_jul_25_jul := as.integer(
  ceiling(frank(hgi_24_jul_25_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_zcta_us[, pctl_us_hgi_24_jul_25_nov := as.integer(
  ceiling(frank(hgi_24_jul_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_zcta_us[, pctl_us_hgi_25_jul_25_nov := as.integer(
  ceiling(frank(hgi_25_jul_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_zcta_us[, pctl_us_hgi_20_apr_25_jul := as.integer(
  ceiling(frank(hgi_20_apr_25_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_zcta_us[, pctl_us_hgi_20_apr_25_nov := as.integer(
  ceiling(frank(hgi_20_apr_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

# Percentiles for cagr_* metrics
hu_zcta_us[, pctl_us_cagr_20_apr_24_jul := as.integer(
  ceiling(frank(cagr_20_apr_24_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_zcta_us[, pctl_us_cagr_24_jul_25_nov := as.integer(
  ceiling(frank(cagr_24_jul_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_zcta_us[, pctl_us_cagr_25_jul_25_nov := as.integer(
  ceiling(frank(cagr_25_jul_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_zcta_us[, pctl_us_cagr_20_apr_25_jul := as.integer(
  ceiling(frank(cagr_20_apr_25_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]
hu_zcta_us[, pctl_us_cagr_20_apr_25_nov := as.integer(
  ceiling(frank(cagr_20_apr_25_nov.x, ties.method = "min", na.last = "keep") / .N * 100)
)]

# Percentiles for agr_* metrics
hu_zcta_us[, pctl_us_agr_24_jul_25_jul := as.integer(
  ceiling(frank(agr_24_jul_25_jul.x, ties.method = "min", na.last = "keep") / .N * 100)
)]


hu_zcta_us <- hu_zcta_us[, .(
  zcta_20,
  idx_us_hgi_20_apr_24_jul,
  idx_us_hgi_24_jul_25_jul,
  idx_us_hgi_24_jul_25_nov,
  idx_us_hgi_25_jul_25_nov,
  idx_us_hgi_20_apr_25_jul,
  idx_us_hgi_20_apr_25_nov,
  idx_us_cagr_20_apr_24_jul,
  idx_us_cagr_24_jul_25_nov,
  idx_us_cagr_25_jul_25_nov,
  idx_us_cagr_20_apr_25_jul,
  idx_us_cagr_20_apr_25_nov,
  idx_us_agr_24_jul_25_jul,
  pctl_us_hgi_20_apr_24_jul,
  pctl_us_hgi_24_jul_25_jul,
  pctl_us_hgi_24_jul_25_nov,
  pctl_us_hgi_25_jul_25_nov,
  pctl_us_hgi_20_apr_25_jul,
  pctl_us_hgi_20_apr_25_nov,
  pctl_us_cagr_20_apr_24_jul,
  pctl_us_cagr_24_jul_25_nov,
  pctl_us_cagr_25_jul_25_nov,
  pctl_us_cagr_20_apr_25_jul,
  pctl_us_cagr_20_apr_25_nov,
  pctl_us_agr_24_jul_25_jul
)]

## MERGE STATE AND US ROLLUPS INTO hu_zcta

hu_zcta_indexes <- as.data.table(
  merge(hu_zcta_state, hu_zcta_us, by = "zcta_20", all.x = TRUE)
)

## CLEANUP FIX FOR DUCKDB BARFING DUE TO UTF ISSUES
## HAT TIP TO https://github.com/duckdb/duckdb-r/issues/12#issuecomment-2419681433
## FOR THESE STRINGI FUNCTIONS

hu_zcta_indexes <- hu_zcta_indexes |> mutate(across(where(is.character), stringi::stri_enc_tonative))

dbWriteTable(conh, "hu_zcta_state", hu_zcta_state, overwrite = TRUE)
dbWriteTable(conh, "hu_zcta_us", hu_zcta_us, overwrite = TRUE)


#### ZCTA INDEXES COMPLETE ###############################


# # use plotly to show a scatter plot of pctl_us_hgi_20_apr_24_jul vs pctl_us_hgi_25_jul_25_nov
# plot_ly(
#   hu_zcta_us,
#   x = ~pctl_us_hgi_20_apr_24_jul,
#   y = ~pctl_us_hgi_24_jul_25_nov,
#   type = "scatter",
#   mode = "markers"
# )