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

drv <- duckdb('./data/housing.duckdb')
conh <- dbConnect(drv)
dbListTables(conh)

congeo <- dbConnect(
  duckdb::duckdb(), 
  dbdir = "./data/spatial_storage.duckdb",
  extensions = c("spatial")
)

dbListTables(conh)
dbListTables(congeo)

dbGetQuery(congeo, "pragma table_info('geo_state')")


### NAMES HEADERS FOR EACH GEO LAYER IN HOUSING DATA

block_group_names <- as.data.table(
  dbGetQuery(congeo, "SELECT geoid as block_group_id, namelsad as block_group_name FROM geo_block_group")
  )

census_tract_names <- as.data.table(
  dbGetQuery(congeo, "SELECT geoid as census_tract_id, namelsad as census_tract_name FROM geo_tract")
  )

county_subdivision_names <- as.data.table(
  dbGetQuery(congeo, "SELECT geoid as county_sub_id, namelsad as county_sub_name, namelsadco as county, stusps as state FROM geo_cosub_23")
  )

county_names <- as.data.table(
  dbGetQuery(congeo, "SELECT statefp || countyfp as county_id, namelsad as county_name, stusps as state FROM geo_county_23")
  )

zcta_names <- as.data.table(
  dbGetQuery(congeo, "SELECT zcta5ce20 as zcta_id, 'ZCTA ' || zcta5ce20 as zcta_name FROM geo_zcta")
  )

place_names <- as.data.table(
  dbGetQuery(congeo, "SELECT statefp || placefp as place_id, namelsad as place_name, stusps as state FROM geo_place_23")
  )

cbsa_names <- as.data.table(
  dbGetQuery(congeo, "SELECT cbsafp as cbsa_id, namelsad as cbsa_name FROM geo_cbsa_23")
  )

## GEO REF NEEDED FOR UA NAMES

congref <- dbConnect(
  duckdb::duckdb(),
  dbdir = "./data/georeference.duckdb",
  extensions = c("spatial"))

dbGetQuery(congref, "pragma table_info('block_ua')")

## note we carry state code as well as unique ua code to allow ua within state comparisons in indexing calculations

ua_look <- data.table(
  dbGetQuery(congref, "select ua, uaname, substr(geoid, 1, 2) as state_code  from block_ua  group by ua, uaname, state_code order by ua;")
)

ua_names <- as.data.table(
  dbGetQuery(congref, "SELECT geoid as ua_id, namelsad as ua_name FROM block_ua order by geoid")
  )

state_names <- as.data.table(
  dbGetQuery(congeo, "SELECT name as state_name, statefp as state_code FROM geo_state")
  )

### end of names headers; layers: block group, census tract, county subdivision, county, zcta, place, cbsa, ua, 