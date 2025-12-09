## GEO REFERENCE LOOKUPS FROM GEOCORR
## block to county subdivision
## block to place
## block to urban area

library(sf)
library(tidycensus)
library(tigris)
library(leaflet)
library(dplyr)
library(data.table)
library(duckdb)
library(stringr)
library(plotly)
library(leafem)
library(DBI)
library(htmltools)

## WE WILL DEFINE A NEW GEO REF DATABASE

dotenv::load_dot_env()

con  <- dbConnect(duckdb::duckdb(), dbdir = "./data/geodem.duckdb")
# Connect with the 'extensions' parameter to auto-load required extensions
congeo <- dbConnect(duckdb::duckdb(),
                    dbdir = "./data/spatial_storage.duckdb",
                    read_only = FALSE,
                    extensions = c("spatial"))

congref <- dbConnect(duckdb::duckdb(),
                    dbdir = "./data/georeference.duckdb",
                    read_only = FALSE,
                    extensions = c("spatial"))

# Ensure the spatial extension is loaded
dbExecute(congeo, "INSTALL spatial")
dbExecute(congeo, "LOAD spatial;")

dbExecute(congref, "INSTALL spatial")
dbExecute(congref, "LOAD spatial;")

dbListTables(con)
dbListTables(congeo)
dbListTables(congref)

### BLOCK TO PLACE REFERENCE #########################################################

block_place_files <- list.files("./data/geo", pattern = "block-place-.*\\.csv", full.names = TRUE)

for (file in block_place_files) {
  # Read the CSV file
  data <- fread(file)
  # create GEOID as concatenation of county, tract, and block; use data.table idioms
  data[, GEOID := paste0(county, gsub("\\.", "", tract), block)]
  data_clean <- data %>%
  mutate(across(where(is.character), ~iconv(.x, to = "UTF-8", sub = "?")))
  
  
  # Extract the table name from the file name
  table_name <- gsub(".*block-place-(.*)\\.csv", "block_place_\\1", basename(file))

  # lower case all column names 
  setnames(data_clean, old = names(data_clean), new = tolower(names(data_clean)))

# convert pop20 to int, intptlat and inptlon to numeric
  data_clean[, pop20 := as.integer(pop20)]
  data_clean[, intptlat := as.numeric(intptlat)]
  data_clean[, intptlon := as.numeric(intptlon)]
  data_clean[, afact := as.numeric(afact)]
  data_clean[, afact2 := as.numeric(afact2)]


  dbWriteTable(congref, table_name, data_clean, overwrite = TRUE)
}

# create a single block_place table by unioning all block_place tables using DuckDB SQL only
# remove row 1 in each table before unioning, as this row contains duplicate column names as long strings

block_place_tables <- dbListTables(congref)[grepl("block_place_", dbListTables(congref))]

# Build the UNION ALL query dynamically
union_query <- paste(
  sapply(block_place_tables, function(table) {
    paste0("SELECT * FROM ", table, " WHERE rowid > 1")
  }),
  collapse = " UNION ALL "
)

create_block_to_place <- paste0(
  "CREATE OR REPLACE TABLE block_place AS ",
  union_query
)

# Execute the query
dbExecute(congref, create_block_to_place)

dbGetQuery(congref, "SELECT * FROM block_place LIMIT 10")

### END BLOCK TO PLACE REFERENCE #########################################################
##########################################################################################

##########################################################################################
### BLOCK TO COUNTY SUBDIVISION REFERENCE ###############################################

block_cosub_files <- list.files("./data/geo", pattern = "block-cosub-.*\\.csv", full.names = TRUE)

for (file in block_cosub_files) {

  data <- fread(file)

  data[, GEOID := paste0(county, gsub("\\.", "", tract), block)]

  data_clean <- data %>%
    mutate(across(where(is.character), ~iconv(.x, to = "UTF-8", sub = "?")))

  table_name <- gsub(".*block-cosub-(.*)\\.csv", "block_cosub_\\1", basename(file))
  
  setnames(data_clean, old = names(data_clean), new = tolower(names(data_clean)))

# convert pop20 to int, intptlat and inptlon to numeric
  data_clean[, pop20 := as.integer(pop20)]
  data_clean[, intptlat := as.numeric(intptlat)]
  data_clean[, intptlon := as.numeric(intptlon)]
  data_clean[, afact := as.numeric(afact)]
  data_clean[, afact2 := as.numeric(afact2)]

  
  dbWriteTable(congref, table_name, data_clean, overwrite = TRUE)
}

block_cosub_tables <- dbListTables(congref)[grepl("block_cosub_", dbListTables(congref))]

# Build the UNION ALL query dynamically
union_query <- paste(
  sapply(block_cosub_tables, function(table) {
    paste0("SELECT * FROM ", table, " WHERE rowid > 1")
  }),
  collapse = " UNION ALL "
)

create_block_to_cosub <- paste0(
  "CREATE OR REPLACE TABLE block_cosub AS ",
  union_query
)

# Execute the query
dbExecute(congref, create_block_to_cosub)

### END BLOCK TO COUNTY SUBDIVISION REFERENCE ###############################################
##########################################################################################


##########################################################################################
### BLOCK TO URBAN AREAS REFERENCE ###############################################

block_ua_files <- list.files("./data/geo", pattern = "block-ua-.*\\.csv", full.names = TRUE)

for (file in block_ua_files) {

  data <- fread(file)

  data[, GEOID := paste0(county, gsub("\\.", "", tract), block)]

  data_clean <- data %>%
    mutate(across(where(is.character), ~iconv(.x, to = "UTF-8", sub = "?")))

  table_name <- gsub(".*block-ua-(.*)\\.csv", "block_ua_\\1", basename(file))

  setnames(data_clean, old = names(data_clean), new = tolower(names(data_clean)))

  # convert pop20 to int, intptlat and inptlon to numeric
  data_clean[, pop20 := as.integer(pop20)]
  data_clean[, intptlat := as.numeric(intptlat)]
  data_clean[, intptlon := as.numeric(intptlon)]
  data_clean[, afact := as.numeric(afact)]
  data_clean[, afact2 := as.numeric(afact2)]

  
  dbWriteTable(congref, table_name, data_clean, overwrite = TRUE)
}

block_ua_tables <- dbListTables(congref)[grepl("block_ua_", dbListTables(congref))]

# Build the UNION ALL query dynamically
union_query <- paste(
  sapply(block_ua_tables, function(table) {
    paste0("SELECT * FROM ", table, " WHERE rowid > 1")
  }),
  collapse = " UNION ALL "
)

create_block_to_ua <- paste0(
  "CREATE OR REPLACE TABLE block_ua AS ",
  union_query
)

# Execute the query
dbExecute(congref, create_block_to_ua)


## randomly select records for review

block_place_sample <- dbGetQuery(
  congref, 
  "SELECT * FROM block_place where substr(county, 1, 2) = '27' ORDER BY RANDOM() LIMIT 1000"
)

block_cosub_sample <- dbGetQuery(
  congref, 
  "SELECT * FROM block_cosub where substr(county, 1, 2) = '27' ORDER BY RANDOM() LIMIT 1000"
  )

block_ua_sample <- dbGetQuery(
  congref,
  "SELECT * FROM block_ua where substr(county, 1, 2) = '27' ORDER BY RANDOM() LIMIT 1000"
  )

### BLOCK COUNTS AND POPULATION FOR COUNTY SUBS

block_cosub_counts <- data.table(
  dbGetQuery(congref, "
  SELECT 
    county, 
    countyname,
    cousub20 AS cosub, 
    mcdname,
    COUNT(*) AS block_count,
    SUM(pop20) AS total_population 
  FROM block_cosub 
  GROUP BY all"
  )
)


block_place_counts <- data.table(
  dbGetQuery(congref, "
  SELECT 
    county, 
    countyname,
    place, 
    placename,
    COUNT(*) AS block_count,
    SUM(pop20) AS total_population 
  FROM block_place
  GROUP BY all"
  )
)


block_ua_counts <- data.table(
  dbGetQuery(congref, "
  SELECT 
    county, 
    countyname,
    ua, 
    uaname,
    COUNT(*) AS block_count,
    SUM(pop20) AS total_population 
  FROM block_ua
  GROUP BY all"
  )
)

block_ua_counts[, population_per_block := total_population / block_count]

library(reactable)

reactable::reactable(
  block_ua_counts,
  columns = list(
    county = reactable::colDef(name = "County FIPS"),
    countyname = reactable::colDef(name = "County Name"),
    ua = reactable::colDef(name = "Urban Area Code"),
    uaname = reactable::colDef(name = "Urban Area Name"),
    block_count = reactable::colDef(name = "Block Count", format = reactable::colFormat(separators = TRUE)),
    total_population = reactable::colDef(name = "Total Population", format = reactable::colFormat(separators = TRUE)),
    population_per_block = reactable::colDef(name = "Population per Block", format = reactable::colFormat(separators = TRUE))
  ),
  defaultPageSize = 10,
  filterable = TRUE,
  searchable = TRUE,
  highlight = TRUE,
)

dbGetQuery(congref, "PRAGMA table_info(block_ua)")

dbGetQuery(congref, "select geoid, ua from block_ua")


dbDisconnect(congref)
