## ZCTA TO COUNTY MAPPING FILE
## THIS IS BASED ON THE MASTER REFERENCE FILE FROM THE CENSUS BUREAU
## WE WILL CALCULATE THE AREA OF EACH ZCTA AND THE AREA OF EACH COUNTY
## THEN WE WILL CALCULATE THE OVERLAP BETWEEN THE TWO TO DETERMINE THE
## A SCORE THAT DETERMINES WHICH COUNTY 'WINS' THE ZCTA ASSIGNMENT

library(sf)
library(tidycensus)
library(tigris)
library(leaflet)
library(dplyr)
library(data.table)
library(duckdb)
library(stringr)
library(plotly)

# Set up DuckDB connection
drv <- duckdb(dbdir = '/home/joel/data/geodem.duckdb')
con <- dbConnect(drv)

# LOAD REFERENCE DATA INTO DUCKDB '/home/joel/data/tab20_zcta520_county20_natl.txt'


zcta_to_county <- data.table(
  dbGetQuery(
    con,
    "select * from read_csv_auto(
     './data/tab20_zcta520_county20_natl.txt',
     header=True, normalize_names=True
     );"
  )
)

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

zcta_to_county <- zcta_to_county[rank == 1]

# save to csv; keep only 'geoid_zcta5_20' and 'geoid_county_20' columns, rename them to 'zcta' and 'county_fips_code'

zcta_to_county <- zcta_to_county[, .(zcta = geoid_zcta5_20, county_fips_code = geoid_county_20)]

fwrite(zcta_to_county, './data/zcta_to_county.csv')