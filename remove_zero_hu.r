#!/usr/bin/env Rscript
# remove zero housing unit geographies from core geos
# county, tract, block group for now only; can expand later

library(data.table)
library(duckdb)
library(DBI)

# DETECTION SQL
# nohu_county <- dbGetQuery(con, "select a.*, b.* FROM hu_county a
# JOIN hu_county_indexes b on a.co_fips = b.co_fips WHERE HU_20 = 0 AND HU_24 = 0 AND HU_25 = 0
# and gq_20 = 0 and gq_24 = 0 and gq_25 = 0;")

# nohu_tract <- dbGetQuery(con, "select a.*, b.* FROM hu_tract a
# JOIN hu_tract_indexes b on a.tract = b.tract WHERE HU_20 = 0 AND HU_24 = 0 AND HU_25 = 0
# and gq_20 = 0 and gq_24 = 0 and gq_25 = 0;")

# nohu_bg <- dbGetQuery(con, "select a.*, b.* FROM hu_block_group a
# JOIN hu_block_group_indexes b on a.block_group = b.block_group WHERE HU_20 = 0 AND HU_24 = 0 AND HU_25 = 0
# and gq_20 = 0 and gq_24 = 0 and gq_25 = 0;")

# Delete records with no housing units from both main and index tables

# FIX SQL

cat("\n========================================\n")
cat("DELETING RECORDS WITH NO HOUSING UNITS\n")
cat("========================================\n\n")

# County deletions
if(nrow(nohu_county) > 0) {
  cat(sprintf("Deleting %d counties with no housing units...\n",
              nrow(nohu_county)))
  dbExecute(con, "DELETE FROM hu_county_indexes WHERE co_fips IN 
    (SELECT co_fips FROM hu_county WHERE HU_20 = 0 AND HU_24 = 0 
     AND HU_25 = 0)")
  dbExecute(con, "DELETE FROM hu_county WHERE HU_20 = 0 AND HU_24 = 0 
    AND HU_25 = 0")
  cat("County deletions complete.\n\n")
}

# Tract deletions
if(nrow(nohu_tract) > 0) {
  cat(sprintf("Deleting %d tracts with no housing units...\n",
              nrow(nohu_tract)))
  dbExecute(con, "DELETE FROM hu_tract_indexes WHERE tract IN 
    (SELECT tract FROM hu_tract WHERE HU_20 = 0 AND HU_24 = 0 
     AND HU_25 = 0)")
  dbExecute(con, "DELETE FROM hu_tract WHERE HU_20 = 0 AND HU_24 = 0 
    AND HU_25 = 0")
  cat("Tract deletions complete.\n\n")
}

# Block group deletions
if(nrow(nohu_bg) > 0) {
  cat(sprintf("Deleting %d block groups with no housing units...\n",
              nrow(nohu_bg)))
  dbExecute(con, "DELETE FROM hu_block_group_indexes WHERE block_group IN 
    (SELECT block_group FROM hu_block_group WHERE HU_20 = 0 AND HU_24 = 0 
     AND HU_25 = 0)")
  dbExecute(con, "DELETE FROM hu_block_group WHERE HU_20 = 0 AND HU_24 = 0 
    AND HU_25 = 0")
  cat("Block group deletions complete.\n\n")
}