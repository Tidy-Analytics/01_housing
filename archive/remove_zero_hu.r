#!/usr/bin/env Rscript
# remove zero housing unit geographies from base tables only

library(data.table)
library(duckdb)
library(DBI)

# Set working directory to ensure correct database path
setwd("/home/joel")

# Connect to database
drv <- duckdb('./data/housing.duckdb')
conh <- dbConnect(drv)

# Define base tables explicitly
base_tables <- c("hu_block", "hu_block_group", "hu_fed9", "hu_cbsa", 
                 "hu_county", "hu_cousub", "hu_place", "hu_state", 
                 "hu_tract", "hu_ua", "hu_us", "hu_zcta")

cat("\n========================================\n")
cat("DELETING RECORDS WITH NO HOUSING UNITS\n")
cat("FROM BASE TABLES ONLY\n")
cat("========================================\n\n")

# Process each base table
for(base_table in base_tables) {
  
  # Extract geography name for display
  geo_name <- sub('^hu_', '', base_table)
  
  # Check for records with all zeros
  zero_check_sql <- sprintf(
    "SELECT COUNT(*) as zero_count FROM %s WHERE HU_20_apr = 0 AND HU_24_jul = 0 AND HU_25_jul = 0 AND HU_25_nov = 0",
    base_table
  )
  
  zero_count <- dbGetQuery(conh, zero_check_sql)$zero_count
  
  if(zero_count > 0) {
    cat(sprintf("Found %d %s records with no housing units\n", zero_count, geo_name))
    
    # Delete from base table
    delete_base_sql <- sprintf(
      "DELETE FROM %s WHERE HU_20_apr = 0 AND HU_24_jul = 0 AND HU_25_jul = 0 AND HU_25_nov = 0",
      base_table
    )
    rows_deleted <- dbExecute(conh, delete_base_sql)
    cat(sprintf("  Deleted %d records from %s\n\n", rows_deleted, base_table))
  } else {
    cat(sprintf("No zero housing unit records found in %s\n\n", base_table))
  }
}

dbGetQuery(conh, "select a.*, b.* from hu_block_group_county a 
left join hu_block_group b on a.block_group = b.block_group 
where b.block_group is null")

dbGetQuery(conh, "select * from hu_block_group_county  where block_group = '050350303021'")

dbGetQuery(conh, "select block_group, count(*) 
from hu_block_group_county group by block_group having count(*) > 1")

dbGetQuery(conh, "pragma table_info('hu_block_group')")

dbGetQuery(conh, "select count(*) from hu_block_group")

dbGetQuery(conh, "select a.*, b.* from hu_block_group a
left join hu_block_group_county b on a.block_group = b.block_group
 where idx_county_hgi_20_apr_24_jul is null limit 10")

idx_county_hgi_20_apr_24_jul
