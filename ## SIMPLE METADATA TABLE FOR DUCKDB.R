## SIMPLE METADATA TABLE FOR DUCKDB
## THIS WILL CONNECT TO A DATABASE, READ THE SCHEMA, AND GENERATE A METADATA TABLE WITH TABLE NAME, COLUMN NAME, DATA TYPE, AND OTHER RELEVANT INFORMATION
create_metadata_table <- function(db_file) {
  # Connect to DuckDB
  conh <- dbConnect(duckdb::duckdb(), db_file)
  
  # Query metadata from information_schema
  metadata <- data.table(dbGetQuery(conh, "SELECT table_name, column_name, data_type, column_default, is_nullable, ordinal_position as cid FROM information_schema.columns WHERE table_schema = 'main'"))
  metadata <- metadata[, .(table_name, column_name, data_type, column_default, is_nullable, cid)]
  metadata[, column_description := NA]
  
  # Save metadata table back to DuckDB
  dbWriteTable(conh, "metadata", metadata, overwrite = TRUE)
  
  # Close the database connection
  dbDisconnect(conh)
  
  return(metadata)
}

# Usage example:
# create_metadata_table("path/to/your/database.duckdb")
