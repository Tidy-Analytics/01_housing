import polars as pl
import duckdb
from pathlib import Path
import os
from dotenv import load_dotenv

# Set working directory
os.chdir("/home/joel")

# Load environment variables
load_dotenv()

### DUCKDB CONNECTION

conh = duckdb.connect('./data/housingpy.duckdb')
print(conh.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall())

# Test query
print(conh.execute("SELECT * FROM hu_tract LIMIT 5").df())


### THIS SUPPLIES THE BLOCK TO COUNTY SUBDIVISION, BLOCK TO PLACE, AND BLOCK TO URBAN AREA LOOKUPS

congref = duckdb.connect(
    './data/georeferencepy.duckdb',
    read_only=False
)
congref.execute("INSTALL spatial; LOAD spatial;")

congeo = duckdb.connect(
    './data/spatial_storage.duckdb',
    read_only=False
)
congeo.execute("INSTALL spatial; LOAD spatial;")

table_list = congeo.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()

# Unpack table list to simple text list
table_names = [table[0] for table in table_list]

# Save to txt file with line breaks
with open('table_list.txt', 'w') as f:
  f.write('\n'.join(table_names))


