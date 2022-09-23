#!/usr/bin/python3

from finviz.screener import Screener
from tqdm import tqdm
from helper_funcs import filterTickers
import sqlite3

# Create current directory
currentDirectory = '/var/www/hmtl/Finviz-Customsdfgs'

# Create connections to databases
con_big = sqlite3.connect(f"{currentDirectory}/databases/big_list.sqlite")
con_toh = sqlite3.connect(f"{currentDirectory}/databases/toh_list.sqlite")
con_big_filtered = sqlite3.connect(f"{currentDirectory}/databases/big_list_filtered.sqlite") 
con_toh_filtered = sqlite3.connect(f"{currentDirectory}/databases/toh_list_filtered.sqlite")

# Create database cursors
cur_big = con_big.cursor()  
cur_toh = con_toh.cursor()
cur_big_filtered = con_big_filtered.cursor() 
cur_toh_filtered = con_toh_filtered.cursor()

# Clear current DB's
try:
    cur_big.execute("drop table screener_results")
    cur_toh.execute("drop table screener_results")
    cur_big_filtered.execute("drop table big_filtered")
    cur_toh_filtered.execute("drop table toh_filtered")
finally:
    pass


# Filters
bigFilters = ["sh_curvol_o200", "sh_opt_option","sh_price_o100"] # Big filter
tohFilters = ["sh_curvol_o2000", "sh_opt_option", "sh_price_20to100"] # 20-100 filter

# Screening/scraping finviz process
print("Starting 'Big' screen...")
big_list = Screener(filters=bigFilters, order="ticker", description='tester')

print("Starting '20-100' screen...")
toh_list = Screener(filters=tohFilters, order="ticker")

print("")
print("Exporting to sqlite databases...")
# Create a SQLite database
big_list.to_sqlite(f"{currentDirectory}/databases/big_list.sqlite")
toh_list.to_sqlite(f"{currentDirectory}/databases/toh_list.sqlite")

# Get and display current counts
print("Big count: " + str(cur_big.execute("select count(No) from screener_results").fetchall()[0][0]))
print('20-100 count: ' + str(cur_toh.execute("select count(No) from screener_results").fetchall()[0][0]))

# Create filtered tables if not exist
cur_big_filtered.execute('create table if not exists big_filtered (ticker varchar(5))')
cur_toh_filtered.execute('create table if not exists toh_filtered (ticker varchar(5))')

print("")
# Filter tickers that only have nearest friday options
filterTickers(cur_big, cur_big_filtered, 'big_filtered')
filterTickers(cur_toh, cur_toh_filtered, 'toh_filtered')

# Get and display current FILTERED counts
print("Filtered big count: " + str(cur_big_filtered.execute("select count(ticker) from big_filtered").fetchall()[0][0]))
print('Filtered 20-100 count: ' + str(cur_toh_filtered.execute("select count(ticker) from toh_filtered").fetchall()[0][0]))

# Commit changes to databases
con_toh_filtered.commit()
con_big_filtered.commit()
print("")
print("Done.")