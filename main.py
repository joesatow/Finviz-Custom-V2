#!/usr/bin/python3
from finviz.screener import Screener

import sqlite3
import time
start_time = time.time()

print("")

# Create current directory
#currentDirectory = '/var/www/hmtl/Finviz-Customs'
currentDirectory = '/Library/Webserver/Documents/v2'

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
cur_big.execute("drop table if exists screener_results")
cur_toh.execute("drop table if exists screener_results")
cur_big_filtered.execute("drop table if exists big_filtered")
cur_toh_filtered.execute("drop table if exists toh_filtered")

# Filters
bigFilters = ["sh_curvol_o200", "sh_opt_option","sh_price_o100"] # Big filter
tohFilters = ["sh_curvol_o2000", "sh_opt_option", "sh_price_20to100"] # 20-100 filter

# Screening/scraping finviz process
big_list = Screener(filters=bigFilters, order="ticker")
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
from helper_funcs import filterTickers
# Filter tickers that only have nearest friday options
filterTickers(cur_big, cur_big_filtered, 'big_filtered')  
filterTickers(cur_toh, cur_toh_filtered, 'toh_filtered')

# Get and display current FILTERED counts
print("")
print("Filtered big count: " + str(cur_big_filtered.execute("select count(ticker) from big_filtered").fetchall()[0][0]))
print('Filtered 20-100 count: ' + str(cur_toh_filtered.execute("select count(ticker) from toh_filtered").fetchall()[0][0]))

# Commit changes to databases
con_toh_filtered.commit()
con_big_filtered.commit()

print("")
end_time = time.time()
print(f"Done. Execution time: {((end_time - start_time)/60):.2f} minutes.")