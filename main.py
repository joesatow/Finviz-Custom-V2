#!/usr/bin/python3
from finviz.screener import Screener

import sqlite3
import time
import os, shutil
start_time = time.time()

# Create current directory
#currentDirectory = '/var/www/hmtl/Finviz-Customs'
#currentDirectory = '/Library/WebServer/Documents/v2'
currentDirectory = '/Library/WebServer/Documents/Finviz-Custom-V2'

# Wipe folders
print("Deleting charts folders contents...")
big_daily_charts_path = f"{currentDirectory}/charts/big-daily"
big_weekly_charts_path = f"{currentDirectory}/charts/big-weekly"
toh_daily_charts_path = f"{currentDirectory}/charts/toh-daily"
toh_weekly_charts_path = f"{currentDirectory}/charts/toh-weekly"
chartFolderPathList = [big_daily_charts_path,big_weekly_charts_path,toh_daily_charts_path,toh_weekly_charts_path]

for folderPath in chartFolderPathList:
    for filename in os.listdir(folderPath):
        file_path = os.path.join(folderPath, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))
print("Done...")
print("")

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
big_count = cur_big.execute("select count(No) from screener_results").fetchall()[0][0]
toh_count = cur_toh.execute("select count(No) from screener_results").fetchall()[0][0]
print("Big count: " + str(big_count))
print('20-100 count: ' + str(toh_count))

# Create filtered tables if not exist
cur_big_filtered.execute('create table if not exists big_filtered (ticker varchar(5))')
cur_toh_filtered.execute('create table if not exists toh_filtered (ticker varchar(5))')

print("")
from helper_funcs import filterTickers
# Filter tickers that only have nearest friday options
filterTickers(cur_big, cur_big_filtered, 'big_filtered')  
filterTickers(cur_toh, cur_toh_filtered, 'toh_filtered')

# Get and display current FILTERED counts
big_filtered_count = cur_big_filtered.execute("select count(ticker) from big_filtered").fetchall()[0][0]
toh_filtered_count = cur_toh_filtered.execute("select count(ticker) from toh_filtered").fetchall()[0][0]
print("")
print("Filtered big count: " + str(big_filtered_count))
print('Filtered 20-100 count: ' + str(toh_filtered_count))

def getHTML(dbCursor,dbTable,filepath):
    html = '' 
    temp = '<tr>'
    count = 1
    for row in dbCursor.execute(f'select ticker from {dbTable}'):
        temp += f'<td><img src="charts/{filepath}/{row[0]}.png"</td>'
        if count % 4 == 0:
            temp += '</tr>'
            html += temp
            temp = '<tr>'
        count += 1
    if temp != "<tr>":
        temp += '</tr>'
        html += temp
    return html

bigDailyHTML = getHTML(cur_big_filtered,'big_filtered','big-daily')
bigWeeklyHTML = getHTML(cur_big_filtered,'big_filtered','big-weekly')
tohDailyHTML = getHTML(cur_toh_filtered,'toh_filtered','toh-daily')
tohWeeklyHTML = getHTML(cur_toh_filtered,'toh_filtered','toh-weekly')

with open('resultsOutput.txt', 'w') as f:
    stringToWrite = f"<td colspan=2>Original count: {big_count}</td><td colspan=2>Original count: {toh_count}</td>,"
    stringToWrite += f"<td colspan=2>Filtered count: {big_filtered_count}</td><td colspan=2>Filtered count: {toh_filtered_count}</td>,"
    stringToWrite += bigDailyHTML + ','
    stringToWrite += bigWeeklyHTML + ','
    stringToWrite += tohDailyHTML + ','
    stringToWrite += tohWeeklyHTML
    f.write(stringToWrite)

# Commit changes to databases
con_toh_filtered.commit()
con_big_filtered.commit()

print("")
filteredTickersBig = []
filteredTickersToh = []

# Change data of screener objects to filtered tickers
for row in cur_big_filtered.execute("select ticker from big_filtered"):
    filteredTickersBig.append(row[0])
big_list.data = filteredTickersBig

for row in cur_toh_filtered.execute("select ticker from toh_filtered"):
    filteredTickersToh.append(row[0])
toh_list.data = filteredTickersToh

big_list.get_charts(period='d',size='m',chart_type='c',ta='0') # daily
big_list.get_charts(period='w',size='m',chart_type='c',ta='0') # weekly
toh_list.get_charts(period='d',size='m',chart_type='c',ta='0') # daily
toh_list.get_charts(period='w',size='m',chart_type='c',ta='0') # daily

print("")
end_time = time.time()
print(f"Done. Execution time: {((end_time - start_time)/60):.2f} minutes.")