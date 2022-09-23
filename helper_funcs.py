from ratelimit import limits, sleep_and_retry
from tqdm import tqdm
import requests
import datetime

toDate = datetime.datetime.now()
toDate = toDate + datetime.timedelta( (4-toDate.weekday()) % 7 )
toDate = toDate.strftime("%Y-%m-%d")

tickers = []

def filterTickers(mainList, filteredList, tableName):
    for row in mainList.execute("select ticker from screener_results"):
        tickers.append(row)

    for ticker in tqdm(tickers, desc = 'Filtering big' if tableName=='big_filtered' else 'Filtering 20-100'):
        data = callApi(ticker)

        if (data['status'] == "FAILED"): # Closest friday had no options, hence failed
            continue

        # Add to filtered list
        filteredList.execute(f"insert into {tableName} (ticker) values ('{ticker[0]}')")

@sleep_and_retry
@limits(calls=115, period=60)
def callApi(ticker):
    url = "https://api.tdameritrade.com/v1/marketdata/chains?apikey=KM7SSWFJANTN4HOJIMYUGZAY1C09QWH3&symbol=" + ticker[0] + "&strikeCount=1&fromDate=2022-01-01&toDate=" + toDate
    payload={}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json()
    return data