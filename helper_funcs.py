from ratelimit import limits, sleep_and_retry, RateLimitException
from backoff import on_exception, expo
from tqdm import tqdm
import requests
import datetime

# Get nearest friday 
toDate = datetime.datetime.now()
toDate = toDate + datetime.timedelta( (4-toDate.weekday()) % 7 )
toDate = toDate.strftime("%Y-%m-%d")
print("Nearest friday: " + toDate + ".")
print("")

def filterTickers(mainList, filteredList, tableName):
    tickers = []
    for row in mainList.execute("select ticker from screener_results"):
        tickers.append(row)

    for ticker in tqdm(tickers, desc = "'Filtering 'big'" if tableName=='big_filtered' else "'Filtering '20-100'"):
        data = callApi(ticker)
        
        if (data['status'] == "FAILED"): # Closest friday had no options, hence failed
                #print('no friday options for: ' + ticker[0] + ' :)')
                continue

        # Add to filtered list
        filteredList.execute(f"insert into {tableName} (ticker) values ('{ticker[0]}')")

@on_exception(expo, requests.exceptions.RequestException, max_time=60)
@sleep_and_retry
@limits(calls=120, period=60)
def callApi(ticker):
    url = "https://api.tdameritrade.com/v1/marketdata/chains?apikey=KM7SSWFJANTN4HOJIMYUGZAY1C09QWH3&symbol=" + ticker[0] + "&strikeCount=1&fromDate=2022-01-01&toDate=" + toDate
    payload={}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json()

    if list(data.keys())[0] == "error":
        #print("Reached max on: " + ticker[0] + ".  Trying again...")
        raise requests.exceptions.RequestException

    return data