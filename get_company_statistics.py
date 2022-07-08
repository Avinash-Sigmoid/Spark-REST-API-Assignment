import requests
import csv
import json

url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"
headers = {
		"X-RapidAPI-Key": "14008bcf0dmsh53056f154f78195p12b0a5jsn55387aeff04f",
		"X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
	}
response = requests.request("GET", url, headers=headers)
data=response.text
data=json.loads(data)
CompanyName=data["stocks"]


CompanyName1=CompanyName[0:14]
for company in CompanyName1:
	querystring = {"ticker_symbol": company, "years": "5", "format": "json"}
	new_response = requests.request("GET", url, headers=headers, params=querystring)
	myjson = new_response.json()
	ourdata = []
	headersName = ["Open", "High", "Low", "Close", "Adj Close", "Volume", "Date", "CompanyName"]
	for i in myjson['historical prices']:
		listing = [i["Open"], i["High"], i["Low"], i["Close"], i["Adj Close"], i["Volume"], i["Date"], company]
		ourdata.append(listing)
	with open(f"f{company}.csv", 'w', encoding='UTF8', newline='') as f:
		writer = csv.writer(f)
		writer.writerow(headersName)
		for i in ourdata:
			writer.writerow(i)

