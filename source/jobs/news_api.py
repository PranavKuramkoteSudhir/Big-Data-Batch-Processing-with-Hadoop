import requests
from datetime import datetime, timedelta
import pandas as pd

API_KEY = "api_key"

def run_news_api(api_key=API_KEY):
    try:
        request = requests.get(f"https://api.marketaux.com/v1/news/all?symbols=TSLA,AMZN,MSFT,APPL&published_after={(datetime.now() - timedelta(minutes=20)).strftime('%Y-%m-%dT%H:%M:%S')}&group_similar=true&filter_entities=true&language=en&api_token={API_KEY}")
        if request.status_code == 200:
            data = request.json()
            data_list = []

            for i in range(len(data['data'])):
                ticker_list = []

                for j in range(len(data['data'][i]['entities'])):
                    ticker_list.append(data['data'][i]['entities'][j]['symbol'])

                # Include datetime in the data_dict
                data_dict = {
                    'ticker': ticker_list,
                    'title': data['data'][i]['title'],
                    'desc': data['data'][i]['description'],
                    'published_at': data['data'][i]['published_at']  # Add this line for datetime
                }

                data_list.append(data_dict)
            return pd.DataFrame(data_list)
        else:
            return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        return pd.DataFrame()

if __name__ == "__main__":
    news_df = run_news_api()
    print(news_df)
