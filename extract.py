import requests
from newsapi import NewsApiClient
import logging

# Configure logging
logging.basicConfig(filename='pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# API keys (replace with your actual keys)
ALPHA_VANTAGE_API_KEY = "DN8HIOUER6YYX070"
NEWSAPI_KEY = "54a0cdc5692649ab896465a9360ed5b0"

# Initialize NewsAPI client
newsapi = NewsApiClient(api_key=NEWSAPI_KEY)

def fetch_stock_data(symbols=["AAPL", "GOOGL", "MSFT"]):
    all_data = {}
    for symbol in symbols:
        try:
            logging.info(f"Fetching stock data for {symbol}...")
            url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={ALPHA_VANTAGE_API_KEY}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                time_series = data.get("Time Series (5min)", {})
                if time_series:
                    all_data[symbol] = time_series
                else:
                    logging.warning(f"No time series data for {symbol}")
            else:
                logging.error(f"Error fetching stock data for {symbol}: {response.status_code}")
        except Exception as e:
            logging.error(f"Exception while fetching stock data for {symbol}: {e}")
    return all_data

def fetch_news_data(companies=["Apple", "Google", "Microsoft"]):
    all_articles = {}
    for company in companies:
        try:
            logging.info(f"Fetching news data for {company}...")
            news = newsapi.get_everything(q=company, language="en", sort_by="publishedAt", page_size=10)
            articles = news.get("articles", [])
            if articles:
                all_articles[company] = articles
            else:
                logging.warning(f"No news articles found for {company}")
        except Exception as e:
            logging.error(f"Exception while fetching news data for {company}: {e}")
    return all_articles

# Test the functions
if __name__ == "__main__":
    # Test stock data
    stock_data = fetch_stock_data(["AAPL", "GOOGL"])
    if stock_data:
        for symbol, data in stock_data.items():
            print(f"Stock Data for {symbol}:", list(data.items())[:2])
    
    # Test news data
    news_data = fetch_news_data(["Apple", "Google"])
    if news_data:
        for company, articles in news_data.items():
            print(f"News Data for {company}:", articles[:2])