import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(filename='pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

def transform_stock_data(stock_data):
    records = []
    for symbol, time_series in stock_data.items():
        for timestamp, values in time_series.items():
            try:
                records.append({
                    "symbol": symbol,
                    "timestamp": datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S"),
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "close": float(values["4. close"]),
                    "volume": int(values["5. volume"])
                })
            except Exception as e:
                logging.error(f"Error transforming stock data for {symbol} at {timestamp}: {e}")
    df = pd.DataFrame(records)
    return df

def transform_news_data(news_data):
    records = []
    for company, articles in news_data.items():
        for article in articles:
            try:
                headline = article["title"]
                sentiment = analyzer.polarity_scores(headline)
                records.append({
                    "company": company,
                    "timestamp": datetime.strptime(article["publishedAt"], "%Y-%m-%dT%H:%M:%SZ"),
                    "headline": headline,
                    "sentiment_score": sentiment["compound"]
                })
            except Exception as e:
                logging.error(f"Error transforming news data for {company} - {headline}: {e}")
    df = pd.DataFrame(records)
    return df

# Test the transformation
if __name__ == "__main__":
    from extract import fetch_stock_data, fetch_news_data
    
    # Test stock transformation
    stock_data = fetch_stock_data(["AAPL", "GOOGL"])
    if stock_data:
        stock_df = transform_stock_data(stock_data)
        print("Transformed Stock Data:\n", stock_df.head())
    
    # Test news transformation
    news_data = fetch_news_data(["Apple", "Google"])
    if news_data:
        news_df = transform_news_data(news_data)
        print("Transformed News Data:\n", news_df.head())