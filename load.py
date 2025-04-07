from sqlalchemy import create_engine
import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(filename='pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection details
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Ashish2011")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "stock_db")
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def load_stock_data(df):
    try:
        df.to_sql("stock_data", engine, if_exists="append", index=False)
        logging.info("Stock data loaded successfully!")
        print("Stock data loaded successfully!")
    except Exception as e:
        logging.error(f"Error loading stock data: {e}")
        print(f"Error loading stock data: {e}")

def load_news_data(df):
    try:
        df.to_sql("news_sentiment", engine, if_exists="append", index=False)
        logging.info("News sentiment data loaded successfully!")
        print("News sentiment data loaded successfully!")
    except Exception as e:
        logging.error(f"Error loading news data: {e}")
        print(f"Error loading news data: {e}")

# Test the load
if __name__ == "__main__":
    from extract import fetch_stock_data, fetch_news_data
    from transform import transform_stock_data, transform_news_data
    
    stock_data = fetch_stock_data(["AAPL", "GOOGL"])
    if stock_data:
        stock_df = transform_stock_data(stock_data)
        load_stock_data(stock_df)
    
    news_data = fetch_news_data(["Apple", "Google"])
    if news_data:
        news_df = transform_news_data(news_data)
        load_news_data(news_df)