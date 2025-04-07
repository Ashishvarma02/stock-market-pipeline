import logging
from extract import fetch_stock_data, fetch_news_data
from transform import transform_stock_data, transform_news_data
from load import load_stock_data, load_news_data
from predict import predict_stock_price
from alerts import check_alerts
import time

# Configure logging
logging.basicConfig(filename='pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_pipeline():
    symbols = ["AAPL", "GOOGL", "MSFT"]
    companies = ["Apple", "Google", "Microsoft"]
    prediction_interval = 12  # Run predictions every 12 cycles (1 hour if cycle is 5 minutes)
    cycle_count = 0

    while True:
        try:
            # Fetch and load data
            logging.info("Fetching stock data...")
            stock_data = fetch_stock_data(symbols)
            if stock_data:
                stock_df = transform_stock_data(stock_data)
                load_stock_data(stock_df)
            
            logging.info("Fetching news data...")
            news_data = fetch_news_data(companies)
            if news_data:
                news_df = transform_news_data(news_data)
                load_news_data(news_df)
            
            # Check for alerts
            logging.info("Checking for alerts...")
            check_alerts()
            
            # Run predictions periodically
            cycle_count += 1
            if cycle_count % prediction_interval == 0:
                logging.info("Running predictions...")
                predictions = predict_stock_price()
                if predictions is not None:
                    logging.info("Predictions completed successfully.")
                else:
                    logging.warning("Predictions failed.")
            
            time.sleep(300)  # Wait 5 minutes
        except Exception as e:
            logging.error(f"An error occurred in the pipeline: {e}")
            print(f"An error occurred in the pipeline: {e}")
            time.sleep(60)

if __name__ == "__main__":
    run_pipeline()
if __name__ == "__main__":
    run_pipeline()
    #THis will satrt the pipeline 