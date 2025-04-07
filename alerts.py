import smtplib
from email.mime.text import MIMEText
import pandas as pd
from sqlalchemy import create_engine
import os
import logging

# Configure logging
logging.basicConfig(filename='pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Email configuration (replace with your email details)
EMAIL_ADDRESS = "ashishmaddha02@gmail.com"
EMAIL_PASSWORD = "jcvx ctff hpdr ytta"  # Use an App Password for Gmail
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# Database connection
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Ashish2011")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "stock_db")
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def send_email(subject, body):
    try:
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = EMAIL_ADDRESS
        msg["To"] = EMAIL_ADDRESS

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, EMAIL_ADDRESS, msg.as_string())
        logging.info(f"Email sent: {subject}")
        print(f"Email sent: {subject}")
    except Exception as e:
        logging.error(f"Error sending email: {e}")
        print(f"Error sending email: {e}")

def check_alerts():
    try:
        # Fetch the latest stock data
        stock_df = pd.read_sql("SELECT * FROM stock_data ORDER BY symbol, timestamp DESC", engine)
        news_df = pd.read_sql("SELECT * FROM news_sentiment ORDER BY company, timestamp DESC", engine)
        
        # Check for significant price changes (e.g., >5% in the last 5 minutes)
        for symbol in stock_df["symbol"].unique():
            stock_subset = stock_df[stock_df["symbol"] == symbol].head(2)
            if len(stock_subset) >= 2:
                latest_close = stock_subset.iloc[0]["close"]
                previous_close = stock_subset.iloc[1]["close"]
                price_change = ((latest_close - previous_close) / previous_close) * 100
                if abs(price_change) > 5:
                    subject = f"Price Alert: {symbol} Changed by {price_change:.2f}%"
                    body = f"The stock price of {symbol} has changed by {price_change:.2f}% in the last 5 minutes.\nLatest Close: {latest_close}\nPrevious Close: {previous_close}"
                    send_email(subject, body)
        
        # Check for significant sentiment shifts (e.g., sentiment_score < -0.5 or > 0.5)
        for company in news_df["company"].unique():
            news_subset = news_df[news_df["company"] == company].head(1)
            if not news_subset.empty:
                sentiment_score = news_subset.iloc[0]["sentiment_score"]
                if sentiment_score < -0.5:
                    subject = f"Sentiment Alert: Negative Sentiment for {company}"
                    body = f"Negative sentiment detected for {company}.\nSentiment Score: {sentiment_score}\nHeadline: {news_subset.iloc[0]['headline']}"
                    send_email(subject, body)
                elif sentiment_score > 0.5:
                    subject = f"Sentiment Alert: Positive Sentiment for {company}"
                    body = f"Positive sentiment detected for {company}.\nSentiment Score: {sentiment_score}\nHeadline: {news_subset.iloc[0]['headline']}"
                    send_email(subject, body)
    except Exception as e:
        logging.error(f"Error checking alerts: {e}")
        print(f"Error checking alerts: {e}")

# Test the alerts
if __name__ == "__main__":
    check_alerts()