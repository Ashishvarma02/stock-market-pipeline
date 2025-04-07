# Stock Market Data Pipeline

A real-time data pipeline that fetches stock prices (Alpha Vantage API), analyzes news sentiment (NewsAPI, VADER), predicts stock prices using an LSTM model (TensorFlow), and sends email alerts. Data is stored in a PostgreSQL database managed with Docker.

## Features
- Real-time stock data for multiple stocks (e.g., AAPL, GOOGL, MSFT).
- Sentiment analysis of news headlines.
- LSTM-based stock price prediction.
- Automated data pipeline with 5-minute refresh.
- Email alerts for significant price changes or sentiment shifts.

## Technologies
- Python, SQL, Docker
- TensorFlow, Scikit-learn, VADER
- PostgreSQL, SQLAlchemy

## Setup
1. Clone the repository: `git clone https://github.com/yourusername/stock-market-pipeline.git`
2. Install dependencies: `pip install -r requirements.txt`
3. Create a `.env` file with API keys (see `.env.example`).
4. Run Docker: `docker run -d --name postgres-container -e POSTGRES_PASSWORD=Ashish2011 -p 5432:5432 postgres`
5. Start the pipeline: `python main.py`

## License
MIT
