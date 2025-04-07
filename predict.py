import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from sqlalchemy import create_engine
import os
import logging

# Configure logging
logging.basicConfig(filename='pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Ashish2011")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "stock_db")
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def fetch_data_for_prediction():
    try:
        # Fetch stock and news data
        stock_df = pd.read_sql("SELECT * FROM stock_data ORDER BY symbol, timestamp", engine)
        news_df = pd.read_sql("SELECT * FROM news_sentiment ORDER BY company, timestamp", engine)
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        print(f"Error fetching data: {e}")
        return None, None

    return stock_df, news_df

def prepare_data(data, look_back=60):
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(data)
    
    X, y = [], []
    for i in range(look_back, len(scaled_data)):
        X.append(scaled_data[i-look_back:i])
        y.append(scaled_data[i, 0])  # Predict the 'close' price
    
    return np.array(X), np.array(y), scaler

def build_lstm_model(input_shape):
    model = Sequential()
    model.add(LSTM(units=50, return_sequences=True, input_shape=input_shape))
    model.add(Dropout(0.2))
    model.add(LSTM(units=50))
    model.add(Dropout(0.2))
    model.add(Dense(units=1))
    model.compile(optimizer="adam", loss="mean_squared_error")
    return model

def predict_stock_price():
    # Fetch data
    stock_df, news_df = fetch_data_for_prediction()
    if stock_df is None or news_df is None:
        return None
    
    # Map companies to symbols
    company_to_symbol = {"Apple": "AAPL", "Google": "GOOGL", "Microsoft": "MSFT"}
    news_df["symbol"] = news_df["company"].map(company_to_symbol)
    
    # Process each stock separately
    all_predictions = []
    for symbol in stock_df["symbol"].unique():
        try:
            # Filter data for the current stock
            stock_subset = stock_df[stock_df["symbol"] == symbol].copy()
            news_subset = news_df[news_df["symbol"] == symbol].copy()
            
            if len(stock_subset) < 60:
                logging.warning(f"Not enough data for {symbol} to make predictions (need at least 60 rows).")
                print(f"Not enough data for {symbol} to make predictions.")
                continue
            
            # Merge stock and news data
            stock_subset["timestamp"] = pd.to_datetime(stock_subset["timestamp"])
            news_subset["timestamp"] = pd.to_datetime(news_subset["timestamp"])
            merged_df = pd.merge_asof(stock_subset, news_subset, on="timestamp", direction="nearest")
            data = merged_df[["close", "sentiment_score"]]
            
            # Prepare data for LSTM
            look_back = 60
            X, y, scaler = prepare_data(data, look_back)
            
            # Split into train and test
            train_size = int(len(X) * 0.8)
            X_train, X_test = X[:train_size], X[train_size:]
            y_train, y_test = y[:train_size], y[train_size:]
            
            # Build and train the model
            model = build_lstm_model((look_back, X.shape[2]))
            model.fit(X_train, y_train, epochs=5, batch_size=32, verbose=1)
            
            # Make predictions
            predictions = model.predict(X_test)
            
            # Inverse transform predictions
            predictions = scaler.inverse_transform(np.concatenate((predictions, np.zeros((predictions.shape[0], 1))), axis=1))[:, 0]
            y_test = scaler.inverse_transform(np.concatenate((y_test.reshape(-1, 1), np.zeros((y_test.shape[0], 1))), axis=1))[:, 0]
            
            # Create a DataFrame for predictions
            prediction_df = pd.DataFrame({
                "symbol": symbol,
                "timestamp": stock_subset["timestamp"].iloc[-len(predictions):].values,
                "predicted_close": predictions,
                "actual_close": y_test
            })
            all_predictions.append(prediction_df)
            
            logging.info(f"Predictions made for {symbol}")
            print(f"Predictions made for {symbol}")
        except Exception as e:
            logging.error(f"Error predicting for {symbol}: {e}")
            print(f"Error predicting for {symbol}: {e}")
    
    if not all_predictions:
        logging.warning("No predictions were made.")
        print("No predictions were made.")
        return None
    
    # Concatenate all predictions and save to database
    final_predictions = pd.concat(all_predictions, ignore_index=True)
    try:
        final_predictions.to_sql("predictions", engine, if_exists="append", index=False)
        logging.info("Predictions saved to database.")
        print("Predictions saved to database.")
    except Exception as e:
        logging.error(f"Error saving predictions to database: {e}")
        print(f"Error saving predictions to database: {e}")
    
    return final_predictions

# Test the prediction
if __name__ == "__main__":
    predictions = predict_stock_price()
    if predictions is not None:
        print("Sample Predictions:\n", predictions.head())
    else:
        print("Prediction failed.")