import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os

def download_forex_data(pair, start_date, end_date):
    try:
        data = yf.download(pair, start=start_date, end=end_date)
        if data.empty:
            print(f"No data available for {pair}")
            return None
        
        # インデックスがタイムスタンプであることを確認し、必要に応じてローカライズ
        if not data.index.tz:
            data.index = data.index.tz_localize('UTC')  # UTCにローカライズ
        
        return data['Close']
    except Exception as e:
        print(f"Error downloading data for {pair}: {str(e)}")
        return None

# Define the currency pairs
pairs = ['USDJPY=X', 'EURJPY=X']

# Set the start date to January 1, 2018
start_date = '2018-01-01'

# Set the end date to yesterday (to avoid potential issues with incomplete current day data)
end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

# Create an empty DataFrame to store the results
df = pd.DataFrame()

# Fetch data for each currency pair
for pair in pairs:
    pair_data = download_forex_data(pair, start_date, end_date)
    if pair_data is not None:
        df[pair] = pair_data

# Check if we have any data
if df.empty:
    print("No data was retrieved. Please check your internet connection and try again.")
else:
    # Rename columns to remove '=X' suffix
    df.columns = [col.replace('=X', '') for col in df.columns]

    # Display the first few rows of the data
    print(df.head())

    # Specify the save directory
    save_dir = r'C:\Users\100ca\Documents\PyCode\TRADEHISTORY\DIC'

    # Create the directory if it doesn't exist
    os.makedirs(save_dir, exist_ok=True)

    # Specify the full path for the CSV file
    csv_path = os.path.join(save_dir, 'forex_data.csv')

    # Save the data to a CSV file
    df.to_csv(csv_path)
    print(f"Data saved to {csv_path}")

    # Basic statistics
    print("\nBasic Statistics:")
    print(df.describe())

    # Print the total number of data points
    print(f"\nTotal number of data points: {len(df)}")

    # Print the date range
    print(f"Date range: from {df.index.min()} to {df.index.max()}")

print("Script execution completed.")



