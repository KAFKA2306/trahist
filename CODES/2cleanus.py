import pandas as pd
import numpy as np
import os
import re
from datetime import datetime, timedelta

# Path settings
base_path = r"C:\Users\100ca\Documents\PyCode\TRADEHISTORY"
input_file = os.path.join(base_path, "integrated_trade_history_20240921.csv")
security_code_file = os.path.join(base_path, "DIC", "securitycode.csv")
forex_data_file = os.path.join(base_path, "DIC", "forex_data.csv")
jpx_codes_us_file = os.path.join(base_path, "DIC", "jpxcodesus.csv")  # New file path
output_file = os.path.join(base_path, "trade_history3.csv")

# Load data
df = pd.read_csv(input_file)
security_code = pd.read_csv(security_code_file)
forex_data = pd.read_csv(forex_data_file)
jpx_codes_us = pd.read_csv(jpx_codes_us_file)  # Load the jpxcodesus.csv file

# Clean and standardize date formats
def parse_dates(date_series):
    parsed_dates = pd.to_datetime(date_series, errors='coerce')
    if parsed_dates.isnull().all():
        print("No valid dates found in the series.")
    return parsed_dates

df['trade_date'] = parse_dates(df['trade_date'])
df['settlement_date'] = parse_dates(df['settlement_date'])

forex_data['Date'] = pd.to_datetime(forex_data['Date'], errors='coerce')
forex_data = forex_data.rename(columns={'Date': 'trade_date'})
forex_data['trade_date'] = forex_data['trade_date'].dt.tz_localize(None)

# Merge with forex data
df = pd.merge(df, forex_data, on='trade_date', how='left')

# Clean and convert numeric columns
def clean_numeric(x):
    if pd.isna(x) or x == '-':
        return np.nan
    if isinstance(x, str):
        x = re.sub(r'[,円]', '', x)
        match = re.search(r'-?\d+(\.\d+)?', x)
        if match:
            return float(match.group())
    try:
        return float(x)
    except ValueError:
        return np.nan

numeric_columns = ['quantity', 'price', 'settlement_amount']
for col in numeric_columns:
    df[col] = df[col].apply(clean_numeric)

# Standardize currency
currency_mapping = {
    'JPY': 'JPY', '日本円': 'JPY', '円': 'JPY',
    'USD': 'USD', '米国ドル': 'USD', '米ドル': 'USD'
}
df['currency'] = df['currency'].map(currency_mapping).fillna('Unknown')

# Standardize transaction types
transaction_type_mapping = {
    'buy': 'Buy', 'sell': 'Sell', '買付': 'Buy', '売付': 'Sell'
}
df['transaction_type'] = df['transaction_type'].map(transaction_type_mapping).fillna('Other')

# Standardize account types
account_type_mapping = {
    '特定': 'Specific', 'つみたてNISA': 'Cumulative NISA', 
    'つみたてNISA   ': 'Cumulative NISA', '一般': 'General'
}
df['account_type'] = df['account_type'].map(account_type_mapping).fillna('Other')

# Merge with security code data
df = pd.merge(df, security_code, on='security_name', how='left', suffixes=('', '_y'))
df['security_code'] = df['security_code'].fillna(df['security_code_y'])
df = df.drop('security_code_y', axis=1)

# Convert Japanese ETF codes to US ETF codes
def convert_etf_code(code):
    if code in jpx_codes_us['コード'].values:
        us_code = jpx_codes_us.loc[jpx_codes_us['コード'] == code, '類似米国ETFティッカー'].iloc[0]
        return us_code
    return code

df['security_code'] = df['security_code'].apply(convert_etf_code)

def classify_investment_type(row):
    if 'JP' in row['data_source']:
        return '日本株'
    elif 'US' in row['data_source']:
        return '米国株'
    elif 'INVST' in row['data_source']:
        return '投資信託'
    elif 'SaveFile' in row['data_source']:
        return '日本株か投資信託'
    elif 'SBI' in row['data_source']:
        return '日本株か投資信託'  
    elif 'yakujo' in row['data_source']:
        return '米国株' 
    else:
        return 'その他'

df['investment_type'] = df.apply(classify_investment_type, axis=1)

def convert_to_jpy(row):
    if row['currency'] == 'USD':
        if pd.isna(row['USDJPY']):
            print(f"Warning: USDJPY rate is missing for row: {row}")
            return np.nan
        return row['price'] * row['quantity'] * row['USDJPY']
    elif row['currency'] == 'JPY':
        if row['settlement_amount'] == 0 and row['price'] * row['quantity'] > 0:
            return row['price'] * row['quantity']         
        elif row['currency'] == 'JPY':
            return row['settlement_amount']
    else:
        return -1 

# Calculate amount_jpy
df['amount_jpy'] = df.apply(convert_to_jpy, axis=1)

# Save cleaned and integrated data
df.to_csv(output_file, index=False)

# Print summary to identify missing or problematic data
print("Data summary after cleaning and integration:")
print(df.info())

print("\nMissing values:")
print(df.isnull().sum())

print("\nUnique values in key columns:")
for col in ['currency', 'transaction_type', 'account_type', 'security_code']:
    print(f"\n{col}:")
    print(df[col].value_counts(dropna=False))

print(f"\nCleaned and integrated data saved to {output_file}")

# Group data by security_code
df_grouped = df.groupby('security_code').agg({
    'quantity': 'sum',
    'price': 'mean',
    'settlement_amount': 'sum',
    'amount_jpy': 'sum'
}).reset_index()

# Save grouped data
output_grouped_file = os.path.join(base_path, "grouped_trade_history.csv")
df_grouped.to_csv(output_grouped_file, index=False)

print(f"\nGrouped data saved to {output_grouped_file}")

