import pandas as pd
import yfinance as yf
import re
from datetime import datetime, timedelta

def process_code(x):
    if pd.notna(x):
        x = str(x).strip()
        return f"{x}.T" if x.isdigit() or re.match(r'\d+A', x) else x
    return None

try:
    df = pd.read_csv(r'C:\Users\100ca\Documents\PyCode\TRADEHISTORY\trade_history3.csv')
    codes = df['security_code'].apply(process_code).dropna().unique()
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365*5)  # 5年分のデータを取得
    
    data = yf.download(list(codes), start=start_date, end=end_date, group_by='ticker')
    
    # Adj Closeのデータを抽出
    adj_close_data = data.xs('Adj Close', axis=1, level=1, drop_level=True)
    
    # 列名を証券コードに変更（.Tを削除）
    adj_close_data.columns = adj_close_data.columns.str.rstrip('.T')
    
    adj_close_data.to_csv(r'C:\Users\100ca\Documents\PyCode\trahist\DIC\charts.csv')
    print(f"Processed {len(adj_close_data.columns)} codes. Sample columns: {list(adj_close_data.columns)[:10]}")
    print(f"Failed downloads: {set(codes) - set(adj_close_data.columns)}")
except Exception as e:
    print(f"An error occurred: {str(e)}")
    if 'data' in locals():
        print("Data structure:", data.columns.levels)
        print("Available columns:", data.columns.get_level_values(1).unique())
        
        
        
