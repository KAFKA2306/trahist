import pandas as pd
import yfinance as yf
import re
from datetime import datetime, timedelta
import time
import logging

# ロギングの設定
logging.basicConfig(filename='download_log.txt', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def process_code(x):
    if pd.notna(x):
        x = str(x).strip()
        if x.isdigit() or re.match(r'\d+A', x):
            return f"{x}.JP"
        elif '.' not in x:
            return x
    return None

def download_data(code, start_date, end_date, max_retries=3):
    for attempt in range(max_retries):
        try:
            data = yf.download(code, start=start_date, end=end_date)
            if not data.empty:
                return data
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1} failed for {code}: {str(e)}")
            time.sleep(2)  # 2秒待機してから再試行
    logging.error(f"Failed to download data for {code} after {max_retries} attempts")
    return None

def main():
    try:
        df = pd.read_csv(r'C:\Users\100ca\Documents\PyCode\TRADEHISTORY\trade_history4.csv')
        codes = df['security_code'].apply(process_code).dropna().unique()
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365*5)  # 5年分のデータを取得
        
        all_data = {}
        failed_downloads = []

        for i, code in enumerate(codes):
            print(f"Downloading data for {code} ({i+1}/{len(codes)})")
            data = download_data(code, start_date, end_date)
            if data is not None and not data.empty:
                all_data[code] = data['Adj Close']
            else:
                failed_downloads.append(code)

        # データフレームの作成
        adj_close_data = pd.DataFrame(all_data)
        
        # .JPサフィックスの削除
        adj_close_data.columns = adj_close_data.columns.str.rstrip('.JP')
        
        # CSVファイルとして保存
        adj_close_data.to_csv(r'C:\Users\100ca\Documents\PyCode\trahist\DIC\charts.csv')
        
        print(f"Processed {len(adj_close_data.columns)} codes.")
        print(f"Failed downloads: {failed_downloads}")
        
        # 結果のログ出力
        logging.info(f"Successfully downloaded data for {len(adj_close_data.columns)} codes")
        logging.info(f"Failed downloads: {failed_downloads}")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()