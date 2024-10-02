import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

def process_code(x):
    if pd.notna(x):
        x = str(x).strip().upper()
        if x.isdigit():
            return f"{x}.T"
        elif x.endswith('.JP'):
            return f"{x[:-3]}.T"
        elif x.endswith('.US'):
            return x[:-3]
        else:
            return x
    return None

# CSVファイルを読み込み、銘柄コードを処理
df = pd.read_csv(r'C:\Users\100ca\Documents\PyCode\TRADEHISTORY\trade_history4.csv')
codes = df['security_code'].apply(process_code).dropna().unique().tolist()

# 日付範囲を設定（今日から5年前まで）
end_date = datetime.now()
print(end_date)
start_date = end_date - timedelta(days=365*5)

# データをダウンロード（エラーを無視）
data = yf.download(codes, start=start_date, end=end_date, ignore_tz=True, group_by='column')

# Adj Closeのデータを抽出
adj_close_data = data['Adj Close'].copy()

# 列名から'.T'を削除し、NaNのみの列を削除
adj_close_data.columns = adj_close_data.columns.str.rstrip('.T')
adj_close_data = adj_close_data.dropna(axis=1, how='all')


print(adj_close_data.tail(1))


# CSVファイルに保存
output_path = r'C:\Users\100ca\Documents\PyCode\trahist\DIC\charts.csv'
print(adj_close_data)
print(f"処理された銘柄数: {len(adj_close_data.columns)}")
print(f"サンプル列: {list(adj_close_data.columns)[:10]}")
print(f"ダウンロードに失敗した銘柄: {set(codes) - set(adj_close_data.columns)}")
print(f"データは {output_path} に保存されました。")


adj_close_data.to_csv(output_path)