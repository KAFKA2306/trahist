import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import numpy as np
import yfinance as yf

# File path
cleaned_file = r'C:\Users\100ca\Documents\PyCode\TRADEHISTORY\trade_history4.csv'

# Output folder
output_folder = r'C:\Users\100ca\Documents\PyCode\TRADEHISTORY\EDA_results'
os.makedirs(output_folder, exist_ok=True)

# Load data
df = pd.read_csv(cleaned_file)

# Convert date columns to datetime
df['trade_date'] = pd.to_datetime(df['trade_date'])
df['settlement_date'] = pd.to_datetime(df['settlement_date'])

# Convert trade_date to month for grouping
df['month'] = df['trade_date'].dt.to_period('M')  # 'month'列を追加

# Display basic information
print("Dataset information:")
print(df.info())

# Display summary statistics
print("\nSummary statistics:")
print(df.describe())

# Save summary statistics to CSV
df.describe().to_csv(os.path.join(output_folder, 'summary_statistics.csv'))

# Analyze missing values
print("\nMissing values:")
missing_values = df.isnull().sum()
print(missing_values)
missing_values.to_csv(os.path.join(output_folder, 'missing_values.csv'))

# Visualization

# 1. Distribution of transaction types
plt.figure(figsize=(10, 6))
sns.countplot(x='transaction_type', data=df)
plt.title('Distribution of Transaction Types (Buy/Sell)')
plt.xlabel('Transaction Type')
plt.ylabel('Count')
plt.tight_layout()
plt.savefig(os.path.join(output_folder, 'transaction_types_distribution.png'))
plt.close()

# 2. Distribution of account types
plt.figure(figsize=(10, 6))
df['account_type'].value_counts().plot(kind='bar')
plt.title('Distribution of Account Types')
plt.xlabel('Account Type')
plt.ylabel('Count')
plt.tight_layout()
plt.savefig(os.path.join(output_folder, 'account_types_distribution.png'))
plt.close()

# 3. Distribution of currencies
plt.figure(figsize=(8, 8))
df['currency'].value_counts().plot(kind='pie', autopct='%1.1f%%')
plt.title('Distribution of Currencies')
plt.ylabel('')
plt.tight_layout()
plt.savefig(os.path.join(output_folder, 'currency_distribution.png'))
plt.close()

# 4. Time series of transaction amounts with moving average and color-coded by transaction type
plt.figure(figsize=(12, 6))
weekly_data = df.resample('W-Mon', on='trade_date').agg(
    buy_amount=('amount_jpy', lambda x: x[df.loc[x.index, 'transaction_type'] == 'buy'].sum()),
    sell_amount=('amount_jpy', lambda x: x[df.loc[x.index, 'transaction_type'] == 'sell'].sum())
)

# TOPIX Total Marketのデータを取得
topix_data = yf.download("^TOPX", start=df['trade_date'].min(), end=pd.Timestamp.today().strftime('%Y-%m-%d'))
topix_data['weekly_return'] = topix_data['Close'].pct_change().rolling(window=7).mean()  # 7日間の移動平均リターンを計算

# Merge TOPIX data with weekly_data
weekly_data = weekly_data.join(topix_data['weekly_return'], how='outer')

# Plotting for weekly transaction amounts with TOPIX
plt.bar(weekly_data.index, weekly_data['buy_amount'], label='Buy', alpha=0.5, color='blue')
plt.bar(weekly_data.index, weekly_data['sell_amount'], label='Sell', alpha=0.5, color='red')
plt.plot(weekly_data.index, weekly_data['buy_amount'].rolling(window=7).mean(), label='Buy Moving Avg', linewidth=2, color='darkblue')
plt.plot(weekly_data.index, weekly_data['sell_amount'].rolling(window=7).mean(), label='Sell Moving Avg', linewidth=2, color='darkred')
plt.plot(weekly_data.index, weekly_data['weekly_return'], label='TOPIX Weekly Return', linewidth=2, color='green')  # TOPIXをプロット

plt.title('Weekly Transaction Amounts Over Time with Moving Average and TOPIX')
plt.xlabel('Date')
plt.ylabel('Total Transaction Amount (JPY)')
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(output_folder, 'weekly_transaction_amounts_moving_avg_with_topix.png'))
plt.close()

# 5. Daily Transaction Counts with TOPIX
daily_counts = df.groupby(df['trade_date'].dt.date).size()
plt.figure(figsize=(12, 6))
plt.plot(daily_counts.index, daily_counts, label='Daily Transaction Counts', color='blue')
plt.plot(daily_counts.index, daily_counts.rolling(window=7).mean(), label='Daily Moving Avg', linewidth=2, color='darkblue')
plt.plot(topix_data.index, topix_data['weekly_return'].rolling(window=7).mean(), label='TOPIX Weekly Return', linewidth=2, color='green')  # TOPIXをプロット
plt.title('Daily Transaction Counts with TOPIX')
plt.xlabel('Date')
plt.ylabel('Transaction Count')
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(output_folder, 'daily_transaction_counts_with_topix.png'))
plt.close()

# 6. Monthly Transaction Counts with TOPIX
monthly_counts = df.groupby('month').size()  # 'month'列を使用して集計
plt.figure(figsize=(12, 6))
plt.bar(monthly_counts.index.astype(str), monthly_counts, label='Monthly Transaction Counts', alpha=0.5, color='blue')
plt.plot(monthly_counts.index.astype(str), monthly_counts.rolling(window=3).mean(), label='Monthly Moving Avg', linewidth=2, color='darkblue')
topix_monthly = topix_data['weekly_return'].resample('M').mean()  # TOPIXを月ごとにリサンプリング
plt.plot(topix_monthly.index.astype(str), topix_monthly.rolling(window=3).mean(), label='TOPIX Monthly Return', linewidth=2, color='green')  # TOPIXをプロット
plt.title('Monthly Transaction Counts with TOPIX')
plt.xlabel('Month')
plt.ylabel('Transaction Count')
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(output_folder, 'monthly_transaction_counts_with_topix.png'))
plt.close()

# 7. Boxplot of Transaction Amounts by Transaction Type with TOPIX
plt.figure(figsize=(12, 6))
sns.boxplot(x='transaction_type', y='amount_jpy', data=df)
plt.title('Distribution of Transaction Amounts by Transaction Type with TOPIX')
plt.xlabel('Transaction Type')
plt.ylabel('Transaction Amount (JPY)')
plt.axhline(y=topix_data['weekly_return'].mean(), color='green', linestyle='--', label='TOPIX Weekly Return')  # TOPIXをプロット
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(output_folder, 'transaction_amounts_by_type_with_topix.png'))
plt.close()

# 8. Transaction Amounts by Security Code with TOPIX
plt.figure(figsize=(12, 6))
top_10_securities = df.groupby('security_code')['amount_jpy'].sum().sort_values(ascending=False).head(10)
top_10_securities.plot(kind='bar', color='blue', label='Transaction Amounts')
plt.title('Top 10 Securities by Transaction Amount with TOPIX')
plt.xlabel('Security Code')
plt.ylabel('Total Transaction Amount (JPY)')
plt.xticks(rotation=45, ha='right')

# Add TOPIX data to the plot
plt.plot(top_10_securities.index, topix_data['weekly_return'].rolling(window=3).mean().head(10), label='TOPIX Weekly Return', linewidth=2, color='green')  # TOPIXをプロット

plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(output_folder, 'transaction_amounts_by_security_code_with_topix.png'))
plt.close()

# Calculate total investment amount
total_investment = df['amount_jpy'].sum()
print(f"\nTotal Investment Amount (JPY): {total_investment}")

# Save total investment amount to a text file
with open(os.path.join(output_folder, 'total_investment_amount.txt'), 'w') as f:
    f.write(f"Total Investment Amount (JPY): {total_investment}")

# Calculate cumulative investment amount from start to today
cumulative_investment = df[df['trade_date'] <= pd.Timestamp.today()]['amount_jpy'].sum()
print(f"\nCumulative Investment Amount from Start to Today (JPY): {cumulative_investment}")

# Save cumulative investment amount to a text file
with open(os.path.join(output_folder, 'cumulative_investment_amount.txt'), 'w') as f:
    f.write(f"Cumulative Investment Amount from Start to Today (JPY): {cumulative_investment}")

print(f"\nEDA completed. Results and visualizations saved in {output_folder}.")

import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt

# 取引データの読み込み
df = pd.read_csv(r'C:\Users\100ca\Documents\PyCode\TRADEHISTORY\trade_history4.csv')

# security_codeごとの取引額を計算
top_securities = df.groupby('security_code')['amount_jpy'].sum().nlargest(5).index.tolist()

# 各銘柄のデータを取得
data = {}
for code in top_securities:
    data[code] = yf.download(code, start='2022-01-01', end='2023-01-01')

# 追加: security_codeごとのデータを整形して出力
formatted_data = pd.DataFrame()
for code in top_securities:
    if code in data:
        formatted_data[code] = data[code]['Close']

# 整形したデータをCSVとして保存
formatted_data.to_csv(os.path.join(output_folder, 'security_code_data.csv'))

# プロット
plt.figure(figsize=(14, 7))
for code in top_securities:
    plt.plot(data[code].index, data[code]['Close'], label=code)

plt.title('取引タイミングのプロット (トップ5銘柄)')
plt.xlabel('日付')
plt.ylabel('終値 (JPY)')
plt.legend()
plt.grid()


import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt
import os

# 1. データの読み込み
file_path = r'C:\Users\100ca\Documents\PyCode\TRADEHISTORY\trade_history4.csv'
df = pd.read_csv(file_path)
print("データの読み込みが完了しました。")

# 2. 取引額の計算
top_securities = df.groupby('security_code')['amount_jpy'].sum().sort_values(ascending=False).head(5).index.tolist()
print(f"トップ5の銘柄コード: {top_securities}")

# 3. データの取得
data = {}
start_date = '2018-01-01'
end_date = '2025-01-01'
for code in top_securities:
    try:
        data[code] = yf.download(code, start=start_date, end=end_date)
    except Exception as e:
        print(f"{code}のデータ取得中にエラーが発生しました: {e}")
print("データの取得が完了しました。")

# 4. プロットの作成
plt.figure(figsize=(14, 7))
for code in top_securities:
    if code in data and not data[code].empty:  # データが取得できた銘柄のみプロット
        plt.plot(data[code].index, data[code]['Close'], label=code)
plt.title('取引タイミングのプロット (トップ5銘柄)')
plt.xlabel('日付')
plt.ylabel('終値 (JPY)')
plt.legend()
plt.grid(True)
plot_path = r'C:\Users\100ca\Documents\PyCode\TRADEHISTORY\plots\top5_securities_plot.png'
# 追加: プロット保存先のディレクトリを作成
os.makedirs(os.path.dirname(plot_path), exist_ok=True)
plt.savefig(plot_path)
print("プロットを保存しました。")


# 6. エラーハンドリング
try:
    df = pd.read_csv(file_path)
except Exception as e:
    print(f"データの読み込み中にエラーが発生しました: {e}")

# 7. EDAの実施
eda_results_path = r'C:\Users\100ca\Documents\PyCode\TRADEHISTORY\EDA_results'
os.makedirs(eda_results_path, exist_ok=True)  # EDA結果保存先のディレクトリを作成
df.info()
df.describe().to_csv(f'{eda_results_path}\summary_statistics.csv')
df.isnull().sum().to_csv(f'{eda_results_path}\missing_values.csv')

# 取引タイプ、アカウントタイプ、通貨の分布を視覚化
df['transaction_type'].value_counts().plot(kind='bar', title='取引タイプの分布')
df['account_type'].value_counts().plot(kind='bar', title='アカウントタイプの分布')
df['currency'].value_counts().plot(kind='bar', title='通貨の分布')
