import pandas as pd
import os
from datetime import datetime

def standardize_date(date_str):
    if pd.isna(date_str):
        return None
    date_formats = ['%Y/%m/%d', '%Y年%m月%d日', '%Y-%m-%d']
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    return date_str  # 変換できない場合は元の文字列を返す

def process_csv(file_path):
    file_name = os.path.basename(file_path)
    
    if 'INVST' in file_name:
        df = pd.read_csv(file_path, encoding='shift-jis')
        df = df.rename(columns={
            '約定日': 'trade_date',
            '受渡日': 'settlement_date',
            'ファンド名': 'security_name',
            '取引': 'transaction_type',
            '数量［口］': 'quantity',
            '単価': 'price',
            '受渡金額/(ポイント利用)[円]': 'settlement_amount',
            '決済通貨': 'currency',
            '口座': 'account_type'
        })
        df['security_code'] = ''
    elif 'JP' in file_name:
        df = pd.read_csv(file_path, encoding='shift-jis')
        df = df.rename(columns={
            '約定日': 'trade_date',
            '受渡日': 'settlement_date',
            '銘柄コード': 'security_code',
            '銘柄名': 'security_name',
            '売買区分': 'transaction_type',
            '数量［株］': 'quantity',
            '単価［円］': 'price',
            '受渡金額［円］': 'settlement_amount',
            '口座区分': 'account_type'
        })
        df['currency'] = 'JPY'
    elif 'US' in file_name:
        df = pd.read_csv(file_path, encoding='shift-jis')
        df = df.rename(columns={
            '約定日': 'trade_date',
            '受渡日': 'settlement_date',
            'ティッカー': 'security_code',
            '銘柄名': 'security_name',
            '売買区分': 'transaction_type',
            '数量［株］': 'quantity',
            '単価［USドル］': 'price',
            '受渡金額［円］': 'settlement_amount',
            '口座': 'account_type'
        })
        df['currency'] = 'USD'
    elif 'SaveFile' in file_name:
        df = pd.read_csv(file_path, encoding='shift-jis', skiprows=8)
        df = df.rename(columns={
            '約定日': 'trade_date',
            '受渡日': 'settlement_date',
            '銘柄コード': 'security_code',
            '銘柄': 'security_name',
            '取引': 'transaction_type',
            '約定数量': 'quantity',
            '約定単価': 'price',
            '受渡金額/決済損益': 'settlement_amount',
            '預り': 'account_type'
        })
        df['currency'] = 'JPY'
    elif 'yakujo' in file_name:
        df = pd.read_csv(file_path, encoding='shift-jis', skiprows=2)
        df = df.rename(columns={
            '国内約定日': 'trade_date',
            '国内受渡日': 'settlement_date',
            '銘柄名': 'security_name',
            '取引': 'transaction_type',
            '約定数量': 'quantity',
            '約定単価': 'price',
            '受渡金額': 'settlement_amount',
            '通貨': 'currency',
            '預り区分': 'account_type'
        })
        df['security_code'] = df['security_name'].str.extract(r'(\w+) / ')
    else:
        raise ValueError(f"Unknown file format: {file_name}")

    df['data_source'] = file_name
    
    columns_to_select = ['trade_date', 'settlement_date', 'security_code', 'security_name', 
                         'transaction_type', 'quantity', 'price', 'settlement_amount', 
                         'currency', 'account_type', 'data_source']
    
    for col in columns_to_select:
        if col not in df.columns:
            df[col] = None
    
    return df[columns_to_select]

def standardize_transaction_type(transaction_type):
    if pd.isna(transaction_type):
        return 'unknown'
    
    original_type = str(transaction_type)
    lower_type = original_type.lower()
    
    if any(word in lower_type for word in ['買', 'buy', '買付', '再投資','入庫']):
        return 'buy'
    elif any(word in lower_type for word in ['売', 'sell', '解約']):
        return 'sell'
    else:
        return original_type

def integrate_csv_files(folder_paths):
    all_data = []
    
    for folder_path in folder_paths:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.csv'):
                    file_path = os.path.join(root, file)
                    print(f"Processing {file_path}...")
                    try:
                        df = process_csv(file_path)
                        all_data.append(df)
                    except Exception as e:
                        print(f"Error processing {file_path}: {str(e)}")
    
    combined_df = pd.concat(all_data, ignore_index=True)
    
    combined_df['trade_date'] = combined_df['trade_date'].apply(standardize_date)
    combined_df['settlement_date'] = combined_df['settlement_date'].apply(standardize_date)
    
    combined_df['transaction_type'] = combined_df['transaction_type'].apply(standardize_transaction_type)
    
    combined_df = combined_df.sort_values('trade_date')
    
    output_file = fr"C:\Users\100ca\Documents\PyCode\TRADEHISTORY\integrated_trade_history_{datetime.now().strftime('%Y%m%d')}.csv"
    combined_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Integrated CSV file saved as: {output_file}")
    
    print("\nRows per data source:")
    print(combined_df['data_source'].value_counts())

# フォルダパスを指定
folder_paths = [
    r"C:\Users\100ca\Documents\PyCode\TRADEHISTORY\RAWDATA\rakuten",
    r"C:\Users\100ca\Documents\PyCode\TRADEHISTORY\RAWDATA\sbi"
]

# CSVファイルを統合して出力
integrate_csv_files(folder_paths)