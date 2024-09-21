import pandas as pd
import os
from datetime import datetime
import logging
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TradeDataIntegrator:
    def __init__(self, base_path):
        self.base_path = base_path
        self.output_folder = os.path.join(base_path)
        os.makedirs(self.output_folder, exist_ok=True)
        self.common_columns = ['trade_date', 'settlement_date', 'security_code', 'security_name',
                               'transaction_type', 'quantity', 'price', 'settlement_amount',
                               'currency', 'account_type', 'data_source']

    def standardize_date(self, date_str):
        if pd.isna(date_str):
            return None
        date_formats = ['%Y/%m/%d', '%Y年%m月%d日', '%Y-%m-%d', '%y/%m/%d']
        for fmt in date_formats:
            try:
                return pd.to_datetime(date_str, format=fmt)
            except ValueError:
                continue
        logger.warning(f"Unable to parse date: {date_str}")
        return pd.NaT

    def clean_numeric(self, value):
        if pd.isna(value):
            return None
        if isinstance(value, (int, float)):
            return value
        value = str(value).replace(',', '').replace('(', '').replace(')', '')
        try:
            return float(value)
        except ValueError:
            logger.warning(f"Unable to convert to number: {value}")
            return None

    def read_wise_data(self):
        wise_file = os.path.join(self.base_path, "RAWDATA", "wise", "cleaned_wise_data_20240921.csv")
        logger.info(f"Reading Wise data from {wise_file}")
        df = pd.read_csv(wise_file)
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df['data_source'] = 'wise'
        return df

    def read_rakuten_csv(self, file_path):
        logger.info(f"Processing Rakuten file: {file_path}")
        df = pd.read_csv(file_path, encoding='shift-jis')
        file_name = os.path.basename(file_path)
        
        if 'INVST' in file_name:
            df = df.rename(columns={
                '約定日': 'trade_date', '受渡日': 'settlement_date', 'ファンド名': 'security_name',
                '取引': 'transaction_type', '数量［口］': 'quantity', '単価': 'price',
                '受渡金額/(ポイント利用)[円]': 'settlement_amount', '決済通貨': 'currency', '口座': 'account_type'
            })
            df['security_code'] = ''
        elif 'JP' in file_name:
            df = df.rename(columns={
                '約定日': 'trade_date', '受渡日': 'settlement_date', '銘柄コード': 'security_code',
                '銘柄名': 'security_name', '売買区分': 'transaction_type', '数量［株］': 'quantity',
                '単価［円］': 'price', '受渡金額［円］': 'settlement_amount', '口座区分': 'account_type'
            })
            df['currency'] = 'JPY'
        elif 'US' in file_name:
            df = df.rename(columns={
                '約定日': 'trade_date', '受渡日': 'settlement_date', 'ティッカー': 'security_code',
                '銘柄名': 'security_name', '売買区分': 'transaction_type', '数量［株］': 'quantity',
                '単価［USドル］': 'price', '受渡金額［円］': 'settlement_amount', '口座': 'account_type'
            })
            df['currency'] = 'USD'
        else:
            raise ValueError(f"Unknown Rakuten file format: {file_name}")
        
        df['data_source'] = f'rakuten_{file_name}'
        df['trade_date'] = df['trade_date'].apply(self.standardize_date)
        df['settlement_date'] = df['settlement_date'].apply(self.standardize_date)
        df['settlement_amount'] = df['settlement_amount'].apply(self.clean_numeric)
        df['price'] = df['price'].apply(self.clean_numeric)
        df['quantity'] = df['quantity'].apply(self.clean_numeric)
        return df[self.common_columns]

    def read_sbi_csv(self, file_path):
        logger.info(f"Processing SBI file: {file_path}")
        file_name = os.path.basename(file_path)
        if 'SaveFile' in file_name:
            df = pd.read_csv(file_path, encoding='shift-jis', skiprows=8)
            df = df.rename(columns={
                '約定日': 'trade_date', '受渡日': 'settlement_date', '銘柄コード': 'security_code',
                '銘柄': 'security_name', '取引': 'transaction_type', '約定数量': 'quantity',
                '約定単価': 'price', '受渡金額/決済損益': 'settlement_amount', '預り': 'account_type'
            })
            df['currency'] = 'JPY'
        elif 'yakujo' in file_name:
            df = pd.read_csv(file_path, encoding='shift-jis', skiprows=2)
            df = df.rename(columns={
                '国内約定日': 'trade_date', '国内受渡日': 'settlement_date', '銘柄名': 'security_name',
                '取引': 'transaction_type', '約定数量': 'quantity', '約定単価': 'price',
                '受渡金額': 'settlement_amount', '通貨': 'currency', '預り区分': 'account_type'
            })
            df['security_code'] = df['security_name'].str.extract(r'(\w+) / ')
        else:
            raise ValueError(f"Unknown SBI file format: {file_name}")
        
        df['data_source'] = f'sbi_{file_name}'
        df['trade_date'] = df['trade_date'].apply(self.standardize_date)
        df['settlement_date'] = df['settlement_date'].apply(self.standardize_date)
        df['settlement_amount'] = df['settlement_amount'].apply(self.clean_numeric)
        df['price'] = df['price'].apply(self.clean_numeric)
        df['quantity'] = df['quantity'].apply(self.clean_numeric)
        return df[self.common_columns]

    def process_data(self):
        wise_df = self.read_wise_data()
        all_data = [wise_df]

        for folder in ['rakuten', 'sbi']:
            folder_path = os.path.join(self.base_path, "RAWDATA", folder)
            for file in os.listdir(folder_path):
                if file.endswith('.csv'):
                    file_path = os.path.join(folder_path, file)
                    try:
                        if folder == 'rakuten':
                            df = self.read_rakuten_csv(file_path)
                        else:
                            df = self.read_sbi_csv(file_path)
                        all_data.append(df)
                    except Exception as e:
                        logger.error(f"Error processing file {file_path}: {str(e)}")

        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df = combined_df.sort_values('trade_date')
        return combined_df

    def save_data(self, df):
        output_file = os.path.join(self.output_folder, f"integrated_trade_history_{datetime.now().strftime('%Y%m%d')}.csv")
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(df)
        print(output_file)
        logger.info(f"Integrated data saved to {output_file}")

    def log_data_summary(self, df):
        logger.info("\nData Summary:")
        logger.info(f"Total rows: {len(df)}")
        logger.info("\nSample data:")
        logger.info(df.head())
        logger.info("\nColumn info:")
        logger.info(df.info())
        logger.info("\nMissing values:")
        logger.info(df.isnull().sum())
        logger.info("\nUnique values in key columns:")
        for col in ['security_code', 'transaction_type', 'currency', 'data_source']:
            logger.info(f"{col}: {df[col].unique()}")
        logger.info("\nTotal amount in JPY:")
        jpy_amount = df[(df['currency'] == 'JPY') & (df['settlement_amount'].notnull())]['settlement_amount'].sum()
        logger.info(f"Total JPY amount: {jpy_amount}")

def main():
    base_path = r"C:\Users\100ca\Documents\PyCode\TRADEHISTORY"
    integrator = TradeDataIntegrator(base_path)
    combined_df = integrator.process_data()
    integrator.save_data(combined_df)
    integrator.log_data_summary(combined_df)

if __name__ == "__main__":
    main()
    print("finish")
    
    
# 失敗中です。
