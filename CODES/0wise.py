import pandas as pd
import os
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WiseDataCleaner:
    def __init__(self, input_file: str, output_folder: str):
        self.input_file = input_file
        self.output_folder = output_folder

    def read_data(self) -> pd.DataFrame:
        logger.info(f"Reading data from {self.input_file}")
        df = pd.read_csv(self.input_file)
        df = df[df['ステータス'] == 'COMPLETED'][df['送金の種類'] == 'NEUTRAL']
        return df

    def generate_security_code(self, from_currency: str, to_currency: str) -> str:
        # 通貨ペアの順序を定義（基準通貨が先）
        currency_pairs = {
            ('EUR', 'JPY'): 'EURJPY=X',
            ('USD', 'JPY'): 'USDJPY=X',
            ('EUR', 'USD'): 'EURUSD=X',
            ('JPY', 'EUR'): 'EURJPY=X',  # 逆順の場合も同じコードを使用
            ('JPY', 'USD'): 'USDJPY=X',  # 逆順の場合も同じコードを使用
            ('USD', 'EUR'): 'EURUSD=X',  # 逆順の場合も同じコードを使用
        }
        pair = (from_currency, to_currency)
        return currency_pairs.get(pair, f"{from_currency}{to_currency}=X")

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Cleaning and processing data")
        
        # Rename columns
        df = df.rename(columns={
            '完了日': 'trade_date',
            '為替レート': 'exchange_rate',
            '送金元通貨.1': 'from_currency',
            '受取通貨': 'to_currency',
            '送金額（手数料差し引き後）': 'from_amount',
            '受取額（手数料差し引き後）': 'to_amount'
        })

        # Convert date and numeric columns
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df['exchange_rate'] = pd.to_numeric(df['exchange_rate'], errors='coerce')
        df['from_amount'] = pd.to_numeric(df['from_amount'], errors='coerce')
        df['to_amount'] = pd.to_numeric(df['to_amount'], errors='coerce')

        # Create currency pair columns
        currency_pairs = ['EURJPY=X', 'USDJPY=X', 'EURUSD=X']
        for pair in currency_pairs:
            from_curr, to_curr = pair[:3], pair[3:6]
            df[pair] = df.apply(
                lambda row: row['exchange_rate'] if self.generate_security_code(row['from_currency'], row['to_currency']) == pair else None, 
                axis=1
            )

        # Create transaction type column
        df['transaction_type'] = df.apply(lambda row: 'Buy' if row['to_currency'] in ['USD', 'EUR'] else 'Sell', axis=1)

        # Generate security code
        df['security_code'] = df.apply(lambda row: self.generate_security_code(row['from_currency'], row['to_currency']), axis=1)

        # Calculate amount in JPY
        df['amount_jpy'] = df.apply(self.calculate_jpy_amount, axis=1)

        logger.info(f"Columns after cleaning: {df.columns.tolist()}")

        return df

    def calculate_jpy_amount(self, row):
        if row['to_currency'] == 'JPY':
            return row['to_amount']
        elif row['from_currency'] == 'JPY':
            return row['from_amount']
        elif row['security_code'] == 'USDJPY=X':
            return row['to_amount'] * row['USDJPY=X'] if pd.notnull(row['USDJPY=X']) else None
        elif row['security_code'] == 'EURJPY=X':
            return row['to_amount'] * row['EURJPY=X'] if pd.notnull(row['EURJPY=X']) else None
        else:
            logger.warning(f"Unable to calculate JPY amount for row: {row}")
            return None

    def save_data(self, df: pd.DataFrame):
        # Filter out rows where amount_jpy is NA
        df = df.dropna(subset=['amount_jpy'])

        # Remove columns where all values are NA
        df = df.dropna(how='all', axis=1)

        # Select and reorder columns
        base_columns = ['trade_date', 'security_code', 'from_currency', 'to_currency', 'from_amount', 'to_amount', 
                        'exchange_rate', 'transaction_type', 'amount_jpy']
        extra_columns = [col for col in df.columns if col not in base_columns]
        columns = base_columns + extra_columns

        df = df[columns]

        output_file = os.path.join(self.output_folder, f"cleaned_wise_data_{datetime.now().strftime('%Y%m%d')}.csv")
        df.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"Cleaned data saved to {output_file}")
        return df

    def process(self):
        df = self.read_data()
        cleaned_df = self.clean_data(df)
        final_df = self.save_data(cleaned_df)
        self.log_data_summary(final_df)

    def log_data_summary(self, df: pd.DataFrame):
        logger.info("\nData Summary:")
        logger.info(f"Total rows: {len(df)}")
        logger.info("\nSample data:")
        logger.info(df.head())
        logger.info("\nColumn info:")
        logger.info(df.info())
        logger.info("\nMissing values:")
        logger.info(df.isnull().sum())
        logger.info("\nUnique values in currency columns:")
        for col in ['from_currency', 'to_currency', 'security_code']:
            logger.info(f"{col}: {df[col].unique()}")
        logger.info("\nTotal amount in JPY:")
        logger.info(df['amount_jpy'].sum())

def main():
    base_path = r"C:\Users\100ca\Documents\PyCode\TRADEHISTORY"
    input_file = os.path.join(base_path, "RAWDATA", "wise", "wizefx.csv")
    output_folder = os.path.join(base_path, "RAWDATA","wise")
    
    cleaner = WiseDataCleaner(input_file, output_folder)
    cleaner.process()

if __name__ == "__main__":
    main()