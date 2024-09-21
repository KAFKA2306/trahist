import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf
from datetime import datetime

# Load the data
df = pd.read_csv(r'C:\Users\100ca\Documents\PyCode\TRADEHISTORY\RAWDATA\wise\cleaned_wise_data_20240921.csv', parse_dates=['trade_date'])

# Filter for EURJPY=X transactions
eurjpy = df[df['security_code'] == 'EURJPY=X']

# Download historical data from Yahoo Finance
eurjpy_yf = yf.download('EURJPY=X', start='2024-01-01', end=datetime.today())

# Create the plot
plt.figure(figsize=(12, 6))

# Plot Yahoo Finance data
plt.plot(eurjpy_yf.index, eurjpy_yf['Close'], label='Yahoo Finance Exchange Rate')

# Color-code buy and sell transactions
colors = eurjpy['transaction_type'].map({'Buy': 'g', 'Sell': 'r'})
plt.scatter(eurjpy['trade_date'], eurjpy['exchange_rate'], c=colors, label='Sell')

# Add vertical lines for August 2024
plt.axvline(pd.Timestamp('2024-08-01'), color='k', linestyle='--')

# Add horizontal lines with specific ranges
plt.hlines(y=142, xmin=pd.Timestamp('2024-01-01'), xmax=pd.Timestamp('2024-08-01'), color='g', linestyle='--', label='Buy at 142 (until Aug 2024)')
plt.hlines(y=161, xmin=pd.Timestamp('2024-08-01'), xmax=pd.Timestamp('2024-12-31'), color='r', linestyle='--', label='Sell at 161 (from Aug 2024)')

plt.title('EURJPY=X')
plt.xlabel('Date')
plt.ylabel('Exchange Rate')
plt.legend(loc='best')  # Adjust legend location

plt.grid(True)
plt.tight_layout()

# Ensure the save path exists
path = r'C:\Users\100ca\Documents\PyCode\trahist\charts\EURJPY=X'
plt.savefig(path)
plt.show()
