import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

def load_data(trade_history_path, adj_close_data_path):
    trade_history = pd.read_csv(trade_history_path, parse_dates=['trade_date'])
    adj_close_data = pd.read_csv(adj_close_data_path, index_col=0, parse_dates=True)
    return trade_history, adj_close_data

import matplotlib.pyplot as plt

def generate_combined_chart(trade_history, adj_close_data, security_code, output_folder):
    security_trades = trade_history[trade_history['security_code'] == security_code]
    adj_close = adj_close_data.get(str(security_code))
    if adj_close is None:
        
        print(f"No adjusted close data found for {security_code}")
        return

    fig, ax1 = plt.subplots(figsize=(14, 8))
    ax1.grid(True, linestyle='--', alpha=0.7)
    ax1.plot(adj_close.index, adj_close.values, color='tab:blue', label='Adjusted Close', linewidth=2)
    ax1.set_xlabel('Date', fontsize=12)
    ax1.set_ylabel('Adjusted Close Price', color='tab:blue', fontsize=12)

    ax2 = ax1.twinx()
    ax2.set_ylabel('Trade Amount (JPY)', color='tab:orange', fontsize=12)

    max_amount = security_trades['amount_jpy'].max()
    for _, trade in security_trades.iterrows():
        color, marker = ('g', '^') if trade['transaction_type'] == 'Buy' else ('r', 'v')
#color, marker = ('g', '^') if trade['transaction_type'] == 'Buy' else ('r', 'v') if trade['transaction_type'] == 'Sell' else ('k', 'o')
        size = 50 * (trade['amount_jpy'] / max_amount) + 20
        ax2.scatter(trade['trade_date'], trade['amount_jpy'], c=color, marker=marker, s=size, alpha=0.7, zorder=5)
        ax1.axvline(x=trade['trade_date'], color='gray', alpha=0.3, linestyle='--', zorder=1)

    plt.title(f'Adjusted Close and Trade Amount (JPY) for {security_code}', fontsize=16)
    fig.autofmt_xdate()

    ax2.scatter([], [], c='g', marker='^', s=100, alpha=0.7, label='Buy Trade')
    ax2.scatter([], [], c='r', marker='v', s=100, alpha=0.7, label='Sell Trade')
    ax1.legend(loc='upper left', fontsize=10).get_frame().set_alpha(0.7)

    plt.tight_layout()
    output_file = output_folder / f'{security_code}.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Chart for {security_code} saved to {output_file}")    
def main():
    trade_history_path = r'C:\Users\100ca\Documents\PyCode\TRADEHISTORY\trade_history3.csv'
    adj_close_data_path = r'C:\Users\100ca\Documents\PyCode\trahist\DIC\charts.csv'
    output_folder = Path(r'C:\Users\100ca\Documents\PyCode\trahist\charts')
    output_folder.mkdir(parents=True, exist_ok=True)

    trade_history, adj_close_data = load_data(trade_history_path, adj_close_data_path)

    # Get unique security codes
    security_codes = trade_history['security_code'].unique()

    for security_code in security_codes:
        generate_combined_chart(trade_history, adj_close_data, security_code, output_folder)

    print("All charts have been generated.")

if __name__ == "__main__":
    main()