import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import logging

# ロギングの設定
logging.basicConfig(filename='plot_log.txt', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def normalize_code(code):
    if pd.isna(code):
        return None
    code = str(code).strip().upper()
    return code.rstrip('.JP').rstrip('.T')

def load_data(trade_history_path, adj_close_data_path):
    trade_history = pd.read_csv(trade_history_path, parse_dates=['trade_date'])
    trade_history['security_code'] = trade_history['security_code'].apply(normalize_code)
    
    adj_close_data = pd.read_csv(adj_close_data_path, index_col=0, parse_dates=True)
    adj_close_data.columns = [normalize_code(col) for col in adj_close_data.columns]
    
    return trade_history, adj_close_data

def generate_combined_chart(trade_history, adj_close_data, security_code, output_folder):
    security_trades = trade_history[trade_history['security_code'] == security_code]
    adj_close = adj_close_data.get(security_code)
    
    if adj_close is None or adj_close.empty:
        logging.warning(f"No adjusted close data found for {security_code}")
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
        color = 'g' if trade['transaction_type'].lower() == 'buy' else 'r'
        marker = '^' if trade['transaction_type'].lower() == 'buy' else 'v'
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
    logging.info(f"Chart for {security_code} saved to {output_file}")

def main():
    trade_history_path = r'C:\Users\100ca\Documents\PyCode\TRADEHISTORY\trade_history4.csv'
    adj_close_data_path = r'C:\Users\100ca\Documents\PyCode\trahist\DIC\charts.csv'
    output_folder = Path(r'C:\Users\100ca\Documents\PyCode\trahist\charts')
    output_folder.mkdir(parents=True, exist_ok=True)

    trade_history, adj_close_data = load_data(trade_history_path, adj_close_data_path)

    security_codes = trade_history['security_code'].dropna().unique()

    for security_code in security_codes:
        try:
            generate_combined_chart(trade_history, adj_close_data, security_code, output_folder)
        except Exception as e:
            logging.error(f"Error generating chart for {security_code}: {str(e)}")

    logging.info("All charts have been generated.")
    print("All charts have been generated. Check plot_log.txt for details.")

if __name__ == "__main__":
    main()