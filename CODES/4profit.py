import pandas as pd
from collections import defaultdict

def analyze_stock_transactions(trade_history_path, adj_close_data_path):
    # データの読み込み
    trade_history = pd.read_csv(trade_history_path, parse_dates=['trade_date'])
    print(trade_history.columns)
    adj_close_data = pd.read_csv(adj_close_data_path, index_col=0, parse_dates=True)

    # 結果を格納するための辞書
    results = defaultdict(lambda: {
        'buys': [], 'sells': [], 'total_profit': 0, 'total_loss': 0,
        'current_shares': 0, 'current_value': 0
    })

    # 取引履歴の処理
    for _, trade in trade_history.iterrows():
        security_code = trade['security_code']
        transaction_type = trade['transaction_type']
        # 'shares'の代わりに'quantity'を使用
        shares = trade['quantity']  # 'shares'を'quantity'に変更
        price = trade['price']
        amount = trade['amount_jpy']  # JPYの金額を使用

        if transaction_type == 'Buy':
            results[security_code]['buys'].append((shares, price))
            results[security_code]['current_shares'] += shares
        elif transaction_type == 'Sell':
            results[security_code]['sells'].append((shares, price))
            results[security_code]['current_shares'] -= shares

            # 利益/損失の計算の前に、'buys'が空でないことを確認
            if results[security_code]['buys']:
                avg_buy_price = sum(buy[0] * buy[1] for buy in results[security_code]['buys']) / sum(buy[0] for buy in results[security_code]['buys'])
                profit_loss = (price - avg_buy_price) * shares  # 価格はJPYで計算
                if profit_loss > 0:
                    results[security_code]['total_profit'] += profit_loss
                else:
                    results[security_code]['total_loss'] += abs(profit_loss)
            else:
                print(f"No buy transactions for {security_code}. Skipping profit/loss calculation.")

    # 現在の価値の計算
    for security_code, data in results.items():
        if security_code in adj_close_data.columns:
            current_price = adj_close_data[str(security_code)].iloc[-1]
            data['current_value'] = data['current_shares'] * current_price

    # 結果のDataFrameの作成
    df_results = pd.DataFrame([
        {
            'Security Code': security_code,
            'Buy Transactions': ', '.join([f"{shares}@{price:.2f}" for shares, price in data['buys']]),
            'Sell Transactions': ', '.join([f"{shares}@{price:.2f}" for shares, price in data['sells']]),
            'Total Profit/Loss': data['total_profit'] - data['total_loss'],  # 合計利益/損失を1つのカラムに
            'Current Shares': data['current_shares'],
            'Current Value': data['current_value']
        }
        for security_code, data in results.items()
    ])

    return df_results

# 使用例
trade_history_path = r'C:\Users\100ca\Documents\PyCode\TRADEHISTORY\trade_history4.csv'
adj_close_data_path = r'C:\Users\100ca\Documents\PyCode\trahist\DIC\charts.csv'

result_table = analyze_stock_transactions(trade_history_path, adj_close_data_path)
print(result_table)

# CSVファイルとして保存
output_path = r'C:\Users\100ca\Documents\PyCode\trahist\stock_transaction_analysis.csv'
result_table.to_csv(output_path, index=False)
print(f"結果を {output_path} に保存しました。")



result_table = analyze_stock_transactions(trade_history_path, adj_close_data_path)
print(result_table)

# CSVファイルとして保存
output_path = r'C:\Users\100ca\Documents\PyCode\trahist\stock_transaction_analysis.csv'
result_table.to_csv(output_path, index=False)
print(f"結果を {output_path} に保存しました。")

