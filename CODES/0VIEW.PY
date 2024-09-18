import pandas as pd
import os
import chardet

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
        return result['encoding']

def read_csv_head(folder_path):
    # フォルダ内のすべてのCSVファイルを検索
    for root, dirs, files in os.walk(folder_path):
        csv_files = [file for file in files if file.endswith('.csv')]
        
        # 最大5つのファイルを処理
        for file in csv_files[:5]:  # 最初の5つのファイルを対象
            file_path = os.path.join(root, file)
            print(f"Reading {file_path}...")
            # CSVファイルを読み込み、ヘッド15行を出力
            for start_row in range(21):  # 0行目から20行目まで試す
                try:
                    df = pd.read_csv(file_path, encoding='shift_jis', skiprows=start_row)
                    print(f"成功: {file_path} (エンコーディング: shift_jis, 開始行: {start_row})")
                    print("最初の15行:")
                    print(df.head(15))  # 最初の15行を表示
                    print("すべてのカラム:")
                    print(df.columns.tolist())  # すべてのカラムを表示
                    break  # 読み込み成功したらループを抜ける
                except:
                    continue  # エラーが発生した場合は次の行を試す

folder_path = r"C:\Users\100ca\Documents\PyCode\TRADEHISTORY\RAWDATA\rakuten"
read_csv_head(folder_path)

folder_path = r"C:\Users\100ca\Documents\PyCode\TRADEHISTORY\RAWDATA\sbi"
read_csv_head(folder_path)
