
# TRADEHISTORY プロジェクト README

## 概要
このプロジェクトは、異なる取引履歴のCSVファイルを統合し、データをクリーンアップし、EDA（探索的データ分析）を行うための一連のスクリプトを提供します。最終的には、取引履歴データを分析し、視覚化することを目的としています。

## ディレクトリ構成
```
TRADEHISTORY/
├── CODES/
│   ├── 0fx.py               # 為替データをダウンロードするスクリプト
│   ├── 0VIEW.PY             # CSVファイルのヘッダーを表示するスクリプト
│   ├── 1concat.py           # CSVファイルを統合するスクリプト
│   ├── 2clean.py            # データをクリーンアップするスクリプト
│   ├── 3eda.py              # EDAを実施するスクリプト
├── DIC/
│   ├── forex_data.csv       # ダウンロードした為替データ
│   ├── securitycode.csv      # 銘柄コードデータ
├── RAWDATA/                 # 生データを格納するフォルダ
│   ├── rakuten/             # 楽天の取引データ
│   ├── sbi/                 # SBI証券の取引データ
├── note.md                  # CSVファイル統合プランの概要
└── note2.md                 # CSVファイル統合プランの詳細
```

## 必要なライブラリ
このプロジェクトを実行するには、以下のPythonライブラリが必要です。
- pandas
- numpy
- matplotlib
- seaborn
- yfinance
- chardet

これらは、以下のコマンドでインストールできます。
```bash
pip install pandas numpy matplotlib seaborn yfinance chardet
```

## 使用方法

### 1. 為替データのダウンロード
`0fx.py`を実行して、指定した通貨ペアの為替データをダウンロードします。
```bash
python CODES/0fx.py
```

### 2. CSVファイルの統合
`1concat.py`を実行して、複数の取引履歴CSVファイルを統合します。
```bash
python CODES/1concat.py
```

### 3. データのクリーンアップ
`2clean.py`を実行して、統合されたデータをクリーンアップします。
```bash
python CODES/2clean.py
```

### 4. EDAの実施
`3eda.py`を実行して、クリーンアップされたデータに対して探索的データ分析を行います。
```bash
python CODES/3eda.py
```


## 出力ファイル
- 統合された取引履歴データは、`integrated_trade_history_YYYYMMDD.csv`として保存されます。
- クリーンアップされたデータは、`trade_history3.csv`として保存されます。
- EDAの結果や視覚化は、`EDA_results`フォルダに保存されます。

## 注意事項
- 各スクリプトは、適切なフォルダパスを指定する必要があります。特に、`RAWDATA`フォルダ内に取引データが存在することを確認してください。
