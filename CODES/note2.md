# CSV ファイル統合プラン

## 1. ファイルの概要

1. `tradehistory(INVST)_20240919.csv`: 投資信託の取引履歴
2. `tradehistory(JP)_20240919.csv`: 日本株の取引履歴
3. `tradehistory(US)_20240919.csv`: 米国株の取引履歴
4. `SaveFile_000001_000122.csv`: SBI証券の取引履歴
5. `yakujo20240919001827.csv`: SBI証券の約定履歴（海外株式）

## 2. 共通カラムの特定と標準化

すべてのファイルに共通する、または類似する重要なカラムを特定し、標準化します：

1. 取引日（約定日）: 'trade_date'
2. 決済日（受渡日）: 'settlement_date'
3. 銘柄名: 'security_name'
4. 銘柄コード / ティッカー: 'security_code'
5. 取引タイプ（買い/売り）: 'transaction_type'
6. 数量: 'quantity'
7. 価格: 'price'
8. 通貨: 'currency'
9. 決済金額: 'settlement_amount'
10. 口座タイプ: 'account_type'

## 3. ファイル別の処理方法

### 3.1. tradehistory(INVST)_20240919.csv

- エンコーディング: shift_jis
- 開始行: 0
- 主要カラム: ['約定日', '受渡日', 'ファンド名', '取引', '数量［口］', '単価', '受渡金額/(ポイント利用)[円]', '決済通貨']
- 変換:
  - '約定日' → 'trade_date'
  - '受渡日' → 'settlement_date'
  - 'ファンド名' → 'security_name'
  - '取引' → 'transaction_type'
  - '数量［口］' → 'quantity'
  - '単価' → 'price'
  - '受渡金額/(ポイント利用)[円]' → 'settlement_amount'
  - '決済通貨' → 'currency'
  - '口座' → 'account_type'

### 3.2. tradehistory(JP)_20240919.csv

- エンコーディング: shift_jis
- 開始行: 0
- 主要カラム: ['約定日', '受渡日', '銘柄コード', '銘柄名', '売買区分', '数量［株］', '単価［円］', '受渡金額［円］', '口座区分']
- 変換:
  - '約定日' → 'trade_date'
  - '受渡日' → 'settlement_date'
  - '銘柄コード' → 'security_code'
  - '銘柄名' → 'security_name'
  - '売買区分' → 'transaction_type'
  - '数量［株］' → 'quantity'
  - '単価［円］' → 'price'
  - '受渡金額［円］' → 'settlement_amount'
  - 'currency' を 'JPY' として追加
  - '口座区分' → 'account_type'

### 3.3. tradehistory(US)_20240919.csv

- エンコーディング: shift_jis
- 開始行: 0
- 主要カラム: ['約定日', '受渡日', 'ティッカー', '銘柄名', '売買区分', '数量［株］', '単価［USドル］', '受渡金額［円］', '口座']
- 変換:
  - '約定日' → 'trade_date'
  - '受渡日' → 'settlement_date'
  - 'ティッカー' → 'security_code'
  - '銘柄名' → 'security_name'
  - '売買区分' → 'transaction_type'
  - '数量［株］' → 'quantity'
  - '単価［USドル］' → 'price'
  - '受渡金額［円］' → 'settlement_amount'
  - 'currency' を 'USD' として追加
  - '口座' → 'account_type'

### 3.4. SaveFile_000001_000122.csv

- エンコーディング: shift_jis
- 開始行: 5
- 主要カラム: ['約定日', '銘柄', '銘柄コード', '取引', '約定数量', '約定単価', '受渡日', '受渡金額/決済損益', '預り']
- 変換:
  - '約定日' → 'trade_date'
  - '受渡日' → 'settlement_date'
  - '銘柄コード' → 'security_code'
  - '銘柄' → 'security_name'
  - '取引' → 'transaction_type'
  - '約定数量' → 'quantity'
  - '約定単価' → 'price'
  - '受渡金額/決済損益' → 'settlement_amount'
  - 'currency' を 'JPY' として追加
  - '預り' → 'account_type'

### 3.5. yakujo20240919001827.csv

- エンコーディング: shift_jis
- 開始行: 1
- 主要カラム: ['国内約定日', '通貨', '銘柄名', '取引', '約定数量', '約定単価', '国内受渡日', '受渡金額', '預り区分']
- 変換:
  - '国内約定日' → 'trade_date'
  - '国内受渡日' → 'settlement_date'
  - '銘柄名' → 'security_name'（ティッカーを抽出して 'security_code' に設定）
  - '取引' → 'transaction_type'
  - '約定数量' → 'quantity'
  - '約定単価' → 'price'
  - '受渡金額' → 'settlement_amount'
  - '通貨' → 'currency'
  - '預り区分' → 'account_type'

## 4. データ統合手順

1. 各ファイルを適切なエンコーディングと開始行で読み込む
2. 各ファイルのカラムを標準化されたカラム名に変換
3. 必要に応じて、データ型の変換を行う（例：日付をdatetime型に）
4. 各ファイルのデータフレームを連結（concat）
5. 重複するデータがないか確認し、必要に応じて削除
6. 結合されたデータフレームをソート（例：trade_dateで昇順）
7. 統合されたCSVファイルとして出力

## 5. 追加の考慮事項

- 日付形式の統一（例：すべてYYYY-MM-DD形式に）
- 通貨の取り扱い（円建てと米ドル建ての取引の区別）
- 取引タイプの標準化（例：「買付」「買い」「Buy」をすべて「buy」に統一）
- 不足しているデータの取り扱い（NaN値の処理）
- 大量のデータを扱う場合のメモリ管理（必要に応じてchunk処理を検討）

## 6. 出力ファイル形式

- ファイル名：integrated_trade_history_YYYYMMDD.csv
- エンコーディング：UTF-8
- 区切り文字：カンマ（,）
- 日付形式：YYYY-MM-DD

この統合プランに従うことで、5つの異なるCSVファイルから一貫性のある単一の取引履歴データセットを作成できます。