import pandas as pd
path = r"C:\Users\100ca\Downloads\data_j.xls"
import chardet

with open(path, 'rb') as file:
    raw_data = file.read()
    result = chardet.detect(raw_data)
    encoding = result['encoding']

print(f"Detected encoding: {encoding}")

df = pd.read_excel(path)#, en0coding='IBM855')

filtered_df = df[df['17業種コード'] == '-'][['コード', '銘柄名']]
filtered_df.to_csv(r'C:\Users\100ca\Documents\PyCode\trahist\DIC\jpxcodes.csv', index=False)


df2 = pd.read_csv(r'C:\Users\100ca\Documents\PyCode\trahist\DIC\jpxcodesus.csv')

