import pandas as pd
import os

# Path settings
base_path = r"C:\Users\100ca\Documents\PyCode\TRADEHISTORY"
input_file = os.path.join(base_path, "trade_history3.csv")
security_code_file = os.path.join(base_path, "DIC", "securitycode2.csv")
output_file = os.path.join(base_path, "trade_history4.csv")

def load_data():
    try:
        df = pd.read_csv(input_file)
        security_code = pd.read_csv(security_code_file)
        return df, security_code
    except FileNotFoundError as e:
        print(f"Error: File not found - {e}")
        return None, None
    except pd.errors.EmptyDataError as e:
        print(f"Error: Empty file - {e}")
        return None, None

def replace_security_codes(df, security_code):
    # Create a dictionary for replacement
    code_dict = dict(zip(security_code['security_name'], security_code['security_code']))
    
    # Replace security_code based on security_name
    df['security_code'] = df['security_name'].map(code_dict).fillna(df['security_code'])
    
    return df

def main():
    df, security_code = load_data()
    if df is None or security_code is None:
        return

    print("Original data shape:", df.shape)
    print("Original unique security codes:", df['security_code'].nunique())

    df = replace_security_codes(df, security_code)

    print("Updated data shape:", df.shape)
    print("Updated unique security codes:", df['security_code'].nunique())

    # Save the updated DataFrame
    df.to_csv(output_file, index=False)
    print(f"Updated data saved to {output_file}")

if __name__ == "__main__":
    main()