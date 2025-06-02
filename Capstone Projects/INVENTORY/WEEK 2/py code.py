import pandas as pd
import numpy as np

try:
    df = pd.read_csv("stock_movements.csv")
except FileNotFoundError:
    print("🔥 File not found. Did you even export it?")
    exit()

df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
df = df.dropna(subset=['quantity'])
df['quantity'] = df['quantity'].astype(int)
df = df[df['movement_type'].isin(['IN', 'OUT'])]
df['quantity'] = df.apply(
    lambda row: -row['quantity'] if row['movement_type'] == 'OUT' else row['quantity'], axis=1
)
df['movement_date'] = pd.to_datetime(df['movement_date'], errors='coerce')
stock_summary = df.groupby('product_id')['quantity'].sum()
print("📦 Current Stock Levels:")
print(stock_summary)
reorder_thresholds = {
    1: 10,
    2: 15,
    3: 5,
    4: 12,
}
stock_df = stock_summary.reset_index().rename(columns={'quantity': 'current_stock'})
stock_df['reorder_level'] = stock_df['product_id'].map(reorder_thresholds)
stock_df['low_stock'] = stock_df['current_stock'] < stock_df['reorder_level']
low_stock = stock_df[stock_df['low_stock'] == True]
print("\n🚨 Low Stock Items:")
print(low_stock[['product_id', 'current_stock', 'reorder_level']])
low_stock.to_csv("low_stock_report.csv", index=False)
