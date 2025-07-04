import pandas as pd

df = pd.read_csv("stock_movements.csv")

df['movement_date'] = pd.to_datetime(df['movement_date'])

df['month'] = df['movement_date'].dt.to_period('M')

df['quantity'] = df.apply(
    lambda row: -row['quantity'] if row['movement_type'] == 'OUT' else row['quantity'], axis=1
)

monthly_summary = df.groupby(['product_id', 'month'])['quantity'].sum().reset_index()

avg_monthly = monthly_summary.groupby('product_id')['quantity'].mean().reset_index()
avg_monthly.rename(columns={'quantity': 'avg_monthly_movement'}, inplace=True)



monthly_summary.to_csv("monthly_stock_summary.csv", index=False)
avg_monthly.to_csv("avg_monthly_movement.csv", index=False)

print("✅ Monthly stock analysis completed. CSVs saved.")
