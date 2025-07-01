import pandas as pd

# Load stock movement history
df = pd.read_csv("stock_movements.csv")

# Convert movement_date to datetime
df['movement_date'] = pd.to_datetime(df['movement_date'])

# Add month column
df['month'] = df['movement_date'].dt.to_period('M')

# Convert OUT to negative quantity
df['quantity'] = df.apply(
    lambda row: -row['quantity'] if row['movement_type'] == 'OUT' else row['quantity'], axis=1
)

# Group by product and month
monthly_summary = df.groupby(['product_id', 'month'])['quantity'].sum().reset_index()

# Calculate average monthly movement per product
avg_monthly = monthly_summary.groupby('product_id')['quantity'].mean().reset_index()
avg_monthly.rename(columns={'quantity': 'avg_monthly_movement'}, inplace=True)

# Merge with latest stock data if needed
# stock_df = pd.read_csv("stock_summary.csv")
# final_df = pd.merge(stock_df, avg_monthly, on='product_id', how='left')

# Save the report
monthly_summary.to_csv("monthly_stock_summary.csv", index=False)
avg_monthly.to_csv("avg_monthly_movement.csv", index=False)

print("✅ Monthly stock analysis completed. CSVs saved.")
