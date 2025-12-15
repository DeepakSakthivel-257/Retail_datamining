# mining.py
import pandas as pd
from sqlalchemy import create_engine
from mlxtend.frequent_patterns import apriori, association_rules
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import numpy as np

# DB config
USER = 'root'
PASSWORD = '22052006'  
HOST = '127.0.0.1'
PORT = '3306'
DB = 'retail_dw'
engine = create_engine(f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}', echo=False)

# Load tables
fact = pd.read_sql('SELECT * FROM FactSales', engine)
prod = pd.read_sql('SELECT * FROM DimProduct', engine)
cust = pd.read_sql('SELECT * FROM DimCustomer', engine)

# ========== Association Rules ==========
# Build basket: group by sale_id and product_name presence
merged = fact.merge(prod[['product_id','product_name']], on='product_id', how='left')
basket = (merged.groupby(['sale_id','product_name'])['quantity']
          .sum().unstack().fillna(0))
# Convert to 1/0
basket_binary = basket.applymap(lambda x: 1 if x>0 else 0)

# Frequent itemsets
freq_items = apriori(basket_binary, min_support=0.03, use_colnames=True)
rules = association_rules(freq_items, metric="confidence", min_threshold=0.4)
rules = rules[['antecedents','consequents','support','confidence','lift']].sort_values(by='lift', ascending=False)
print("Top association rules:")
print(rules.head(10))

# ========== Customer Segmentation (KMeans) ==========
# Features per customer: total_spent, total_qty, unique_products
cust_stats = fact.groupby('customer_id').agg(
    total_spent=('total_amount','sum'),
    total_qty=('quantity','sum'),
    unique_products=('product_id', lambda x: x.nunique())
).reset_index()
# Normalize
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
X = scaler.fit_transform(cust_stats[['total_spent','total_qty','unique_products']])

# Choose k=3
kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
cust_stats['cluster'] = kmeans.fit_predict(X)

print("\nCustomer cluster counts:")
print(cust_stats['cluster'].value_counts())

# Save cluster centers (inverse transform to original scale)
centers = scaler.inverse_transform(kmeans.cluster_centers_)
centers_df = pd.DataFrame(centers, columns=['total_spent','total_qty','unique_products'])
print("\nCluster centers (approx):")
print(centers_df)

# ========== Plots ==========
plt.figure(figsize=(8,5))
# monthly sales from DB
monthly = pd.read_sql("""
SELECT d.month, SUM(f.total_amount) AS monthly_sales
FROM FactSales f JOIN DimDate d ON f.date_id = d.date_id
WHERE d.year = 2024
GROUP BY d.month ORDER BY d.month;
""", engine)
plt.plot(monthly['month'], monthly['monthly_sales'])
plt.title('Monthly Sales (2024)')
plt.xlabel('Month')
plt.ylabel('Sales')
plt.grid(True)
plt.tight_layout()
plt.savefig('monthly_sales.png')
print("\nSaved monthly_sales.png")

# scatter of customers by total_spent vs total_qty colored by cluster
plt.figure(figsize=(8,5))
for c in sorted(cust_stats['cluster'].unique()):
    subset = cust_stats[cust_stats['cluster']==c]
    plt.scatter(subset['total_qty'], subset['total_spent'], label=f'Cluster {c}', alpha=0.7)
plt.xlabel('Total Quantity')
plt.ylabel('Total Spent')
plt.title('Customer Segments')
plt.legend()
plt.tight_layout()
plt.savefig('customer_segments.png')
print("Saved customer_segments.png")

# Save rules to CSV
rules.to_csv('association_rules.csv', index=False)
cust_stats.to_csv('customer_clusters.csv', index=False)
print("\nSaved association_rules.csv and customer_clusters.csv")
