# generate_data.py
import pandas as pd
import random
from datetime import date, timedelta

random.seed(42)

# 1) DimProduct: 20 products
categories = ['Grocery','Dairy','Bakery','Beverages','Snacks']
brands = ['BrandA','BrandB','BrandC','BrandD']
products = []
for pid in range(1,21):
    products.append({
        'product_id': pid,
        'product_name': f'Product_{pid}',
        'category': random.choice(categories),
        'brand': random.choice(brands),
        'price': round(random.uniform(20,500),2)
    })
pd.DataFrame(products).to_csv('DimProduct.csv', index=False)

# 2) DimCustomer: 50 customers
cities = ['Chennai','Bengaluru','Hyderabad','Mumbai','Kochi']
age_groups = ['18-25','26-35','36-45','46-60']
customers = []
for cid in range(1,51):
    customers.append({
        'customer_id': cid,
        'name': f'Cust_{cid}',
        'gender': random.choice(['M','F']),
        'age_group': random.choice(age_groups),
        'city': random.choice(cities)
    })
pd.DataFrame(customers).to_csv('DimCustomer.csv', index=False)

# 3) DimStore: 5 stores
stores = []
for sid in range(1,6):
    stores.append({
        'store_id': sid,
        'store_name': f'Store_{sid}',
        'location': random.choice(cities),
        'region': random.choice(['South','West','North','East'])
    })
pd.DataFrame(stores).to_csv('DimStore.csv', index=False)

# 4) DimDate: one year of dates
start = date(2024,1,1)
dates = []
for i in range(365):
    d = start + timedelta(days=i)
    dates.append({
        'date_id': int(d.strftime('%Y%m%d')),
        'dt': d.isoformat(),
        'month': d.month,
        'quarter': (d.month-1)//3 + 1,
        'year': d.year
    })
pd.DataFrame(dates).to_csv('DimDate.csv', index=False)

# 5) Transactions / FactSales CSV
# Generate 1000 transactions
trans = []
tid = 1
for _ in range(1000):
    d = random.choice(dates)
    pid = random.choice(products)['product_id']
    cid = random.choice(customers)['customer_id']
    sid = random.choice(stores)['store_id']
    qty = random.choices([1,2,3,4,5], weights=[60,20,10,7,3])[0]
    price = next(p['price'] for p in products if p['product_id']==pid)
    total = round(price * qty, 2)
    trans.append({
        'sale_id': tid,
        'date_id': d['date_id'],
        'product_id': pid,
        'customer_id': cid,
        'store_id': sid,
        'quantity': qty,
        'total_amount': total
    })
    tid += 1
pd.DataFrame(trans).to_csv('FactSales.csv', index=False)

print("CSV files created: DimProduct.csv, DimCustomer.csv, DimStore.csv, DimDate.csv, FactSales.csv")
