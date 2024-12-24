from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy_task import Customers
from sqlalchemy_task import Products
from sqlalchemy_task import Orders
from sqlalchemy_task import OrderItems
from sqlalchemy import func
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px


#%%
# Database credentials
username = 'postgres'
password = 'ZcXv5267'
host = 'localhost'
port = '5432'
database = 'big_ps_task'

# Connection string
connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'

try:
    # Create engine
    engine = create_engine(connection_string)

    # Test connection
    with engine.connect() as connection:
        print("Connected to the database!")

except SQLAlchemyError as e:
    print(f"An error occurred while connecting to the database: {e}")

except Exception as e:
    print(f"An unexpected error occurred: {e}")


#%%
# Create a session
Session = sessionmaker(bind=engine)
session = Session()


#%%
# Query the customers who have placed the most orders, sorting by descending order
top_clients = (session.query(Customers, func.count(Orders.order_id).label("order_count"))
                      .join(Orders, Orders.customer_id == Customers.customer_id)
                      .group_by(Customers.customer_id)
                      .order_by(func.count(Orders.order_id).desc())
                      #.all()
              )
#%%
for customer, order_count in top_clients:
    print(f"Customer: {customer.name}: Orders: {order_count}")

#%%
# Query top 5 bestsellers products with indication of category and sales amount
bestsellers = (session.query(Products.name, Products.category, func.sum(OrderItems.quantity).label("quant_sum"))
                      .join(OrderItems, OrderItems.product_id == Products.product_id)
                      .group_by(Products.name, Products.category)
                      .order_by(func.sum(OrderItems.quantity).desc())
                      .limit(5)
              )
#%%
for product, category_name, quantity in bestsellers:
    print(f"Product: {product}: Category: {category_name}: Quantity: {quantity}")


#%%
# Create a dataframe for top clients query
df_1 = pd.read_sql_query(top_clients.statement, con=engine)

#%%
# Create histogram for top clients
fig = px.histogram(
    df_1,
    x='order_count',  # Column to plot
    title='Distribution of Order Counts per Customer',
    labels={'order_count': 'Number of Orders', 'count': 'Number of Customers'},  # Axis label customization
    nbins=10,  # Number of bins (optional)
                   )
fig.show()


#%%
# Create a dataframe for bestsellers query
df_2 = pd.read_sql_query(bestsellers.statement, con=engine)

#%%
# Create the line plot for bestsellers
plt.figure(figsize=(10, 6))
sns.lineplot(data=df_2, x='name', y='quant_sum', marker='o')

# Customize the plot
plt.title('Product Quantities Sold', fontsize=16)
plt.xlabel('Product Name', fontsize=14)
plt.ylabel('Quantity Sold', fontsize=14)
plt.xticks(rotation=45, ha='right')
plt.grid(True)
plt.tight_layout()  # Adjust layout to avoid overlap

plt.show()
