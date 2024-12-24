from sqlalchemy import create_engine, ForeignKey, Column, Integer, String, Date
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from datetime import date, timedelta
import random
import logging

#%%
# Set a seed for reproducibility
random.seed(42)

#%%
Base = declarative_base()

# Define the Customers model
class Customers(Base):
    __tablename__ = 'customers'
    customer_id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    email = Column(String(50), unique=True)
    registration_date = Column(Date)

    def __init__(self, customer_id, name, email, registration_date):
        self.customer_id = customer_id
        self.name = name
        self.email = email
        self.registration_date = registration_date

    def __repr__(self):
        return (f"<Customers(customer_id={self.customer_id}"
                f", name='{self.name}'"
                f", email='{self.email}'"
                f", registration_date={self.registration_date})>")


# Define the Products model
class Products(Base):
    __tablename__ = 'products'
    product_id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    category = Column(String(50))
    price = Column(Integer)

    def __init__(self, product_id, name, category, price):
        self.product_id = product_id
        self.name = name
        self.category = category
        self.price = price

    def __repr__(self):
        return (f"<Products(product_id={self.product_id}"
                f", name='{self.name}'"
                f", category='{self.category}'"
                f", price={self.price})>")


# Define the Orders model
class Orders(Base):
    __tablename__ = 'orders'
    order_id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey('customers.customer_id'))
    order_date = Column(Date)
    status = Column(String(50))

    def __init__(self, order_id, customer_id, order_date, status):
        self.order_id = order_id
        self.customer_id = customer_id
        self.order_date = order_date
        self.status = status

    def __repr__(self):
        return (f"<Orders(order_id={self.order_id}"
                f", customer_id={self.customer_id}"
                f", order_date={self.order_date}"
                f", status='{self.status}')>")

# Define the OrderItems model
class OrderItems(Base):
    __tablename__ = 'order_items'
    order_item_id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey('orders.order_id'))
    product_id = Column(Integer, ForeignKey('products.product_id'))
    quantity = Column(Integer)

    def __init__(self, order_item_id, order_id, product_id, quantity):
        self.order_item_id = order_item_id
        self.order_id = order_id
        self.product_id = product_id
        self.quantity = quantity

    def __repr__(self):
        return (f"<OrderItems(order_item_id={self.order_item_id}"
                f", order_id={self.order_id}"
                f", product_id={self.product_id}"
                f", quantity={self.quantity})>")


#%%
# Database credentials
username = 'postgres'
password = '********'
host = 'localhost'
port = '5432'
database = '********'

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
# Define the base
Base.metadata.create_all(engine)

#%%
# Create a session
Session = sessionmaker(bind=engine)
session = Session()

#%%
# Write function to generate data for customers table
def create_customers():
    names = ["Alice", "Bob", "Charlie", "Diana", "Ethan", "Fiona", "George", "Hannah", "Ian", "Jasmine"]
    customers = []
    for i, name in enumerate(names, start=1):
        customers.append(
            Customers(
                customer_id=i,
                name=name,
                email=f"{name.lower()}@example.com",
                registration_date=date.today() - timedelta(days=random.randint(0, 365))
            )
        )
    session.add_all(customers)
    session.commit()

#%%
# Write function to generate data for products table
def create_products():
    products_objects = [
        (1, "iPhone 15", "Electronics", 999),
        (2, "Samsung Galaxy S22", "Electronics", 899),
        (3, "Sony Bravia TV", "Electronics", 1200),
        (4, "MacBook Pro", "Electronics", 2500),
        (5, "Dell XPS 13", "Electronics", 1400),
        (6, "Nike Air Max", "Clothing", 120),
        (7, "Adidas Ultraboost", "Clothing", 180),
        (8, "The Great Gatsby", "Books", 15),
        (9, "1984 by George Orwell", "Books", 20),
        (10, "Harry Potter Series", "Books", 100),
        (11, "Wooden Dining Table", "Furniture", 600),
        (12, "Leather Sofa", "Furniture", 1200),
        (13, "LEGO Star Wars Set", "Toys", 150),
        (14, "Barbie Dreamhouse", "Toys", 200),
        (15, "Hot Wheels Track", "Toys", 50)
    ]
    products = [
        Products(product_id=product_id, name=name, category=category, price=price)
        for product_id, name, category, price in products_objects
    ]
    session.add_all(products)
    session.commit()

#%%
# Write function to generate data for orders table
def create_orders():
    statuses = ['done', 'canceled', 'processing']
    orders = []
    customers = session.query(Customers).all()

    for i in range(1, 29): # Generate 28 orders
        customer = random.choice(customers)
        min_order_date = customer.registration_date # Create order_date concerning registration date of customer
        max_order_date = date.today()
        order_date = min_order_date + timedelta(days=random.randint(0, (max_order_date - min_order_date).days))

        orders.append(
            Orders(
                order_id=i,
                customer_id=customer.customer_id,
                order_date=order_date,
                status=random.choice(statuses)
            )
        )
    session.add_all(orders)
    session.commit()

#%%
# Write function to generate data for order_items table
def create_order_items():
    orders = session.query(Orders).all() # Get the order_id from orders table
    product_ids = [product.product_id for product in session.query(Products).all()] # product_id from products table
    order_items = []
    order_item_id = 1
    for order in orders:
        num_items = random.randint(1, 5)  # Each order gets 1 to 5 items
        for _ in range(num_items):
            order_items.append(
                OrderItems(
                    order_item_id=order_item_id,
                    order_id=order.order_id,
                    product_id=random.choice(product_ids),
                    quantity=random.randint(1, 10)
                )
            )
            order_item_id += 1
    session.add_all(order_items)
    session.commit()

#%%
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#%%
# Execute data creation with logging
try:
    logging.info("Starting data generation...")
    create_customers()
    logging.info("Customers created successfully.")
    create_products()
    logging.info("Products created successfully.")
    create_orders()
    logging.info("Orders created successfully.")
    create_order_items()
    logging.info("Order items created successfully.")
    logging.info("Data generation completed.")
except Exception as e:
    logging.error(f"An error occurred during data generation: {e}")
#%%