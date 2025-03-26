import os

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///data/data.db")
WAREHOUSES = int(os.environ.get("WAREHOUSES", 10))
