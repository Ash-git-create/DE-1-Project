import json
import os
import mysql.connector

# Define MySQL connection parameters (using container name)
conn = mysql.connector.connect(
    host="100.66.117.85",
    port=3306,
    user="root",
    password="123123",
    database="fashion_store"
)

cursor = conn.cursor()

# Step 1: Create database and tables
cursor.execute("CREATE DATABASE IF NOT EXISTS fashion_store;")
cursor.execute("USE fashion_store;")

table_creation_queries = [
    """
    CREATE TABLE IF NOT EXISTS products (
        product_id INT PRIMARY KEY,
        title VARCHAR(255),
        brand VARCHAR(50),
        category VARCHAR(50),
        gender VARCHAR(20),
        season VARCHAR(20),
        fit_type VARCHAR(50),
        source VARCHAR(50)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS pricing (
        product_id INT,
        original_price VARCHAR(50),
        FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS images (
        product_id INT,
        image_url VARCHAR(500),
        FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS links (
        product_id INT,
        product_link VARCHAR(500),
        FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS inventory (
        product_id INT,
        availability_status VARCHAR(50),
        FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
    );
    """
]

# Execute table creation queries
for query in table_creation_queries:
    cursor.execute(query)

print("✅ Tables created successfully.")

# Step 2: Extract JSON files from HDFS
hdfs_paths = {
    "products": "/user/hadoop/fashion_store/products.json",
    "pricing": "/user/hadoop/fashion_store/pricing.json",
    "images": "/user/hadoop/fashion_store/images.json",
    "links": "/user/hadoop/fashion_store/links.json",
    "inventory": "/user/hadoop/fashion_store/inventory.json"
}

# Define local storage directory for extracted files
local_path = "/mnt/data/"

# Ensure local directory exists
if not os.path.exists(local_path):
    os.makedirs(local_path)

# Extract files from HDFS before processing
for table, hdfs_path in hdfs_paths.items():
    local_file = os.path.join(local_path, f"{table}.json")
    command = f"hdfs dfs -get {hdfs_path} {local_file}"
    
    exit_code = os.system(command)
    if exit_code == 0:
        print(f"✅ Extracted {hdfs_path} to {local_file}")
    else:
        print(f"❌ Failed to extract {hdfs_path} from HDFS.")

# Step 3: Insert data from JSON files into MySQL
json_files = {
    "products": os.path.join(local_path, "products.json"),
    "pricing": os.path.join(local_path, "pricing.json"),
    "images": os.path.join(local_path, "images.json"),
    "links": os.path.join(local_path, "links.json"),
    "inventory": os.path.join(local_path, "inventory.json")
}

for table, file_path in json_files.items():
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)

        print(f"🔄 Inserting data into {table}...")

        for entry in data:
            if table == "products":
                cursor.execute("""
                    INSERT INTO products (product_id, title, brand, category, gender, season, fit_type, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (entry["product_id"], entry["title"], entry["brand"], entry["category"],
                      entry["gender"], entry["season"], entry["fit_type"], entry["source"]))

            elif table == "pricing":
                cursor.execute("""
                    INSERT INTO pricing (product_id, original_price)
                    VALUES (%s, %s)
                """, (entry["product_id"], entry["original_price"]))

            elif table == "images":
                cursor.execute("""
                    INSERT INTO images (product_id, image_url)
                    VALUES (%s, %s)
                """, (entry["product_id"], entry["image_url"]))

            elif table == "links":
                cursor.execute("""
                    INSERT INTO links (product_id, product_link)
                    VALUES (%s, %s)
                """, (entry["product_id"], entry["product_link"]))

            elif table == "inventory":
                cursor.execute("""
                    INSERT INTO inventory (product_id, availability_status)
                    VALUES (%s, %s)
                """, (entry["product_id"], entry["availability_status"]))

        conn.commit()
        print(f"✅ Data inserted successfully into {table}.")

    except Exception as e:
        print(f"❌ Error inserting into {table}: {e}")

# Step 4: Validate insertion by running queries
cursor.execute("SELECT COUNT(*) as count FROM products")
result = cursor.fetchone()
print(f"📊 Total records in products table: {result[0]}")

# Close connection
cursor.close()
conn.close()
print("✅ Database connection closed.")

