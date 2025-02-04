import json
import os
import re

# File paths
base_path = r"C:\Users\ACER\Desktop\DE Project"
tables_path = os.path.join(base_path, "Tables")  # Ensure tables are saved in this directory
os.makedirs(tables_path, exist_ok=True)  # Create directory if it does not exist

hm_file = os.path.join(base_path, "hm_products.json")
superdry_file = os.path.join(base_path, "superdry_products.json")
zara_file = os.path.join(base_path, "zara_products.json")

merged_file = os.path.join(base_path, "merged_fashion_products.json")
enriched_file = os.path.join(base_path, "enriched_fashion_products.json")

# Function to load JSON data and add brand
def load_json(file_path, brand):
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)
        for item in data:
            item["brand"] = brand  # Add brand field
        return data

# Load datasets
hm_data = load_json(hm_file, "H&M")
superdry_data = load_json(superdry_file, "Superdry")
zara_data = load_json(zara_file, "Zara")

# Merge datasets
merged_data = hm_data + superdry_data + zara_data

# Save merged dataset
with open(merged_file, "w", encoding="utf-8") as outfile:
    json.dump(merged_data, outfile, indent=4, ensure_ascii=False)

# Keywords to determine gender
GENDER_KEYWORDS = {
    "Men": ["men", "male", "herr", "man"],
    "Women": ["women", "female", "damen", "lady", "girl"],
    "Unisex": ["unisex", "all"]
}

# Keywords for clothing type
CLOTHING_TYPE_KEYWORDS = {
    "Outerwear": ["jacket", "coat", "parka", "sweater", "cardigan"],
    "Bottoms": ["jeans", "trousers", "pants", "shorts", "chinos"],
    "Tops": ["shirt", "t-shirt", "polo", "blouse"],
    "Footwear": ["boots", "shoes", "sneakers", "sandals"],
    "Accessories": ["cap", "hat", "watch", "bag", "belt", "scarf"]
}

# Keywords for fit type
FIT_TYPE_KEYWORDS = {
    "Slim Fit": ["slim fit", "fitted", "skinny"],
    "Regular Fit": ["regular fit", "classic fit"],
    "Oversized Fit": ["oversized", "loose fit", "baggy"],
    "Loose Fit": ["relaxed fit", "loose", "wide leg"]
}

# Keywords for seasonality
SEASON_KEYWORDS = {
    "Winter": ["coat", "jacket", "parka", "sweater", "wool", "hoodie"],
    "Summer": ["shorts", "t-shirt", "dress", "linen"],
    "All-Season": ["jeans", "trousers", "accessory", "shoes"]
}

# Function to determine gender
def extract_gender(title):
    title_lower = title.lower()
    for gender, keywords in GENDER_KEYWORDS.items():
        if any(keyword in title_lower for keyword in keywords):
            return gender
    return "Unisex"

# Function to determine clothing type
def extract_clothing_type(title):
    title_lower = title.lower()
    for category, keywords in CLOTHING_TYPE_KEYWORDS.items():
        if any(keyword in title_lower for keyword in keywords):
            return category
    return "Other"

# Function to determine fit type
def extract_fit_type(title):
    title_lower = title.lower()
    for fit, keywords in FIT_TYPE_KEYWORDS.items():
        if any(keyword in title_lower for keyword in keywords):
            return fit
    return "Regular Fit"

# Function to extract season
def extract_season(title):
    title_lower = title.lower()
    for season, keywords in SEASON_KEYWORDS.items():
        if any(keyword in title_lower for keyword in keywords):
            return season
    return "All-Season"

# Enrich dataset with extracted fields
for product in merged_data:
    product["gender"] = extract_gender(product["title"])
    product["clothing_type"] = extract_clothing_type(product["title"])
    product["fit_type"] = extract_fit_type(product["title"])
    product["season"] = extract_season(product["title"])

# Save enriched dataset
with open(enriched_file, "w", encoding="utf-8") as outfile:
    json.dump(merged_data, outfile, indent=4, ensure_ascii=False)

# Splitting into tables
products_table = []
pricing_table = []
images_table = []
links_table = []
inventory_table = []

for idx, product in enumerate(merged_data):
    product_id = idx + 1
    products_table.append({
        "product_id": product_id,
        "title": product["title"],
        "brand": product["brand"],
        "category": product["clothing_type"],
        "gender": product["gender"],
        "season": product["season"],
        "fit_type": product["fit_type"],
        "source": product["brand"],
    })

    pricing_table.append({
        "product_id": product_id,
        "original_price": product["price"],
    })

    images_table.append({
        "product_id": product_id,
        "image_url": product["image_url"],
    })

    links_table.append({
        "product_id": product_id,
        "product_link": product["product_link"],
    })

    inventory_table.append({
        "product_id": product_id,
        "availability_status": "In Stock",  # Placeholder, could be dynamic
    })

# Save tables
tables = {
    "products": products_table,
    "pricing": pricing_table,
    "images": images_table,
    "links": links_table,
    "inventory": inventory_table,
}

for table_name, data in tables.items():
    table_file = os.path.join(tables_path, f"{table_name}.json")
    with open(table_file, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

print("Data processing completed successfully! All tables are saved in:", tables_path)

