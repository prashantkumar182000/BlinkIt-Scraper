import requests
import pandas as pd
import time
from datetime import datetime

# API Configuration (from your DevTools logs)
API_URL = "https://blinkit.com/v1/layout/listing_widgets"
HEADERS = {
    "authority": "blinkit.com",
    "auth_key": "c761ec3633c22afad934fb17a66385c1c06c5472b4898b866b7306186d0bb477",
    "app_client": "consumer_web"
}

# Read Input Files
categories_df = pd.read_csv("blinkit_categories.csv")
locations_df = pd.read_csv("blinkit_locations.csv")

# Output Storage
output_data = []

def scrape_blinkit(lat, lon, l0_cat, l1_cat, l1_name, l2_name):
    params = {
        "l0_cat": l0_cat,
        "l1_cat": l1_cat,
        "lat": lat,
        "lon": lon,
        "offset": 0,
        "limit": 15  # Adjust based on API pagination
    }
    
    try:
        response = requests.post(API_URL, headers=HEADERS, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Extract products (adjust based on actual API response)
        products = data.get("widgets", [{}])[0].get("data", {}).get("products", [])
        
        for product in products:
            output_data.append({
                "date": datetime.now().strftime("%Y-%m-%d"),
                "l1_category": l1_name,
                "l1_category_id": l0_cat,
                "l2_category": l2_name,
                "l2_category_id": l1_cat,
                "variant_id": product.get("variant_id", ""),
                "variant_name": product.get("name", ""),
                "selling_price": product.get("selling_price", ""),
                "mrp": product.get("mrp", ""),
                "in_stock": product.get("in_stock", False),
                "brand": product.get("brand", ""),
                "image_url": product.get("image_url", "")
            })
        
        print(f"Scraped {len(products)} products for {l2_name} at ({lat}, {lon})")
    
    except Exception as e:
        print(f"Error scraping {l2_name}: {e}")

# Main Execution
for _, location in locations_df.iterrows():
    lat, lon = location["latitude"], location["longitude"]
    for _, category in categories_df.iterrows():
        scrape_blinkit(
            lat, lon,
            category["l1_category_id"], category["l2_category_id"],
            category["l1_category"], category["l2_category"]
        )
    time.sleep(1)  # Avoid rate limits

# Save Output
pd.DataFrame(output_data).to_csv("blinkit_products.csv", index=False)
print("Scraping completed! Output saved to 'blinkit_products.csv'")