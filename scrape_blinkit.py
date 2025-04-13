import requests
import pandas as pd
import json
import time
import uuid
from datetime import datetime
import random

# Load the input CSV files
categories_df = pd.read_csv('blinkit_categories.csv')
locations_df = pd.read_csv('blinkit_locations.csv')
print(f"Loaded {len(categories_df)} categories and {len(locations_df)} locations")

# Generate a random device ID (similar to what a browser would have)
device_id = str(uuid.uuid4())
session_uuid = str(uuid.uuid4())

# Headers setup with the device ID
headers = {
    'authority': 'blinkit.com',
    'accept': '*/*',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'app_client': 'consumer_web',
    'app_version': '1010101010',
    'content-type': 'application/json',
    'device_id': device_id,
    'origin': 'https://blinkit.com',
    'platform': 'desktop_web',
    'referer': 'https://blinkit.com/',
    'session_uuid': session_uuid,
    'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    'web_app_version': '1008010016',
    'x-age-consent-granted': 'true'
}

# Create an empty list to store all scraped data
all_data = []

# Current date for the scraping
current_date = datetime.now().strftime('%Y-%m-%d')

# Using a fixed store ID for testing - we'll try this approach first
DEFAULT_STORE_ID = "30377"  # This was visible in your network logs

# Function to scrape products for a given category, subcategory, and location
def scrape_category(lat, lng, l1_category, l1_category_id, l2_category, l2_category_id):
    print(f"Scraping {l1_category} > {l2_category} at {lat}, {lng}")
    
    # Update location-specific headers
    location_headers = headers.copy()
    location_headers.update({
        'lat': str(lat),
        'lon': str(lng),
    })
    
    # API endpoint for category listing
    url = f"https://blinkit.com/v1/layout/listing_widgets?l0_cat={l1_category_id}&l1_cat={l2_category_id}"
    
    # Empty payload as seen in the network logs
    payload = {}
    
    try:
        response = requests.post(url, headers=location_headers, json=payload)
        
        if response.status_code == 200:
            try:
                data = response.json()
                
                # Check if we got a valid response
                if not data.get('is_success'):
                    print(f"API returned unsuccessful response for {l1_category} > {l2_category}")
                    return 0
                
                products_count = 0
                
                # Extract products from the response
                if 'response' in data and 'snippets' in data['response']:
                    for snippet in data['response']['snippets']:
                        if 'data' not in snippet:
                            continue
                            
                        product_data = snippet.get('data', {})
                        
                        # Skip non-product snippets
                        if 'product_id' not in product_data and 'id' not in product_data:
                            continue
                            
                        product_brand = product_data.get('brand', {})
                        
                        # Get the first variant or use empty dict if no variants
                        variants = product_data.get('variants', [])
                        variant_data = variants[0] if variants else {}
                        
                        # If we have a variant but no inventory data, create an empty dict
                        if 'inventory' not in variant_data:
                            variant_data['inventory'] = {}
                            
                        # Price handling - may be nested differently in different responses
                        price_data = variant_data.get('price', {})
                        
                        # Extract basic product information
                        product_info = {
                            'date': current_date,
                            'l1_category': l1_category,
                            'l1_category_id': l1_category_id,
                            'l2_category': l2_category,
                            'l2_category_id': l2_category_id,
                            'store_id': DEFAULT_STORE_ID,  # Using default store ID
                            'variant_id': variant_data.get('id', ''),
                            'variant_name': variant_data.get('name', ''),
                            'group_id': product_data.get('product_id', '') or product_data.get('id', ''),
                            'selling_price': price_data.get('effective_value', 0),
                            'mrp': price_data.get('marked_value', 0),
                            'in_stock': not variant_data.get('out_of_stock', True),
                            'inventory': variant_data.get('inventory', {}).get('count', 0),
                            'is_sponsored': product_data.get('is_sponsored', False),
                            'image_url': product_data.get('image', {}).get('url', ''),
                            'brand_id': product_brand.get('id', ''),
                            'brand': product_brand.get('name', '')
                        }
                        
                        all_data.append(product_info)
                        products_count += 1
                
                print(f"Found {products_count} products for {l1_category} > {l2_category}")
                return products_count
                
            except json.JSONDecodeError:
                print(f"Failed to parse JSON response for {l1_category} > {l2_category}")
                return 0
        else:
            print(f"HTTP Error {response.status_code} for {l1_category} > {l2_category}: {response.text}")
            return 0
            
    except Exception as e:
        print(f"Error scraping {l1_category} > {l2_category} at {lat}, {lng}: {e}")
        return 0

# Main execution loop
total_products = 0

# We'll use only a few locations to ensure we get some data
# Starting with Bangalore coordinates from your network logs
test_locations = [
    {"latitude": 12.9417261, "longitude": 77.6034555},  # Bangalore coordinates from the logs
    {"latitude": 12.93326667, "longitude": 77.61773333},  # From your CSV
    {"latitude": 13.00826667, "longitude": 77.64273333}   # From your CSV
]

# Loop through test locations first
for location in test_locations:
    lat = location['latitude']
    lng = location['longitude']
    
    print(f"Processing location: {lat}, {lng}")
    
    # Loop through all categories at this location
    for _, category in categories_df.iterrows():
        l1_category = category['l1_category']
        l1_category_id = category['l1_category_id']
        l2_category = category['l2_category']
        l2_category_id = category['l2_category_id']
        
        # Scrape this category at this location
        products_count = scrape_category(
            lat, lng, l1_category, l1_category_id, 
            l2_category, l2_category_id
        )
        
        total_products += products_count
        
        # Sleep to avoid rate limiting
        time.sleep(1.5 + random.random())
    
    # Sleep between locations to avoid rate limiting
    time.sleep(3)

# If we still have no data, try with the original locations
if len(all_data) == 0:
    print("No data from test locations, trying original locations...")
    
    # Try each location from the original dataset
    for idx, location in locations_df.iterrows():
        lat = location['latitude']
        lng = location['longitude']
        
        print(f"Processing location {idx+1}/{len(locations_df)}: {lat}, {lng}")
        
        # Try just one category per location to test
        category = categories_df.iloc[0]
        l1_category = category['l1_category']
        l1_category_id = category['l1_category_id']
        l2_category = category['l2_category']
        l2_category_id = category['l2_category_id']
        
        # Try scraping with this location
        products_count = scrape_category(
            lat, lng, l1_category, l1_category_id, 
            l2_category, l2_category_id
        )
        
        if products_count > 0:
            print(f"Found working location: {lat}, {lng}")
            
            # If this location works, scrape all remaining categories
            for _, category in categories_df.iloc[1:].iterrows():
                l1_category = category['l1_category']
                l1_category_id = category['l1_category_id']
                l2_category = category['l2_category']
                l2_category_id = category['l2_category_id']
                
                more_products = scrape_category(
                    lat, lng, l1_category, l1_category_id, 
                    l2_category, l2_category_id
                )
                
                total_products += more_products
                time.sleep(1.5 + random.random())
        
        # Sleep between locations to avoid rate limiting
        time.sleep(2)

# Create DataFrame from all collected data
results_df = pd.DataFrame(all_data)

# Save results to CSV
output_filename = f"blinkit_category_scraping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
results_df.to_csv(output_filename, index=False)

print(f"Scraping completed. Total products scraped: {total_products}")
print(f"Results saved to {output_filename}")