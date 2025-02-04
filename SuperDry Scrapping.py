import time
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Initialize Selenium WebDriver
def get_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("window-size=1920,1080")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Handle cookies pop-up
def handle_cookies(driver):
    try:
        reject_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "onetrust-reject-all-handler"))
        )
        reject_button.click()
        print("Rejected optional cookies.")
        time.sleep(3)
    except Exception:
        print("No cookie pop-up found.")

# Scroll slowly through the page
def slow_scroll(driver):
    scroll_height = driver.execute_script("return document.body.scrollHeight")
    for i in range(0, scroll_height, 300):  # Scroll down in steps of 300 pixels
        driver.execute_script(f"window.scrollTo(0, {i});")
        time.sleep(0.5)  # Pause to allow content to load

# Extract products using Selenium
def extract_products(driver):
    products = []

    # Locate all product containers
    product_elements = driver.find_elements(By.CSS_SELECTOR, "div.product-elements > div")

    for product in product_elements:
        try:
            # Extract Title
            try:
                title_element = product.find_element(By.CSS_SELECTOR, "div.tile-body div.tile-name a")
                title = title_element.text.strip()
            except Exception:
                title = "Title not found"

            # Extract Price using the correct selector
            try:
                price_element = product.find_element(By.CSS_SELECTOR, "div.price")
                price = price_element.text.strip()
            except Exception:
                price = "Price not found"

            # Extract Image URL
            try:
                img_element = product.find_element(By.CSS_SELECTOR, "img")
                img_url = img_element.get_attribute("src")
            except Exception:
                img_url = "Image not available"

            # Extract Product Link
            try:
                link_element = product.find_element(By.CSS_SELECTOR, "div.tile-body div.tile-name a")
                product_link = link_element.get_attribute("href")
            except Exception:
                product_link = "Link not available"

            products.append({
                "title": title,
                "price": price,
                "image_url": img_url,
                "product_link": product_link
            })
        except Exception as e:
            print(f"Error extracting product: {e}")
            continue

    return products

# Scrape multiple pages and save to JSON
def scrape_multiple_pages():
    driver = get_driver()
    base_url = "https://www.superdry.de/damen/alles-anzeigen?page="
    all_products = []

    try:
        for page in range(1, 16):  # Scrape only page 1 for debugging
            url = f"{base_url}{page}"
            driver.get(url)

            # Handle cookies on the first page only
            if page == 1:
                handle_cookies(driver)

            # Wait for content to load
            print(f"Processing page {page}...")
            time.sleep(5)

            # Scroll slowly through the page to load all images
            slow_scroll(driver)

            # Extract products
            products = extract_products(driver)
            all_products.extend(products)
            print(f"Page {page} scraping completed.")

    finally:
        driver.quit()

    # Save all products to a JSON file
    with open("superdry_products.json", "w", encoding="utf-8") as file:
        json.dump(all_products, file, ensure_ascii=False, indent=4)
    print("Scraping completed. Data saved to 'superdry_products.json'.")

# Run the scraper
if __name__ == "__main__":
    scrape_multiple_pages()

