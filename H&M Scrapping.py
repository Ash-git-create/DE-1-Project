import time
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

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

# Extract products using BeautifulSoup
def extract_products_with_bs(driver):
    base_url = "https://www2.hm.com"
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    product_containers = soup.select("section[data-hydration-on-demand='true']")
    products = []

    for container in product_containers:
        try:
            # Extract Image URL
            img_tag = container.select_one("div[data-testid='next-image'] img")
            img_url = img_tag['src'] if img_tag and 'src' in img_tag.attrs else "Image not available"

            # Extract Title
            title_tag = container.select_one("h2")
            title = title_tag.get_text(strip=True) if title_tag else "Title not found"

            # Extract Price
            price_tag = container.select_one("p.d3254e.db5fc6 span.aeecde.ac3d9e")
            price = price_tag.get_text(strip=True) if price_tag else "Price not found"

            # Extract Product Link
            link_tag = container.select_one("a")
            product_link = base_url + link_tag['href'] if link_tag and 'href' in link_tag.attrs else "Link not available"

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
    base_url = "https://www2.hm.com/de_de/herren/neuheiten/alle-anzeigen.html?page="
    all_products = []

    try:
        for page in range(1, 16):  # Scrape pages 1 to 15
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
            products = extract_products_with_bs(driver)
            all_products.extend(products)
            print(f"Page {page} scraping completed.")

    finally:
        driver.quit()

    # Save all products to a JSON file
    with open("hm_products.json", "w", encoding="utf-8") as file:
        json.dump(all_products, file, ensure_ascii=False, indent=4)
    print("Scraping completed. Data saved to 'hm_products.json'.")

# Run the scraper
if __name__ == "__main__":
    scrape_multiple_pages()

