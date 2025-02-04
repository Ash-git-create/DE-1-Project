import time
import json
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

def get_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

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

def incremental_scroll(driver, total_height=50000, step=100):
    current_height = 0
    while current_height < total_height:
        driver.execute_script(f"window.scrollBy(0, {step});")
        current_height += step
        time.sleep(random.uniform(2, 4))  # Mimic human behavior with longer delays
    print(f"Scrolled to a total height of {total_height}px.")

def extract_zara_data():
    driver = get_driver()
    url = "https://www.zara.com/de/en/man-all-products-l7465.html?v1=2443335"
    driver.get(url)

    # Handle cookies
    handle_cookies(driver)

    # Adjust z-index to ensure visibility
    driver.execute_script(
        "document.querySelectorAll('.media__wrapper--fill').forEach(e => e.style.zIndex = '2');"
    )
    driver.execute_script(
        "document.querySelectorAll('.product-add-to-cart').forEach(e => e.style.zIndex = '0');"
    )

    print("Scrolling incrementally to load products...")
    incremental_scroll(driver, total_height=50000, step=100)

    try:
        WebDriverWait(driver, 60).until(
            EC.presence_of_all_elements_located((By.XPATH, "//ul/li"))
        )
    except Exception:
        print("Error: Products did not load in time.")
        driver.quit()
        return []

    print("Products loaded. Extracting data...")

    products = []
    product_containers = driver.find_elements(By.XPATH, "//ul/li")

    for idx, container in enumerate(product_containers):
        try:
            # Extract product details
            title = container.find_element(By.XPATH, ".//div[2]/div/div/div/div[1]/a/h2").text
            price = container.find_element(By.XPATH, ".//div[2]/div/div/div/div[3]").text
            img_url = container.find_element(By.XPATH, ".//div[1]/a/div/div/div/img").get_attribute("src")
            product_link = container.find_element(By.XPATH, ".//div[2]/div/div/div/div[1]/a").get_attribute("href")

            print(f"Title: {title}, Price: {price}, Image URL: {img_url}, Product Link: {product_link}")

            products.append({
                "title": title,
                "price": price,
                "image_url": img_url,
                "product_link": product_link
            })

            if len(products) >= 400:
                break

        except Exception as e:
            print(f"Error extracting product {idx + 1}: {e}")

    driver.quit()
    return products

def save_to_json(data, filename="zara_products.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    print("Scraping Zara website...")
    zara_products = extract_zara_data()
    save_to_json(zara_products)
    print(f"Scraped {len(zara_products)} products and saved to zara_products.json")

