import asyncio
import aiohttp
import json
import csv
import os
from bs4 import BeautifulSoup

API_URL = "https://api.tiki.vn/product-detail/api/v1/products/{}"

CONCURRENCY = 50
DELAY_BETWEEN_BATCH = 0.1
MAX_RETRIES = 3
BATCH_SIZE = 1000  

INPUT_FILE = "products-0-200000.csv"
OUTPUT_DIR = "output"
ERROR_FILE = os.path.join(OUTPUT_DIR, "errors.log")

def clean_html(html):
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    return " ".join(soup.get_text(separator=" ").split())

def load_product_ids_from_csv(path):
    ids = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = row.get("id") 
            if pid:
                ids.append(pid.strip())
    return ids

async def fetch_product(session, pid):
    url = API_URL.format(pid)

    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, timeout=10) as resp:

                if resp.status == 200:
                    data = await resp.json()

                    return {
                        "id": data.get("id"),
                        "name": data.get("name"),
                        "url_key": data.get("url_key"),
                        "price": data.get("price"),
                        "description": clean_html(data.get("description")),
                        "images": [img.get("base_url") for img in data.get("images", [])]
                    }, None

                elif resp.status == 429:
                    await asyncio.sleep(1 + attempt)

                elif resp.status == 404:
                    return None, f"{pid} | HTTP 404"

                else:
                    return None, f"{pid} | HTTP {resp.status}"

        except Exception as e:
            await asyncio.sleep(1)

    return None, f"{pid} | Failed after retries"

def save_batch(data, index):
    filename = os.path.join(OUTPUT_DIR, f"products_{index}.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"Saved {filename}")

async def main():

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    product_ids = load_product_ids_from_csv(INPUT_FILE)
    total_ids = len(product_ids)

    connector = aiohttp.TCPConnector(limit=CONCURRENCY)

    async with aiohttp.ClientSession(connector=connector) as session:

        buffer = []
        file_index = 0
        errors = []

        for i in range(0, total_ids, CONCURRENCY):
            batch_ids = product_ids[i:i + CONCURRENCY]

            tasks = [fetch_product(session, pid) for pid in batch_ids]
            responses = await asyncio.gather(*tasks)

            for product, err in responses:
                if product:
                    buffer.append(product)
                if err:
                    errors.append(err)

                if len(buffer) >= BATCH_SIZE:
                    save_batch(buffer[:BATCH_SIZE], file_index)
                    buffer = buffer[BATCH_SIZE:]
                    file_index += 1

            await asyncio.sleep(DELAY_BETWEEN_BATCH)

            print(f"Progress: {i}/{total_ids} | files: {file_index}")

        if buffer:
            save_batch(buffer, file_index)

    with open(ERROR_FILE, "w", encoding="utf-8") as f:
        for e in errors:
            f.write(e + "\n")

    print("\nDONE")
    print(f"Total files: {file_index + 1}")
    print(f"Errors: {len(errors)} → {ERROR_FILE}")

if __name__ == "__main__":
    asyncio.run(main())