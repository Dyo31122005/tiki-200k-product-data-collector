# tiki-200k-product-data-collector
Python-based high-performance scraper to collect 200k product details from Tiki API and store them as structured JSON files.

Input data
Resource file:
A list of product IDs collected from Tiki, located in: product_ids.csv

Product Detail API: https://api.tiki.vn/product-detail/api/v1/products/{product_id}

Output data
The crawler outputs product data in JSON format, with each file containing up to 1000 products. Each product record includes the following fields:
id
name
url
key
price
description
images_url
