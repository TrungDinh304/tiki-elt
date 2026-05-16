import requests
import json

url = "https://tiki.vn/api/personalish/v1/blocks/listings?limit=40&include=advertisement&aggregations=2&version=home-persionalized&category=8322&page=1"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

response = requests.get(url, headers=headers)
print(f"Status Code: {response.status_code}")
if response.status_code == 200:
    data = response.json()
    print(f"Total products: {data.get('paging', {}).get('total')}")
    print(f"Last page: {data.get('paging', {}).get('last_page')}")
    print(f"First product name: {data.get('data', [{}])[0].get('name')}")
else:
    print(response.text)
