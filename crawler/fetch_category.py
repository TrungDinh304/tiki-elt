import requests
## fetch category data with api call
category_url="https://api.tiki.vn/raiden/v2/menu-config?platform=desktop&30122025=1"
def fetch_category():
    print(f"Fetching category data from {category_url}")
    try:
        response = requests.get(category_url, timeout=15)
        if response.status_code == 200:
            print("Category data fetched successfully.")
            return response.json()
        else:
            print(f"Failed to fetch category data. Status code: {response.status_code}")
            return None
    except Exception as err:
        print(f"Error fetching category data: {err}")
        return None
    
if __name__ == "__main__":
    category_data = fetch_category()
    if category_data:
        print("Category data structure:")
        print(category_data)
    # save category data to minio 
    import os
    from minio import Minio
    from minio.error import S3Error
    minio_client = Minio(
        os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )
    try:
        minio_client.put_object(
            os.getenv("RAW_DATA_BUCKET"),
            "category/category_data.json",
            data=bytes(str(category_data), encoding='utf-8'),
            length=len(bytes(str(category_data), encoding='utf-8')),
        )
        print("Category data saved to MinIO successfully.")
    except S3Error as err:
        print(f"Error saving category data to MinIO: {err}")
