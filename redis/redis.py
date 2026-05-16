import requests
# Cache helper: Redis client package (PyPI) might not be installed in the current venv.
try:
    import redis as redis_client  # type: ignore
except Exception:  # pragma: no cover
    redis_client = None  # type: ignore


import json
import os
from urllib.parse import urlencode

## load host và port của Redis từ biến môi trường (nếu cần)
# Defaults giúp cache hoạt động ngay cả khi .env không set đủ biến.
# - Chạy local: localhost:6379
# - Chạy trong docker-compose (network): nên set REDIS_HOST=redis, REDIS_PORT=6379
redis_host = os.getenv("REDIS_HOST", "localhost")
redis_port = int(os.getenv("REDIS_PORT", "6379"))




# 1. Khởi tạo kết nối tới Redis
if redis_client is None:
    cache = None
else:
    try:
        cache = redis_client.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
    except Exception:  # pragma: no cover
        cache = None


# Đặt thời gian nhớ (TTL). Ví dụ: 86400 giây = 24 giờ
CACHE_TTL = 86400 

def make_cache_key(url, params=None):
    if not params:
        return url
    try:
        params_string = urlencode(sorted(params.items()), doseq=True)
    except Exception:
        params_string = json.dumps(params, sort_keys=True, separators=(",", ":"))
    return f"{url}?{params_string}"


def get_data_from_api(url, params=None):
    if cache is None:
        response = requests.get(url, params=params, timeout=15)
        return response.json() if response.status_code == 200 else None

    key = make_cache_key(url, params)
    try:
        cached_data = cache.get(key)
    except Exception as err:
        print(f"Redis cache read error: {err}")
        cached_data = None

    if cached_data:
        print("Dữ liệu được lấy từ cache.")
        try:
            return json.loads(cached_data)
        except json.JSONDecodeError:
            print("Cache data invalid json, ignoring cached value.")

    response = requests.get(url, params=params, timeout=15)
    if response.status_code == 200:
        try:
            data = response.json()
        except ValueError:
            print(f"Invalid JSON from API: {url}")
            return None

        if cache is not None:
            try:
                cache.setex(key, CACHE_TTL, json.dumps(data))
            except Exception as err:
                print(f"Redis cache write error: {err}")

        print("Dữ liệu được lấy từ API và lưu vào cache.")
        return data
    else:
        print(f"API call failed with status code: {response.status_code}")
        return None


def check_cache(url, params):
    # Nếu Redis chưa sẵn sàng thì coi như cache miss.
    if cache is None:
        return False

    key = make_cache_key(url, params)
    try:
        cached_data = cache.get(key)
    except Exception as err:
        print(f"Redis cache check error: {err}")
        return False

    if cached_data:
        print("Dữ liệu có trong cache.")
        return True

    print("Dữ liệu không có trong cache.")
    return False

