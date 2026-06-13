"""Power BI data-source script — load Tiki marts từ MinIO qua DuckDB.

Cách dùng: Power BI Desktop → Get Data → Python script → paste toàn bộ file.
Mỗi top-level DataFrame trở thành 1 query trong Navigator.

Quan hệ category ↔ product (hub: dim_categories):

    dim_categories.category_id            [PK, snowflake hub]
        ↑ parent_category_id              (self-ref, mô tả nhánh cha)
        ↑ dim_products.category_id        (FK, Many-to-One)
        ↑ fct_tiki_products.category_id   (FK, Many-to-One, có thể NULL với
                                           SKU listings-only chưa crawl detail)

Sau khi script load xong, vào Power BI → Model view → tạo các relationship:

    dim_products[category_id]      → dim_categories[category_id]     (M→1, single)
    fct_tiki_products[category_id] → dim_categories[category_id]     (M→1, single)
    fct_tiki_products[product_id]  → dim_products[product_id]        (M→1, single)
    fct_tiki_products[seller_id]   → dim_sellers[seller_id]          (M→1, single)
    fct_reviews[product_id]        → dim_products[product_id]        (M→1, single)

Để tránh "ambiguity" khi PBI nối fct_reviews → dim_categories, ưu tiên đi qua
dim_products làm bridge thay vì tạo FK trực tiếp từ fct_reviews.

Script JOIN sẵn metadata category (level/path/parent) vào dim_products + fct
để PBI dùng drill-down hierarchy mà không phải dựng DAX path; đồng thời bỏ
cột `category_name` denormalized khỏi dim_products (canonical name đã ở
dim_categories — tránh 2 nguồn lệch nhau khi Tiki đổi tên danh mục).
"""
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_endpoint='localhost:9000';")
con.execute("SET s3_access_key_id='admin';")
con.execute("SET s3_secret_access_key='minio_password';")
con.execute("SET s3_use_ssl=false;")
con.execute("SET s3_url_style='path';")

LAKEHOUSE = "s3://lakehouse/marts"


def _view(name, fname, fallback=None):
    """Đăng ký 1 parquet thành DuckDB view. fallback dùng cho fct_tiki_books
    (bucket cũ trước rename) — PBI vẫn nhận query name `fct_tiki_products`."""
    try:
        con.execute(
            f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{LAKEHOUSE}/{fname}')"
        )
    except Exception:
        if not fallback:
            raise
        con.execute(
            f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{LAKEHOUSE}/{fallback}')"
        )


_view("v_dim_categories",    "dim_categories.parquet")
_view("v_dim_sellers",       "dim_sellers.parquet")
_view("v_dim_products",      "dim_products.parquet")
_view("v_fct_reviews",       "fct_reviews.parquet")
_view("v_fct_tiki_products", "fct_tiki_products.parquet",
      fallback="fct_tiki_books.parquet")


# dim_sellers + fct_reviews không tham gia hierarchy → pass-through.
dim_sellers = con.execute("SELECT * FROM v_dim_sellers").fetchdf()
fct_reviews = con.execute("SELECT * FROM v_fct_reviews").fetchdf()


# dim_categories: thêm root_category_name = level-0 segment của path để PBI
# slicer "top-level danh mục" gọn 1 dòng thay vì build DAX FIRSTNONBLANK trên path.
dim_categories = con.execute("""
    SELECT
        c.*,
        SPLIT_PART(c.path, '/', 1) AS root_category_name
    FROM v_dim_categories c
""").fetchdf()


# dim_products: bỏ category_name (chuyển sang dim_categories làm single source),
# thêm category_level/parent_category_id/category_path để PBI hierarchy filter.
dim_products = con.execute("""
    SELECT
        p.product_id,
        p.sku,
        p.product_name,
        p.short_description,
        p.description,
        p.price,
        p.list_price,
        p.original_price,
        p.discount,
        p.discount_rate,
        p.rating_average,
        p.review_count,
        p.all_time_quantity_sold,
        p.quantity_sold,
        p.seller_id,
        p.seller_name,
        p.brand_id,
        p.brand_name,
        p.category_id,
        c.category_level,
        c.parent_category_id,
        c.path AS category_path,
        p.type,
        p.inventory_status,
        p.url_key,
        p.url_path,
        p.last_seen_at,
        p.dt
    FROM v_dim_products p
    LEFT JOIN v_dim_categories c USING (category_id)
""").fetchdf()


# fct_tiki_products: chỉ thêm category_path (cho aggregate theo nhánh) — không
# copy category_name vào fact để tránh nhân đôi storage; PBI sẽ lookup qua
# relationship fct → dim_categories.
fct_tiki_products = con.execute("""
    SELECT
        f.*,
        c.path AS category_path
    FROM v_fct_tiki_products f
    LEFT JOIN v_dim_categories c USING (category_id)
""").fetchdf()
