import os

import duckdb
import matplotlib.pyplot as plt
import seaborn as sns

# All connection details come from env so the script works both on host
# (localhost:9000) and inside the Airflow container (minio:9000) without
# code changes. Same defaults as dbt's profiles.yml.
S3_ENDPOINT = os.getenv("S3_ENDPOINT_HOST", "localhost:9000")
S3_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password")
LAKEHOUSE_BUCKET = os.getenv("LAKEHOUSE_BUCKET", "lakehouse")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")

MARTS_PREFIX = f"s3://{LAKEHOUSE_BUCKET}/marts"


def _connect():
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
    conn.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    return conn


def plot_top_products(conn, out_dir):
    # `fct_tiki_books.parquet` is the legacy filename produced by previous dbt
    # runs (model was renamed to `fct_tiki_products` on 2026-06-09). New runs
    # write `fct_tiki_products.parquet`; we prefer that and fall back to the
    # legacy file when only old marts are still on disk.
    try:
        sample = conn.execute(
            f"SELECT 1 FROM read_parquet('{MARTS_PREFIX}/fct_tiki_products.parquet') LIMIT 1"
        ).fetchone()
        marts_file = "fct_tiki_products.parquet" if sample is not None else "fct_tiki_books.parquet"
    except Exception:
        marts_file = "fct_tiki_books.parquet"

    query = f"""
        SELECT product_name, MAX(quantity_sold) AS quantity_sold
        FROM read_parquet('{MARTS_PREFIX}/{marts_file}')
        WHERE quantity_sold IS NOT NULL
        GROUP BY product_name
        ORDER BY quantity_sold DESC
        LIMIT 10
    """
    print(f"Querying top 10 products from {marts_file}...")
    df = conn.execute(query).df()
    if df.empty:
        print(f"  No data in {marts_file} — skipping top_10_products chart")
        return

    df["short_name"] = df["product_name"].apply(
        lambda x: (x[:45] + "...") if len(x) > 45 else x
    )

    plt.figure(figsize=(12, 7))
    ax = sns.barplot(data=df, x="quantity_sold", y="short_name", palette="viridis")
    plt.title("Top 10 Best-Selling Products on Tiki", fontsize=16, fontweight="bold", pad=20)
    plt.xlabel("Quantity Sold", fontsize=12)
    plt.ylabel("Product Name", fontsize=12)

    for p in ax.patches:
        width = p.get_width()
        plt.text(
            width + (df["quantity_sold"].max() * 0.01),
            p.get_y() + p.get_height() / 2,
            f"{int(width):,}",
            ha="left",
            va="center",
        )
    plt.tight_layout()
    out = os.path.join(out_dir, "top_10_products.png")
    plt.savefig(out, dpi=300)
    plt.close()
    print(f"  Saved: {out}")


def plot_top_categories(conn, out_dir):
    # dim_products carries category_name denormalized from dim_categories, so
    # we can count products per category without a join.
    query = f"""
        SELECT category_name, COUNT(DISTINCT product_id) AS product_count
        FROM read_parquet('{MARTS_PREFIX}/dim_products.parquet')
        WHERE category_name IS NOT NULL
        GROUP BY category_name
        ORDER BY product_count DESC
        LIMIT 10
    """
    print("Querying top 10 categories by product count...")
    df = conn.execute(query).df()
    if df.empty:
        print("  No data in dim_products — skipping top_10_categories chart")
        return

    plt.figure(figsize=(12, 7))
    ax = sns.barplot(data=df, x="product_count", y="category_name", palette="mako")
    plt.title("Top 10 Categories by Product Count", fontsize=16, fontweight="bold", pad=20)
    plt.xlabel("Distinct Products", fontsize=12)
    plt.ylabel("Category", fontsize=12)
    for p in ax.patches:
        width = p.get_width()
        plt.text(
            width + (df["product_count"].max() * 0.01),
            p.get_y() + p.get_height() / 2,
            f"{int(width):,}",
            ha="left",
            va="center",
        )
    plt.tight_layout()
    out = os.path.join(out_dir, "top_10_categories.png")
    plt.savefig(out, dpi=300)
    plt.close()
    print(f"  Saved: {out}")


def plot_top_sellers(conn, out_dir):
    # Rank sellers by a follower-weighted rating; filter out sellers with
    # too few reviews to avoid a 5-star outlier dominating the chart.
    query = f"""
        SELECT
            seller_name,
            avg_rating_point,
            total_follower,
            seller_review_count
        FROM read_parquet('{MARTS_PREFIX}/dim_sellers.parquet')
        WHERE total_follower IS NOT NULL
          AND avg_rating_point IS NOT NULL
          AND seller_review_count >= 50
        ORDER BY avg_rating_point * LN(GREATEST(total_follower, 1)) DESC
        LIMIT 10
    """
    print("Querying top 10 sellers...")
    df = conn.execute(query).df()
    if df.empty:
        print("  No qualifying sellers in dim_sellers — skipping top_10_sellers chart")
        return

    plt.figure(figsize=(12, 7))
    ax = sns.barplot(data=df, x="total_follower", y="seller_name", palette="rocket")
    plt.title(
        "Top 10 Sellers (rating-weighted follower count)",
        fontsize=16,
        fontweight="bold",
        pad=20,
    )
    plt.xlabel("Total Followers", fontsize=12)
    plt.ylabel("Seller", fontsize=12)
    for p, rating in zip(ax.patches, df["avg_rating_point"]):
        width = p.get_width()
        plt.text(
            width + (df["total_follower"].max() * 0.01),
            p.get_y() + p.get_height() / 2,
            f"{int(width):,}  ★{rating:.2f}",
            ha="left",
            va="center",
        )
    plt.tight_layout()
    out = os.path.join(out_dir, "top_10_sellers.png")
    plt.savefig(out, dpi=300)
    plt.close()
    print(f"  Saved: {out}")


def main():
    out_dir = "images"
    os.makedirs(out_dir, exist_ok=True)
    print(f"Connecting to MinIO at {S3_ENDPOINT} (bucket={LAKEHOUSE_BUCKET})...")
    conn = _connect()
    plot_top_products(conn, out_dir)
    plot_top_categories(conn, out_dir)
    plot_top_sellers(conn, out_dir)
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
