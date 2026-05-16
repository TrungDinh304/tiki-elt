import duckdb
import matplotlib.pyplot as plt
import seaborn as sns
import os

def main():
    # Create images directory if it doesn't exist
    os.makedirs('images', exist_ok=True)

    # Initialize DuckDB and load S3 extensions to read from MinIO
    print("Connecting to MinIO via DuckDB...")
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute("SET s3_endpoint='localhost:9000';")
    conn.execute("SET s3_access_key_id='admin';")
    conn.execute("SET s3_secret_access_key='minio_password';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")

    query = """
        SELECT product_name, MAX(quantity_sold) as quantity_sold 
        FROM read_parquet('s3://raw-data/marts/fct_tiki_books.parquet')
        WHERE quantity_sold IS NOT NULL
        GROUP BY product_name
        ORDER BY quantity_sold DESC 
        LIMIT 10
    """
    
    print("Querying top 10 books...")
    df = conn.execute(query).df()
    
    # Shorten the product names for better display on the Y-axis
    df['short_name'] = df['product_name'].apply(lambda x: (x[:45] + '...') if len(x) > 45 else x)

    # Plotting
    print("Generating bar chart...")
    plt.figure(figsize=(12, 7))
    
    # Check if there's data to plot
    if not df.empty:
        # Use sns.barplot
        ax = sns.barplot(data=df, x='quantity_sold', y='short_name', palette='viridis')
        plt.title('Top 10 Best-Selling Books on Tiki', fontsize=16, fontweight='bold', pad=20)
        plt.xlabel('Quantity Sold', fontsize=12)
        plt.ylabel('Book Name', fontsize=12)
        
        # Add data labels
        for p in ax.patches:
            width = p.get_width()
            plt.text(width + (df['quantity_sold'].max() * 0.01), p.get_y() + p.get_height() / 2, 
                     f'{int(width):,}', ha='left', va='center')
        
        plt.tight_layout()
        output_path = 'images/top_10_books.png'
        plt.savefig(output_path, dpi=300)
        print(f"Successfully saved chart to: {output_path}")
    else:
        print("No data found to plot!")

if __name__ == "__main__":
    main()
