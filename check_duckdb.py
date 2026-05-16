import duckdb

conn = duckdb.connect('tiki.duckdb')
print('=== DuckDB Tables ===')

# Get all tables
tables = conn.execute('''
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema = 'main'
''').fetchall()

if not tables:
    print('No tables found')
else:
    for table in tables:
        table_name = table[0]
        print(f'\nTable: {table_name}')
        
        # Count records
        result = conn.execute(f'SELECT COUNT(*) as cnt FROM {table_name}').fetchall()
        count = result[0][0]
        print(f'  Records: {count}')
        
        # Show columns
        cols = conn.execute(f'PRAGMA table_info({table_name})').fetchall()
        print(f'  Columns: {len(cols)}')
        for col in cols[:5]:
            col_name = col[1]
            col_type = col[2]
            print(f'    - {col_name} ({col_type})')
        if len(cols) > 5:
            print(f'    ... and {len(cols)-5} more')
        
        # Show sample data
        if count > 0:
            sample = conn.execute(f'SELECT * FROM {table_name} LIMIT 1').fetchall()
            print(f'  Sample row: {sample[0][:3]}...')
