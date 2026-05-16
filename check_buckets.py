import boto3

s3 = boto3.client('s3', endpoint_url='http://localhost:9000', 
                  aws_access_key_id='admin', 
                  aws_secret_access_key='minio_password', 
                  region_name='us-east-1')

print('=== MinIO Buckets ===')
buckets = s3.list_buckets()
for b in buckets.get('Buckets', []):
    bucket_name = b['Name']
    print(f'\nBucket: {bucket_name}')
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=100)
        if 'Contents' in response:
            for obj in response['Contents']:
                size = obj['Size']
                key = obj['Key']
                print(f'  - {key} ({size} bytes)')
        else:
            print('  (empty)')
    except Exception as e:
        print(f'  Error: {e}')
