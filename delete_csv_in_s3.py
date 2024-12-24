import boto3
from botocore.exceptions import ClientError

# 접두사가 raw-data, 파일 유형이 csv인 파일 제거 함수
def delete_s3_objects(bucket_name, prefix='raw-data', file_type='.csv'):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    deleted_count = 0

    try:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents'] if obj['Key'].endswith(file_type)]
                if objects_to_delete:
                    response = s3.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': objects_to_delete}
                    )
                    if 'Deleted' in response:
                        deleted_count += len(response['Deleted'])
                    if 'Errors' in response:
                        for error in response['Errors']:
                            print(f"Error deleting {error['Key']}: {error['Message']}")
    except ClientError as e:
        print(f"An error occurred: {e}")
    
    print(f"Total objects deleted: {deleted_count}")

# 사용 예
bucket_name = 'travel-de-storage'
delete_s3_objects(bucket_name)