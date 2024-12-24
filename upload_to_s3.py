import boto3
import os

# S3 클라이언트 생성
s3_client = boto3.client('s3')

# S3 버킷 이름과 객체(폴더) 이름 설정
bucket_name = 'travel-de-storage'
s3_folder = 'raw-data'

# 루트 폴더 경로 설정
root_local_folder = 'data/aihub/filtered'

# 모든 하위 폴더를 동적으로 가져오기
local_folders = [
    os.path.join(root, d) for root, dirs, _ in os.walk(root_local_folder) for d in dirs
]

def upload_folder(folder_path):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            local_path = os.path.join(root, file)

            # 'data/aihub/filtered' 이후의 경로를 S3 키로 사용
            relative_path = os.path.relpath(local_path, start=root_local_folder)

            # S3 경로에서 항상 슬래시('/')를 사용하도록 변환
            s3_key = f"{s3_folder}/{relative_path.replace(os.path.sep, '/')}"
            
            try:
                print(f"Uploading {local_path} to {s3_key}...")
                s3_client.upload_file(local_path, bucket_name, s3_key)
                print(f"Successfully uploaded: {s3_key}\n")
            except Exception as e:
                print(f"Error uploading {local_path}: {e}\n")

# 각 폴더에 대해 업로드 실행
for folder in local_folders:
    if os.path.exists(folder):
        upload_folder(folder)
    else:
        print(f"Folder does not exist: {folder}")

print("All files uploaded successfully.")

