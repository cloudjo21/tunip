import boto3
import os
from pathlib import Path

from tunip.env import NAUTS_LOCAL_ROOT


class AWSClient:
    def __init__(self, config):
        self.s3 = boto3.client(
            's3'
            # aws_access_key_id=config.get('aws.access_key_id'),
            # aws_secret_access_key=config.get('aws.secret_access_key'),
            # region_name=config.get('aws.region')
        )
        self.bucket_name = config.get('s3.bucketname')

class StorageDriver(AWSClient):

    def __init__(self, config):
        super(StorageDriver, self).__init__(config)
        
    def download(self, path):
        paginator = self.s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=path.lstrip('/')):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith("/"):
                    continue
                file_split = key.split("/")
                directory = f"{NAUTS_LOCAL_ROOT}/{'/'.join(file_split[0:-1])}"
                Path(directory).mkdir(parents=True, exist_ok=True)
                if file_split[-1] == '_SUCCESS':
                    Path(directory + "/_SUCCESS").mkdir(parents=True, exist_ok=True)
                else:
                    self.s3.download_file(self.bucket_name, key, f"{NAUTS_LOCAL_ROOT}/{key}")

    def write_blob_and_parent(self, path: str, contents):
        """AWS S3 에서는 객체 폴더를 생성할 필요가 없습니다."""
        self.s3.put_object(Bucket=self.bucket_name, Key=path.lstrip('/'), Body=contents)

    def recursive_copy(self, source_bucket_name, source_prefix, dest_bucket_name, dest_prefix):
        paginator = self.s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=source_bucket_name, Prefix=source_prefix.lstrip('/')):
            for obj in page.get('Contents', []):
                copy_source = {'Bucket': source_bucket_name, 'Key': obj['Key']}
                key = obj['Key'].replace(source_prefix, dest_prefix, 1)
                self.s3.copy_object(CopySource=copy_source, Bucket=dest_bucket_name, Key=key)
                print(f"Copied object from {obj['Key']} to {key}")

    def recursive_copy_from_local(self, source_local_prefix, dest_bucket_name, dest_blob_prefix):
        for local_path, _, files in os.walk(source_local_prefix):
            for file in files:
                local_file_path = os.path.join(local_path, file)
                remote_path = os.path.join(dest_blob_prefix, os.path.relpath(local_file_path, source_local_prefix))
                self.s3.upload_file(local_file_path, dest_bucket_name, remote_path.lstrip("/"))
                print(f"Uploaded {local_file_path} to {remote_path}")

    def recursive_copy_to_local(self, source_blob_prefix, source_bucket_name, dest_local_prefix):
        paginator = self.s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=source_bucket_name, Prefix=source_blob_prefix.lstrip('/')):
            for obj in page.get('Contents', []):
                destination_file_name = os.path.join(dest_local_prefix, obj['Key'].split("/")[-1])
                os.makedirs(os.path.dirname(destination_file_name), exist_ok=True)
                self.s3.download_file(source_bucket_name, obj['Key'], destination_file_name)
                print(f"Object {obj['Key']} downloaded to {destination_file_name}.")

    def copy_file_to_local(self, source_blob_filepath, source_bucket_name, dest_local_filepath):
        os.makedirs(os.path.dirname(dest_local_filepath), exist_ok=True)
        self.s3.download_file(source_bucket_name, source_blob_filepath.lstrip("/"), dest_local_filepath)