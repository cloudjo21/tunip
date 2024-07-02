import os

from pathlib import Path
from typing import Optional

from google.cloud import bigquery, storage
from google.cloud.storage.fileio import BlobWriter
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from tunip.config import Config
from tunip.env import NAUTS_HOME, NAUTS_LOCAL_ROOT


class GoogleCloudClient:
    def __init__(self, config: Config):
        self.credentials = service_account.Credentials.from_service_account_file(
            str(Path(NAUTS_HOME) / 'resources' / f"{config.get('gcs.project_id')}.json")
        )
        self.bucket_name = config.get('gcs.bucketname')


class BigQueryDriver(GoogleCloudClient):

    def __init__(self, config: Config):
        super(BigQueryDriver, self).__init__(config)
        self.client = bigquery.Client(self.credentials.project_id, self.credentials)
    
    def query(self, query):
        query_job = self.client.query(query)
        return query_job.result()


class StorageDriver(GoogleCloudClient):

    def __init__(self, config: Config):
        super(StorageDriver, self).__init__(config)
        self.client = storage.Client(self.credentials.project_id, self.credentials)

    def download(self, path):
        # Get list of files
        blobs = self.client.list_blobs(bucket_or_name=self.bucket_name, prefix=path.lstrip('/'))
        for blob in blobs:
            if blob.name.endswith("/"):
                continue
            file_split = blob.name.split("/")
            directory = f"{NAUTS_LOCAL_ROOT}/{'/'.join(file_split[0:-1])}"
            Path(directory).mkdir(parents=True, exist_ok=True)
            if file_split[-1] == '_SUCCESS':
                Path(directory + "/_SUCCESS").mkdir(parents=True, exist_ok=True)
            else:
                blob.download_to_filename(f"{NAUTS_LOCAL_ROOT}/{blob.name}")

    def write_blob_and_parent(self, path: str, contents):
        bucket = self.client.get_bucket(self.bucket_name)

        parent_path = '/'.join(path.split('/')[:-1])
        parent_blob = bucket.blob(parent_path.lstrip('/')+ "/")
        parent_writer = BlobWriter(parent_blob)
        parent_writer.close()

        target_blob = bucket.blob(path.lstrip('/'))
        target_writer = BlobWriter(target_blob)
        target_writer.write(contents)
        target_writer.close()

    def recursive_copy(self, source_bucket_name, source_prefix, dest_bucket_name, dest_prefix):
        """
        Recursively copy blobs from source to destination bucket.

        :param source_bucket_name: Name of the source bucket.
        :param source_prefix: Prefix in source bucket to identify blob hierarchy to copy.
        :param dest_bucket_name: Name of the destination bucket.
        :param dest_prefix: Prefix in destination bucket to store copied blobs.
        """
        source_bucket = self.client.get_bucket(source_bucket_name)
        dest_bucket = self.client.get_bucket(dest_bucket_name)

        # Iterate through all blobs with the given source prefix
        for blob in source_bucket.list_blobs(prefix=source_prefix.lstrip("/")):
            leaf_name = blob.name.split("/")[-1]
            dest_blob_name = f"{dest_prefix.lstrip('/')}/{leaf_name}"
            source_bucket.copy_blob(blob, dest_bucket, new_name=dest_blob_name)
            print(f"Copied blob from {blob.name} to {dest_blob_name}")

    def recursive_copy_from_local(self, source_local_prefix, dest_bucket_name, dest_blob_prefix):
        """Recursively uploads a directory and its contents to GCS."""
        dest_bucket = self.client.get_bucket(dest_bucket_name)

        for local_path, _, files in os.walk(source_local_prefix):
            for file in files:
                local_file_path = os.path.join(local_path, file)
                remote_path = os.path.join(dest_blob_prefix, os.path.relpath(local_file_path, source_local_prefix))
                # if dest_blob_prefix:
                #     remote_path = os.path.join(dest_blob_prefix, os.path.relpath(local_file_path, source_local_prefix))
                # else:
                #     remote_path = os.path.relpath(local_file_path, source_local_prefix)

                parent_path = '/'.join(remote_path.split('/')[:-1])
                parent_blob = dest_bucket.blob(parent_path.lstrip('/')+ "/")
                parent_writer = BlobWriter(parent_blob)
                parent_writer.close()

                blob = dest_bucket.blob(remote_path.lstrip("/"))
                blob.upload_from_filename(local_file_path)
                print(f"Uploaded {local_file_path} to {remote_path}")

    def recursive_copy_to_local(self, source_blob_prefix, source_bucket_name, dest_local_prefix):
        """Downloads a blob from the bucket."""
        bucket = self.client.get_bucket(source_bucket_name)

        # List all objects that start with the prefix.
        blobs = bucket.list_blobs(prefix=source_blob_prefix.lstrip("/"))

        for blob in blobs:
            leaf_name = blob.name.split("/")[-1]
            destination_file_name = os.path.join(dest_local_prefix, leaf_name)
            os.makedirs(os.path.dirname(destination_file_name), exist_ok=True)
            blob.download_to_filename(destination_file_name)
            print(f"Blob {blob.name} downloaded to {destination_file_name}.")
    
    def copy_file_to_local(self, source_blob_filepath, source_bucket_name, dest_local_filepath):
        source_bucket = self.client.get_bucket(source_bucket_name)

        os.makedirs(os.path.dirname(dest_local_filepath), exist_ok=True)
        # source_blob_filepath
        blob = source_bucket.blob(source_blob_filepath.lstrip("/"))
        blob.download_to_filename(dest_local_filepath)


class GoogleDriveUploader(GoogleCloudClient):
    def __init__(self, config: Config):
        super(GoogleDriveUploader, self).__init__(config)
        # SCOPES = ['https://www.googleapis.com/auth/drive']
        self.service = build('drive', 'v3', credentials=self.credentials)

    def upload(self, filepath: str, mime_type: str, target_folder_id: str) -> Optional[str]:
        file_id = None
        try:
            media = MediaFileUpload(filename=filepath, mimetype=mime_type)

            file_metadata = {
                "name": filepath,
                "parents": [target_folder_id]
            }

            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id",
                supportsAllDrives=True
            ).execute()

            file_id = file.get("id")
        finally:
            return file_id

    def create_folder(self, folder_name: str, parent_folder_id: str) -> Optional[str]:
        folder_id = None
        try:
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]
            else:
                raise Exception(f"create_folder: parent_folder_id is NOT set for {folder_name}")

            folder = self.service.files().create(
                body=folder_metadata,
                fields='id',
                supportsAllDrives=True
            ).execute()
            folder_id = folder.get('id')
        finally:
            return folder_id

    def list_folders(self, folder_name: Optional[str], parent_folder_id: Optional[str]) -> Optional[list]:
        folders = None
        try:
            query = f"mimeType='application/vnd.google-apps.folder' and trashed=False"
            clauses = []
            if folder_name:
                clauses.append(f"name='{folder_name}'")
            if parent_folder_id:
                clauses.append(f"'{parent_folder_id}' in parents")
            joint_query = " and ".join([query] + clauses)

            response = self.service.files().list(
                q=joint_query,
                spaces="drive",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True
            ).execute()
            folders = response.get("files")
        finally:
            return folders
