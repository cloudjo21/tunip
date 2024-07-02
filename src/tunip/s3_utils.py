from boto3 import client


class S3(object):
    """
    s3 access를 위해 필요한 권한 설정 방법
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials

    예시)
    In ~/.aws/config
    [default]
    output = json
    region = ap-northeast-2
    aws_access_key_id = AAAA
    aws_secret_access_key = r/abcd/123
    [profile-dev]
    output = json
    region = ap-northwest-2
    aws_access_key_id = BBBB
    aws_secret_access_key = r/efgh/456
    """

    bucket_name: str

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.conn = client('s3')

    def upload_to_s3(self, src_path: str, dest_path: str = None) -> bool:
        """
        Upload given file to s3.
        """
        s3_client = self.conn
        if dest_path is None:
            dest_path = src_path.split('/')[-1]
        try:
            s3_client.upload_file(src_path, self.bucket_name, dest_path)
            return True
        except Exception as e:
            print(e)
            return False

    def upload_object(self, body, s3_key):
        """
        Upload object to s3 key
        """
        s3_client = self.conn
        return s3_client.put_object(Body=body, Key=s3_key)

    def download_file(self, src_path, dest_path):
        """
        Download a file given s3 prefix
        inside /tmp directory.
        Returns the download file path.
        """
        s3_client = self.conn
        try:
            s3_client.download_file(self.bucket_name, src_path, dest_path)
            return dest_path
        except Exception as e:
            print('Cannot download file', e)
            return

    def list_dir(self, dir_name):
        """
        Return all contents of a given dir in s3.
        Goes through the pagination to obtain all file names.
        """
        dir_name = dir_name.split('tmp/')[-1]
        paginator = self.conn.get_paginator('list_objects')
        s3_results = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=dir_name,
            PaginationConfig={'PageSize': 1000}
        )
        bucket_object_list = []
        for page in s3_results:
            if "Contents" in page:
                for key in page["Contents"]:
                    s3_file_name = key['Key'].split('/')[-1]
                    bucket_object_list.append(s3_file_name)
        return bucket_object_list