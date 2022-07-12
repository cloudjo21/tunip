import unittest

from tunip import S3


class S3Test(unittest.TestCase):

    def setUp(self) -> None:
        bucket_name = 'aiteam-s3'
        self.s3 = S3(bucket_name)

    def test_s3_crd(self):
        src = '/Users/jhjeon/IdeaProjects/kwazii/README.md'
        dest = 'keywords/README.md'

        assert self.s3.upload_to_s3(src, dest)

        files = self.s3.list_dir('keywords')
        assert 'README.md' in files

        dest_path = self.s3.download_file('keywords/README.md', '/tmp/README2.md')
        with open(dest_path, 'r') as f:
            assert 'What is this repository for' in f.read()


if __name__ == '__main__':
    unittest.main()
