import inspect
import pytest
import tunip.file_utils as fu

from tunip.env import NAUTS_LOCAL_ROOT
from tunip.service_config import get_service_config


@pytest.fixture
def s3_handler():
    service_config = get_service_config()
    return fu.services.get("S3", config=service_config.config)

@pytest.fixture
def service_config():
    service_config = get_service_config()
    return service_config

def test_s3_handler_list(s3_handler, service_config):
    file_list = s3_handler.list_dir(f"/user/{service_config.username}")
    assert len(file_list) > 0


def test_s3_handler_copy_files(s3_handler, service_config):
    try:
        s3_handler.copy_files(
            source=f"/user/{service_config.username}/mart/my_domain/my_schema/my_phase/20241217_110832_943541/test",
            target=f"/user/{service_config.username}/mart/ttest22"
        )
    except Exception as e:
        pytest.fail(f'{inspect.currentframe().f_code.co_name}: {str(e)}')


def test_s3_handler_download(s3_handler, service_config):
    try:
        s3_handler.download(
            path=f"/user/{service_config.username}/mart/my_domain/my_schema/my_phase/20241217_110832_943541/test"
        )
    except Exception as e:
        pytest.fail(f'{inspect.currentframe().f_code.co_name}: {str(e)}')


def test_s3_handler_transfer_from_local(s3_handler, service_config):
    try:
        s3_handler.transfer_from_local(
            source_local_dirpath=f"{NAUTS_LOCAL_ROOT}/user/{service_config.username}/mart/my_domain/my_schema/my_phase/20241217_110832_943541/test",
            dest_dirpath=f"/user/{service_config.username}/test333"
        )
    except Exception as e:
        pytest.fail(f'{inspect.currentframe().f_code.co_name}: {str(e)}')


def test_s3_handler_transfer_to_local(s3_handler, service_config):
    try:
        s3_handler.transfer_to_local(
            source_dirpath=f"/user/{service_config.username}/mart/my_domain/my_schema/my_phase/20241217_110832_943541/test",
            dest_local_dirpath=f"{NAUTS_LOCAL_ROOT}/user/{service_config.username}/test333"
        )
    except Exception as e:
        pytest.fail(f'{inspect.currentframe().f_code.co_name}: {str(e)}')