import pytest

from tunip.log_pubsub import LogPublisher
from tunip.log_trx import LogTransmitter
from tunip.service_config import get_service_config


def test_init_log_trasmitter():
    service_config = get_service_config()
    transmitter = LogTransmitter(
        name="log_transmit",
        publisher=LogPublisher(
            service_config=service_config,
            project_id=service_config.config.get("gc.pubsub.project_id"),
            topic_id="dashi-log-v1"
        )
    )
    assert transmitter is not None