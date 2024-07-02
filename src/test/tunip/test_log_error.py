import unittest
import logging

from tunip.logger import init_logging_handler
from tunip.log_decorators import log_error
from tunip.log_pubsub import LogPublisher
from tunip.log_trx import LogTransmitter
from tunip.service_config import get_service_config


LOGGER = init_logging_handler(name="tunip", level=logging.INFO)


class LogErrorDecoratorTest(unittest.TestCase):
    def test_log_error(self):
        class Operation:
            
            def sub(self,a,b):
                return a - b

            @log_error(logger=LOGGER)
            def ho(self, a, b):
                return self.sub(a,b)
                            
        with self.assertRaises(TypeError):
            Operation().ho(5,"1")

    def test_key_error(self):
        x = {}
        @log_error(logger=LOGGER)
        def get(index):
            return x[index]

        with self.assertRaises(KeyError):
            get(0)

    def test_log_transmit(self):
        x = {}
        service_config = get_service_config()
        transmitter = LogTransmitter(
            name="log_transmit",
            publisher=LogPublisher(
                service_config=service_config,
                project_id=service_config.config.get("gc.pubsub.project_id"),
                topic_id="dashi-log-v1"
            )
        )
        @log_error(logger=LOGGER, sender=transmitter)
        def get(index):
            return x[index]

        with self.assertRaises(KeyError):
            get(0)
