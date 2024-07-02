import concurrent
import json
import time
import unittest

from tunip.google_pubsub_utils import (
    MessagePublisherClient,
    MessageSubscriberClient,
)
from tunip.logger import init_logging_handler_for_klass
from tunip.service_config import get_service_config


class GooglePubsubTest(unittest.TestCase):

    def setUp(self):
        self.service_config = get_service_config()
        self.logger = init_logging_handler_for_klass(klass=self.__class__)
        self.publisher = MessagePublisherClient(self.service_config)
        self.subscriber = MessageSubscriberClient(self.service_config, self.logger)

    def test_publish_and_subscribe_single_message(self):

        # self.publisher.publish(messege_id='id_test_publish_and_subscribe_single_message', content_num=10000)
        self.publisher.publish(sid='abcdef-ghijkl-mnopqr', activity_tag='stem', updated_at='20221209_000000_000000')

        time.sleep(2)

        def username_and_payload(payload, username):
            self.logger.debug(f"username: {username}")
            self.logger.debug(f"payload: {payload}")
            print(f"username: {username}")
            print(f"payload: {payload}")

        with self.assertRaises(concurrent.futures._base.TimeoutError):
            self.subscriber.subscribe(user_callback=username_and_payload, timeout=2, username=self.service_config.username)
