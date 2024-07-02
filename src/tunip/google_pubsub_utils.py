import json
import logging

from pathlib import Path
from google.cloud import pubsub
from google.oauth2 import service_account

from tunip.google_cloud_utils import GoogleCloudClient
from tunip.service_config import ServiceLevelConfig


PUBSUB_SUBSCRIPTIONS_CONSUME = "pubsub.subscriptions.consume"


class MessagePubsubClient(GoogleCloudClient):
    def __init__(self, service_config: ServiceLevelConfig, project_id: str):
        super(MessagePubsubClient, self).__init__(service_config.config)
        self.credentials = service_account.Credentials.from_service_account_file(
            str(Path(service_config.resource_path) / f"{project_id}-pubsub.json")
        )


class MessagePublisherClient(MessagePubsubClient):
    def __init__(self, service_config: ServiceLevelConfig, project_id: str, topic_id: str):
        super(MessagePublisherClient, self).__init__(service_config, project_id)

        self.publish_client = pubsub.PublisherClient(credentials=self.credentials)
        self.topic_path = self.publish_client.topic_path(project_id, topic_id)

    def publish(self, **kwargs):
        payload = json.dumps(kwargs).encode('utf-8')
        future = self.publish_client.publish(self.topic_path, data=payload)
        future.result()


class MessageSubscriberClient(MessagePubsubClient):
    def __init__(self, service_config: ServiceLevelConfig, project_id: str, topic_id: str, subscription_id: str, logger: logging.Logger):
        super(MessageSubscriberClient, self).__init__(service_config, project_id)

        self.logger = logger

        self.subscribe_client = pubsub.SubscriberClient(credentials=self.credentials)
        self.topic_path = self.subscribe_client.topic_path(project_id, topic_id)

        self.subscription_path = f"projects/{project_id}/subscriptions/{subscription_id}"

        permissions_to_check = [
            "pubsub.subscriptions.consume",
        ]

        allowed_permissions = self.subscribe_client.test_iam_permissions(
            request={"resource": self.subscription_path, "permissions": permissions_to_check}
        )
        print(f"allowed_permissions={allowed_permissions}")

        assert PUBSUB_SUBSCRIPTIONS_CONSUME in str(allowed_permissions)

    def subscribe(self, user_callback, timeout=-1, **kwargs):

        def callback(message):
            self.logger.info(message.data)

            user_callback(payload=message.data, **kwargs)

            message.ack()

        future = self.subscribe_client.subscribe(self.subscription_path, callback)
        try:
            future.result(timeout)
        except KeyboardInterrupt:
            future.cancel()
        except TimeoutError:
            future.cancel()
            future.result()

    def __exit__(self):
        self.subscribe_client.close()
