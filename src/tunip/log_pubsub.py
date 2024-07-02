import datetime as dt
import logging

from pydantic import BaseModel, validator
from typing import Optional

from tunip.google_pubsub_utils import (
    MessagePublisherClient,
    MessageSubscriberClient,
)
from tunip.service_config import ServiceLevelConfig


class LogSendRequest(BaseModel):

    name: str
    payload: str
    timestamp: str
    resource: Optional[dict] = None

    @validator('timestamp', pre=True, allow_reuse=True)
    def convert_timestamp(value):
        if value is not None:
            return dt.datetime.isoformat(value)
        return dt.datetime.isoformat(dt.datetime.now())


class LogPublisher(MessagePublisherClient):
    def __init__(self, service_config: ServiceLevelConfig, project_id: str, topic_id: str):
        super(LogPublisher, self).__init__(service_config, project_id, topic_id)

    def publish(self, entity: LogSendRequest):
        super().publish(**entity.model_dump())


class LogSubscriber(MessageSubscriberClient):
    def __init__(self, service_config: ServiceLevelConfig, project_id: str, topic_id: str, subscription_id: str, logger: logging.Logger):
        super(LogSubscriber, self).__init__(service_config, project_id, topic_id, subscription_id, logger)

    def subscribe(self, user_callback, timeout, **kwargs):
        super().subscribe(user_callback, timeout, **kwargs)
