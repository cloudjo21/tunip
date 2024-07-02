import base64
import datetime as dt
import orjson

from pydantic import BaseModel
from typing import Dict, Optional

from tunip.logger import init_logging_handler_for_klass
from tunip.alert_utils import AlertNotifier
from tunip.log_pubsub import LogPublisher, LogSendRequest, LogSubscriber
from tunip.retriable_session import RetriableAdapter, RetriableSession
from tunip.service_config import ServiceLevelConfig, get_service_config


class LogRecord(BaseModel):
    pass


class LogTransmitter:

    def __init__(self, name: str, publisher: LogPublisher, resource: Optional[dict] = None, use_encoding: bool = True):
        self.name = name
        self.publisher = publisher
        self.resource = resource
        self.use_encoding = use_encoding

    def __call__(self, message: str):
        if self.use_encoding:
            encoded = base64.b64encode(message.encode("utf-8"))
        else:
            encoded = message
        self.publisher.publish(
            LogSendRequest(
                name=self.name,
                payload=encoded,
                timestamp=dt.datetime.now(),
                resource=self.resource,
            )
        )

    @staticmethod
    def apply(
        service_config: ServiceLevelConfig,
        name: str,
        project_id: str,
        topic_id: str,
        resource: dict,
    ) -> "LogTransmitter":
        return LogTransmitter(
            name, LogPublisher(service_config, project_id, topic_id), resource
        )


class LogReceiver:

    def __init__(
        self,
        name: str,
        subscriber: LogSubscriber,
        webhook_mapping: dict,
        custom_notifiers: Optional[Dict[str, AlertNotifier]] = None,
        use_decoding: bool = True,
        timeout: float = 0.2,
    ):
        self.name = name
        self.subscriber = subscriber
        self.use_decoding = use_decoding
        self.timeout = timeout  # 200ms

        self.notifiers: Dict[str, AlertNotifier] = self._init_webhook_notifiers(
            webhook_mapping, custom_notifiers
        )
        self.logger = init_logging_handler_for_klass(klass=self.__class__)

    def _init_webhook_notifiers(
        self, webhook_mapping: dict, custom_notifiers: Optional[dict]
    ):
        notifiers = dict()
        for channel, webhook_url in webhook_mapping.items():
            session: RetriableSession = RetriableSession.apply(
                RetriableAdapter(get_service_config()), webhook_url
            )
            if (custom_notifiers is not None) and (channel in custom_notifiers):
                notifiers[channel] = custom_notifiers[channel]
            else:
                notifiers[channel] = AlertNotifier(webhook_url, session)
        return notifiers

    def _notify(self, payload: str, channel: str):
        if self.use_decoding:
            message = orjson.loads(payload.decode('utf-8'))
            decoded = base64.b64decode(message["payload"]).decode('utf-8')
        else:
            decoded = payload.decode("utf-8")
        self.notifiers[channel].send(orjson.loads(decoded))

    def __call__(self, channel: str):
        self.subscriber.subscribe(
            user_callback=self._notify, timeout=self.timeout, channel=channel
        )


class LogTransmittersFactory:

    @staticmethod
    def apply(service_config: ServiceLevelConfig, name2topic: dict) -> Dict[str, LogTransmitter]:
        logging_topics = dict(map(lambda row: (row["name"], row["topic_id"]), name2topic))

        log_senders = dict()
        for name, topic_id in logging_topics.items():
            log_senders[name] = LogTransmitter(
                name=name,
                publisher=LogPublisher(
                    service_config=service_config,
                    project_id=service_config.config.get("gc.pubsub.project_id"),
                    topic_id=topic_id
                )
            )
        return log_senders
