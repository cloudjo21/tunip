import orjson
import requests

from tunip.logger import init_logging_handler_for_klass
from tunip.retriable_session import RetriableSession
from tunip.service_config import get_service_config


def send_slack_alert(context: dict):
    slack_message = (
        f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}"
    )
    session = requests.Session()
    alert_url = get_service_config().config["slack_alert_url"]
    session.post(url=alert_url, json={"text": slack_message})


class AlertNotifier:
    def __init__(self, webhook_url: str, session: RetriableSession):
        self.webhook_url = webhook_url
        self.session = session
        self.logger = init_logging_handler_for_klass(klass=self.__class__)

    def send(self, message: dict):
        self.logger.info(
            f"{__class__.__name__} would send {message} to {self.webhook_url}"
        )

        alert_message = "\n".join(
            [
                f"*Error* : {message['error']}",
                f"*Function* : {message['function']}",
                f"*Code* : {message['code']}",
                f"*Message* : {message['message']}",
            ]
        )

        response = self.session.post(
            message={
                "blocks": [
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": "*DEV_STAGE*"},
                            {"type": "mrkdwn", "text": "*NOTICE_TYPE*"},
                            {
                                "type": "plain_text",
                                "text": f"{'=>'.join(filter(lambda x: x, [get_service_config().service_level.upper(), get_service_config().gcp_project_id or None]))}",
                            },
                            {
                                "type": "plain_text",
                                "text": "ERROR ALERT",
                            },
                        ],
                    },
                    {"type": "divider"},
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": alert_message},
                    },
                ]
            }
        )
        if response.status_code != 200:
            raise ValueError(
                f"Request to Slack returned an error {response.status_code}, the response is:\n{response.text}"
            )

        self.logger.info(
            f"{__class__.__name__} completes send {message} to {self.webhook_url}"
        )
