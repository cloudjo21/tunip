from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from tunip.service_config import ServiceLevelConfig


class RetriableAdapter:

    def __init__(
            self,
            service_config: ServiceLevelConfig,
            timeout: int=3,
            pool_connections: int=10,
            pool_maxsize: int=10,
    ):
        self.service_config = service_config
        self.headers = {'Content-Type': 'application/json; charset=utf-8'}
        self.timeout = timeout

        num_retries = 2
        backoff_factor = 0.5
        status_forcelist = [500, 502, 504]
        retries = Retry(
            total=num_retries,
            read=num_retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist
        )
        self.adapter = HTTPAdapter(
            max_retries=retries,
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize
        )

    def mount(self, host: str) -> Session:
        session = Session()
        session.mount(host, self.adapter)
        return session


class RetriableSession:
    def __init__(self, host: str, session: Session, headers: dict):
        self.host = host
        self.session = session
        self.headers = headers

    def post(self, message: dict):
        return self.session.post(self.host, json=message, headers=self.headers)
        # return self.session.post(self.host, json=message, headers={'Content-Type': 'application/json'})

    @staticmethod
    def apply(adapter: RetriableAdapter, host: str) -> "RetriableSession":
        session: Session = adapter.mount(host)
        return RetriableSession(host, session, adapter.headers)


class RetrialAdaptable:

    def __init__(self, service_config: ServiceLevelConfig, host_url: str, connection_pool: RetriableAdapter):
        self.service_config = service_config
        self.request_session = RetriableSession.apply(connection_pool, host_url)
