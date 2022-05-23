import json

from elasticsearch import Elasticsearch
from elasticsearch import NotFoundError
from opensearchpy import OpenSearch
from requests.auth import HTTPBasicAuth 

from tunip.constants import ELASTICSEARCH_AWS, ELASTICSEARCH_ORIGIN


class NotSupportedHttpAuthForElasticsearchProductType(Exception):
    pass

class NotSupportedElasticsearchProductType(Exception):
    pass


def init_elastic_client(service_config):
    if service_config.has_elastic_http_auth:
        http_auth = (
            service_config.elastic_username,
            service_config.elastic_password
        )
        if service_config.elastic_product == ELASTICSEARCH_ORIGIN:
            es = Elasticsearch(
                hosts=service_config.elastic_host,
                http_auth=http_auth
            )
        elif service_config.elastic_product == ELASTICSEARCH_AWS:
            raise NotSupportedHttpAuthForElasticsearchProductType(f"ElasticsearchProduct: {service_config.elastic_product} doesn't support http auth. yet.")
        else:
            raise NotSupportedElasticsearchProductType()
    else:
        if service_config.elastic_product == ELASTICSEARCH_ORIGIN:
            es = Elasticsearch(
                hosts=service_config.elastic_host
            )
        elif service_config.elastic_product == ELASTICSEARCH_AWS:
            es = OpenSearch(
                hosts=[{'host': service_config.elastic_host}]
            )
    return es


def iterate_all_documents(es, index, logger, pagesize=250, scroll_timeout="1m", hit_value="_source", **kwargs):
    """
    Helper to iterate ALL values from a single index
    Yields all the documents.
    """
    is_first = True
    while True:
        # Scroll next
        try:
            if is_first:  # Initialize scroll
                if 'body' in kwargs.keys():
                    result = es.search(
                        index=index, scroll=scroll_timeout, **kwargs)
                else:
                    result = es.search(index=index, scroll=scroll_timeout, **kwargs, body={
                        "size": pagesize
                    })
                is_first = False
            else:
                result = es.scroll(body={
                    "scroll_id": scroll_id,
                    "scroll": scroll_timeout
                })
        except NotFoundError as nfe:
            # Exit conditions
            # NotFoundError: NotFoundError(404, 'search_phase_execution_exception',
            #   'No search context found for id [262012]')
            # NotFoundError: NotFoundError(404, '{"succeeded":true,"num_freed":0}')
            logger.warning(f'{nfe.status_code}, {nfe.error}')
            if nfe.status_code == 404:
                is_first = True
        finally:
            scroll_id = result["_scroll_id"]
            hits = result["hits"]["hits"]
            # Stop after no more docs
            if not hits:
                logger.info(f"scorll_id={scroll_id} is successfully closed.")
                break
        # Yield each entry
        if(hit_value == 'all'):
            yield from (hit for hit in hits)
        else:
            yield from (hit[hit_value] for hit in hits)


def search_query_match(req_session, host, port, index, items, use_https=True, user=None, passwd=None, timeout=3):
    
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    body = {
        "query": {
            "match": {
                None
            }
        }
    }
    body["query"]["match"] = items

    if use_https:
        protocol = 'https://'
        auth = HTTPBasicAuth(user, passwd)
    else:
        protocol = 'http://'
        auth = None

    response = req_session.post(
        f"{protocol}{host}:{port}/{index}/_search",
        data=json.dumps(body),
        headers=headers,
        timeout=timeout,
        auth=auth
    )
    text = response.text
    return json.loads(text)


def search_query_ids(req_session, host, port, index, ids, use_https=True, user=None, passwd=None, timeout=3):
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    body = {"query": {"ids": {"values": ids}}}

    if use_https:
        protocol = 'https://'
        auth = HTTPBasicAuth(user, passwd)
    else:
        protocol = 'http://'
        auth = None

    response = req_session.post(
        f"{protocol}{host}:{port}/{index}/_search",
        data=json.dumps(body),
        headers=headers,
        timeout=timeout,
        auth=auth
    )
    return json.loads(response.text)
