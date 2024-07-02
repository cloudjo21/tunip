import re
import elasticsearch
import json

from elasticsearch import Elasticsearch
from elasticsearch import NotFoundError
from opensearchpy import OpenSearch
from requests.auth import HTTPBasicAuth 
from typing import Optional

from tunip.constants import ELASTICSEARCH_AWS, ELASTICSEARCH_ORIGIN, INDEX_ALIAS_SNAPSHOT_PATTERN


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
                hosts=[service_config.elastic_host]
            )
    return es


def iterate_all_documents(es, query, index, logger, scroll_timeout="1m", hit_value="_source", **kwargs):
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
                        query=query,
                        index=index, scroll=scroll_timeout, **kwargs)
                else:
                    result = es.search(query=query, index=index, scroll=scroll_timeout, **kwargs)
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
                logger.debug(f"scorll_id={scroll_id} is successfully closed.")
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


def search_knn_query(es, index_name, query_field_name, query_vector, source_fields: list, top_k=10, num_candidates=100):
    query = {
        "field": query_field_name,
        "query_vector": query_vector,
        "k": top_k,
        "num_candidates": num_candidates
    }
    # :ObjectApiResponse
    return es.search(index=index_name, knn=query, size=top_k, _source=source_fields)


def search_query(es, index_name: str, query: dict, source_fields: list, strict_fields=True, size=1000):
    result = es.search(query=query, index=index_name, _source=source_fields, size=size)
    rows = []
    for r in result["hits"]["hits"]:
        row = dict()
        # row = tuple()
        for field in source_fields:
            if (strict_fields is False) and (field not in r["_source"]):
                continue
            row.update({field: r["_source"][field]})
            # row += (r["_source"][field],)
        row.update({"_score": r["_score"]})
        rows.append(row)
    return rows


def search_filter_many_values(es, index_name: str, query_field_name: str, values: list, source_fields: list, size=1000):
    query = {
        "bool": {
            "must": [
                {"terms": {query_field_name: values}}
            ]
        }
    }
    result = es.search(query=query, index=index_name, _source=source_fields, size=size)
    rows = []
    for r in result["hits"]["hits"]:
        row = dict()
        # row = tuple()
        for field in source_fields:
            row.update({field: r["_source"][field]})
            # row += (r["_source"][field],)
        rows.append(row)
    return rows


def search_filter_many_values_multi_field(es, index_name: str, field2values: dict, source_fields: list, size=1000):
    terms_list = [{"terms": {k: v}} for k, v in field2values.items()]
    query = {
        "bool": {
            "must": terms_list
        }
    }
    result = es.search(query=query, index=index_name, _source=source_fields, size=size)
    rows = []
    for r in result["hits"]["hits"]:
        row = dict()
        # row = tuple()
        for field in source_fields:
            row.update({field: r["_source"][field]})
            # row += (r["_source"][field],)
        rows.append(row)
    return rows


def add_or_update_alias(es, index_name: str, alias: str) -> bool:
    try:
        response = es.indices.put_alias(index=index_name, name=alias)
        status = response.body["acknowledged"]
    except elasticsearch.NotFoundError:
        status = False
    except elasticsearch.BadRequestError:
        status = False
    finally:
        return status

def delete_alias(es, index_name: str, alias: str) -> bool:
    try:
        response = es.indices.delete_alias(index=index_name, name=alias)
        status = response.body["acknowledged"]
    except elasticsearch.NotFoundError:
        status = False
    except elasticsearch.BadRequestError:
        status = False
    finally:
        return status

def delete_index(es, index_name: str) -> bool:
    try:
        response = es.indices.delete(index=index_name)
        status = response.body["acknowledged"]
    except elasticsearch.NotFoundError:
        status = False
    except elasticsearch.BadRequestError:
        status = False
    finally:
        return status

def create_index(es, index_name: str, mappings: dict):
    return es.indices.create(index=index_name, mappings=mappings)

def list_indexes_of(es, alias: str, elastic_host: str, desc: bool=False) -> Optional[list]:
    from requests import Session
    with Session() as sess:
        order_by = "desc" if desc else "asc"
        response = sess.get(
            url=f"{elastic_host}/_cat/indices/{alias}*?h=index&s=creation.date:{order_by}"
        )
        if response.status_code == 200:
            index_list = response.text.split()
            alias_pattern = re.compile(INDEX_ALIAS_SNAPSHOT_PATTERN.format(alias=alias))

            matched_index_list = list(filter(alias_pattern.match, index_list))
            return matched_index_list
        else:
            return None
    # NOT WORKING in elasticsearch-py
    # text_response = es.cat.aliases(name=alias, h="index", s="creation.date:asc")
    # text_response.split()
