import json

from elasticsearch import NotFoundError


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


def search_query_match(req_session, host, port, index, items, use_https=True, timeout=3):
    
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    body = {
        "query": {
            "match": {
                None
            }
        }
    }
    body["query"]["match"] = items
    protocol = 'https://' if use_https else 'http://'
    response = req_session.post(
        f"{protocol}{host}:{port}/{index}/_search",
        data=json.dumps(body),
        headers=headers,
        timeout=timeout
    )
    text = response.text
    return json.loads(text)


def search_query_ids(req_session, host, port, index, ids, use_https=True, timeout=3):
    headers = {'Content-Type': 'application/json; charset=utf-8'}
    body = {"query": {"ids": {"values": ids}}}
    protocol = 'https://' if use_https else 'http://'
    response = req_session.post(
        f"{protocol}{host}:{port}/{index}/_search",
        data=json.dumps(body),
        headers=headers,
        timeout=timeout
    )
    return json.loads(response.text)
