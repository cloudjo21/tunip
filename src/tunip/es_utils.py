
from elasticsearch import NotFoundError

#TODO: add batch record module

def iter_search_scroll(es, index, stat_dict, logger):

    has_next = True

    try:
        if stat_dict['scroll_id'] is None:
            res_search = es.search(
                index=index,
                scroll=stat_dict['scroll_timeout'],
                body={
                    'size': stat_dict['scroll_size'],
                    'query': {'match_all': {}},
                    'sort': ['_doc']
                }
            )
        else:
            res_search = es.scroll(
                scroll_id=stat_dict['scroll_id'],
                body={
                    'scroll': stat_dict['scroll_timeout'],
                    'scroll_id': stat_dict['scroll_id']
                }
            )
    except NotFoundError as nfe:
        # Exit conditions
        # NotFoundError: NotFoundError(404, 'search_phase_execution_exception',
        #   'No search context found for id [262012]')
        # NotFoundError: NotFoundError(404, '{"succeeded":true,"num_freed":0}')
        logger.warning(f'{nfe.status_code}, {nfe.error}')
        if nfe.status_code == 404:
            has_next = False
    finally:
        if res_search and has_next is True:
            if len(res_search['hits']['hits']) < 1:
                has_next = False
                es.clear_scroll(scroll_id=stat_dict['scroll_id'])
                # Exit conditions
                # {'succeeded': True, 'num_freed': 1}
                logger.info(f"scorll_id={stat_dict['scroll_id']} is successfully closed.")

    scroll_id = res_search['_scroll_id']
    total = res_search['hits']['total']['value']
    page_size = len(res_search['hits']['hits'])

    res_json = {
        'scroll_id': scroll_id,
        'total': total,
        'page_size': page_size,
        'scroll_timeout': stat_dict['scroll_timeout'],
        'hits': res_search['hits']['hits'],
        'has_next': has_next,
        'page_num': stat_dict['page_num'] + 1 if has_next else stat_dict['page_num']
    }
    return res_json
