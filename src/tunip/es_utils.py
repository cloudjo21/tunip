
from elasticsearch import NotFoundError

def es_iterate_all_documents(self, es, index, pagesize=250, scroll_timeout="1m", **kwargs):
        """
        Helper to iterate ALL values from a single index
        Yields all the documents.
        """
        is_first = True
        while True:
            # Scroll next
            try:
                if is_first:  # Initialize scroll
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
                self.logger.warn(f'{nfe.status_code}, {nfe.error}')
                if nfe.status_code == 404:
                    is_first = True
            finally:
                scroll_id = result["_scroll_id"]
                hits = result["hits"]["hits"]
                # Stop after no more docs
                if not hits:
                    self.logger.info(f"scorll_id={self.stat_dict['scroll_id']} is successfully closed.")
                    break
            # Yield each entry
            yield from (hit['_source'] for hit in hits)