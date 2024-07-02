from collections.abc import Iterator
from itertools import islice


def chunked_iterators(itr_data, len_data, shard_size):
    assert isinstance(itr_data, Iterator) is True
    for _ in range(0, len_data, shard_size):
        yield islice(itr_data, shard_size)
