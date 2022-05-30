from functools import wraps


def timeit(logger):
    def real_timeit(method):
        @wraps(method)
        def timed(*args, **kw):
            ts = time.time()
            result = method(*args, **kw)
            te = time.time()
            logger.info(f'%r.%r  %2.3f ms' % \
                    (method.__module__, method.__qualname__, (te - ts) * 1000))
            return result
        return timed
    return real_timeit

