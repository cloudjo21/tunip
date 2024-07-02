import time

from functools import wraps


def time_it(logger, verbose=False):
    def real_timeit(method):
        @wraps(method)
        def timed(*args, **kw):
            ts = time.time()
            result = method(*args, **kw)
            te = time.time()
            if verbose is False:
                logger.info(f'[%r] Elapsed Time: %2.3f ms' % (method.__qualname__, (te - ts) * 1000))
            else:
                logger.info(f'[%r.%r] Elapsed Time: %2.3f ms' % (method.__module__, method.__qualname__, (te - ts) * 1000))
            return result
        return timed
    return real_timeit


def atime_it(logger, verbose=False):
    def real_timeit(method):
        @wraps(method)
        async def timed(*args, **kw):
            ts = time.time()
            result = await method(*args, **kw)
            te = time.time()
            if verbose is False:
                logger.info(f'[%r] Elapsed Time: %2.3f ms' % (method.__qualname__, (te - ts) * 1000))
            else:
                logger.info(f'[%r.%r] Elapsed Time: %2.3f ms' % (method.__module__, method.__qualname__, (te - ts) * 1000))
            return result
        return timed
    return real_timeit
