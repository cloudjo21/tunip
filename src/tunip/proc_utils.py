import gc
import logging
from typing import Optional

import psutil


class Runtime:
    def __init__(self):
        self.psutil_process: Optional[psutil.Process] = None

    def get_memory_usage_mb(self):
        process = self._psutil_process()
        mem = process.memory_info().rss / 1024 ** 2
        return mem

    def log_memory_usage(self, message: str = None):
        mem = self.get_memory_usage_mb()
        message = message or 'Memory Usage (MB): %s'
        logging.info(message, mem)

    def _psutil_process(self):
        if not self.psutil_process:
            self.psutil_process = psutil.Process()
        return self.psutil_process

    def gc_collect(self, log=True):
        if log:
            mem_before = self.get_memory_usage_mb()
            gc_count_before = gc.get_count()
            ret = gc.collect()
            gc_count_after = gc.get_count()
            mem_after = self.get_memory_usage_mb()

            logging.info({
                'message': f'garbage collected. (mem: {mem_before} -> {mem_after} MB)',
                'collected_unreachable_count': ret,
                'memory_usage_before': mem_before,
                'memory_usage_after': mem_after,
                'gc_count_before': gc_count_before,
                'gc_count_after': gc_count_after,
            })
        else:
            gc.collect()
