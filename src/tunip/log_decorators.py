from functools import wraps

import itertools
import logging
import orjson
import re
import sys
import traceback

from tunip.log_trx import LogTransmitter


MAX_SIZE_MESSAGE = 1000

def log_error(logger: logging.Logger, sender: LogTransmitter=None):
    def errorcheck(original_function):

        @wraps(original_function)
        def wrapper(*args, **kwargs):
            try:
                return original_function(*args, **kwargs)
            except Exception as e:
            
                exc_type, exc_obj, tb = sys.exc_info()
                formatted_lines = traceback.format_exc().splitlines()
                num = ([idx for idx , i 
                        in enumerate(formatted_lines) 
                        if re.search(" line ", i) is not None ][-1])
                s  = [line.split(",") for line in formatted_lines[num:]]
                merged = list(itertools.chain(*s))
                error_message = orjson.dumps({
                    "error": str(exc_type.__name__),
                    "function": original_function.__qualname__,
                    "code": " ".join([string.strip() for string in merged[:2]]),
                    "message": "\n".join([string.strip() for string in merged[2:]])[:MAX_SIZE_MESSAGE]
                }).decode()
                errors =  traceback.extract_stack()
                errors = (
                    "".join(
                        [
                            f"Where : {str(i).split('FrameSummary file ')[1].split(',')[0]} \n Line : {str(i).split('FrameSummary file ')[1].split(',')[1]}\n {'--'*30} \n" 
                            for i in errors[:]
                        ]
                    )
                )
                logger.error(f"{'='*4} {original_function.__qualname__} {'='*4}\n{error_message}\n")

                if sender:
                    sender(message=error_message)
                raise e
        return wrapper
    return errorcheck


def async_log_error(logger: logging.Logger, sender: LogTransmitter=None):
    def errorcheck(original_function):

        @wraps(original_function)
        async def wrapper(*args, **kwargs):
            try:
                return await original_function(*args, **kwargs)
            except Exception as e:
            
                exc_type, exc_obj, tb = sys.exc_info()
                formatted_lines = traceback.format_exc().splitlines()
                num = ([idx for idx , i 
                        in enumerate(formatted_lines) 
                        if re.search(" line ", i) is not None ][-1])
                s  = [line.split(",") for line in formatted_lines[num:]]
                merged = list(itertools.chain(*s))
                error_message = orjson.dumps({
                    "error": str(exc_type.__name__),
                    "function": original_function.__qualname__,
                    "code": " ".join([string.strip() for string in merged[:2]]),
                    "message": "\n".join([string.strip() for string in merged[2:]])[:MAX_SIZE_MESSAGE]
                }).decode()
                errors =  traceback.extract_stack()
                errors = (
                    "".join(
                        [
                            f"Where : {str(i).split('FrameSummary file ')[1].split(',')[0]} \n Line : {str(i).split('FrameSummary file ')[1].split(',')[1]}\n {'--'*30} \n" 
                            for i in errors[:]
                        ]
                    )
                )
                logger.error(f"{'='*4} {original_function.__qualname__} {'='*4}\n{error_message}\n")

                if sender:
                    sender(message=error_message)
                raise e
        return wrapper
    return errorcheck
