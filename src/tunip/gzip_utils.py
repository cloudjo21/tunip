import gzip
import json


def write_json_object(path, obj):
    with gzip.open(path, 'wt', encoding='UTF-8') as zipfile:
        json.dump(obj, zipfile)


def read_json_object(path):
    with gzip.open(path, 'rt', encoding='UTF-8') as zipfile:
        return json.load(zipfile)
