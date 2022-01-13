import re

from importlib import import_module

from tunip.package_utils import package_contents
from tunip.path import lake, warehouse


def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def get_lake_path_by(path_type, user_name, domain_name, snapshot_dt=None):
    # e.g., get the class for corpus_serp
    name2classes = []
    for sub_module_name in package_contents(lake.__name__):
        sub_module = import_module(sub_module_name)
        name2classes.extend([(camel_to_snake(name), cls) for name, cls in sub_module.__dict__.items() if isinstance(cls, type)])
    if snapshot_dt:
        path_name2class = next(filter(lambda n2c: (path_type in n2c[0]) and ('domain_snapshot_path' in n2c[0]), name2classes), None)
        path_obj = path_name2class[1](user_name, domain_name, snapshot_dt)
    else:
        path_name2class = next(filter(lambda n2c: (path_type in n2c[0]) and ('domain_path' in n2c[0]), name2classes), None)
        path_obj = path_name2class[1](user_name, domain_name)
    return path_obj


def get_warehouse_path_by(path_type, user_name, source_type, domain_name, snapshot_dt=None):
    # e.g., get the class for entity_trie
    name2classes = [(camel_to_snake(name), cls) for name, cls in warehouse.__dict__.items() if isinstance(cls, type)]
    if snapshot_dt:
        path_name2class = next(filter(lambda n2c: (path_type in n2c[0]) and ('domain_snapshot_path' in n2c[0]), name2classes), None)
        path_obj = path_name2class[1](user_name, source_type, domain_name, snapshot_dt)
    else:
        path_name2class = next(filter(lambda n2c: (path_type in n2c[0]) and ('domain_path' in n2c[0]), name2classes), None)
        path_obj = path_name2class[1](user_name, source_type, domain_name)

    return path_obj
