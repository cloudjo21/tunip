import json
from typing import Any


class Config:
    def __init__(self, json_path):
        with open(json_path, mode='r') as io:
            params = json.loads(io.read())
        self.__dict__.update(params)

    def __call__(self, attr_name):
        """
        it can provides the nested attributes by '.' separated attribute key
        """
        return self.dict.get(attr_name)
    
    def __getitem__(self, attr_name):
        return self.dict.get(attr_name)
    
    def __setitem__(self, _key: str, _value: Any):
        return self.dict.update({_key: _value})
    
    def get(self, attr_name):
        return self.__call__(attr_name)

    def save(self, json_path):
        with open(json_path, mode='w') as io:
            json.dump(self.__dict__, io, indent=4)

    def update(self, json_path):
        with open(json_path, mode='r') as io:
            params = json.loads(io.read())
        self.__dict__.update(params)

    @property
    def dict(self):
        return self.__dict__
