# !/usr/bin/env python
# *-* coding:utf-8 *-*

import six
from time import sleep
from functools import wraps
from types import FunctionType
from elasticsearch import helpers
from collections import defaultdict
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionTimeout

class Exist(Exception):
    pass

class NotExist(Exception):
    pass

def check_index_or_type(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        instance = args[0]
        if instance.es.indices.exists(index=kwargs['index']) and instance.es.indices.exists_type(index=kwargs['index'], doc_type=kwargs['doc_type']):
            return func(*args, **kwargs)
        raise NotExist('index or doc_type do not exist')
    return wrapper

def handle_time_out(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        num = 0
        while True:
            try:
                result = func(*args, **kwargs)
            except ConnectionTimeout:
                num += 1
                if num == 3:
                    raise
                sleep(2)
            return result
    return wrapper

class CatchException(type):

    def __new__(cls, classname, bases, attrs):
        for attr, val in attrs.items():
            if attr.startswith('_') and not attr.endswith('_'):
                attrs[attr] = check_index_or_type(val)
            if not attr.startswith('__') and isinstance(val, FunctionType):
                attrs[attr] = check_index_or_type(val)
        return type.__new__(cls, classname, bases, attrs)


class EsOperator(six.with_metaclass(CatchException, object)):

    Es = Elasticsearch

    #a part of field type that often appear
    type_set = {'text', 'keyword', 'long', 'integer', 'short', 'byte', 'double', 'float', 'half_float', 'scaled_float',
                'date', 'boolean', 'ip'}

    def __init__(self, hosts=None, **kwargs):
        self.es = self.Es(hosts, **kwargs)

    def get_index_info(self, index):
        '''
        :param index(string): index name
        :return(dict): index info
        '''
        return self.es.indices.get(index=index)

    def index_create(self, index, doc_type, fields):
        '''
        :param index(string): index name
        :param doc_type(string): document type
        :param fields(dict): field map type {name:type}
        :return(boolean): True or False
        '''
        if filter(lambda item: item[1] not in self.type_set, fields.items()):
            raise Exception('field type may be not wrong')
        if not (self.es.indices.exists(index=index) and self.es.indices.exists_type(index=index, doc_type=doc_type)):
            temp_dict = {'mappings': {'person_info': {'properties': {}}}}
            for item in fields.items():
                temp_dict['mappings']['person_info']['properties'].update({item[0]: {'type': item[1]}})
            result = self.es.indices.create(index=index, doc_type=doc_type, body=temp_dict)
            if 'acknowledged' in result and result['acknowledged']:
                return True
            return False
        else:
            Exist('index or doc_type may exist')

    def _mappings_update(self, index, doc_type, fields):
        '''
        :param index(string): index name
        :param doc_type(string): document name
        :param fields(dict): field map type {name:type}
        :return(boolean): True or False
        '''
        temp_dict = {'properties': {}}
        for item in fields.items():
            temp_dict['properties'].update({item[0]: {'type': item[1]}})
        result = self.es.indices.put_mapping(index=index, doc_type=doc_type, body=temp_dict)
        if 'acknowledged' in result and result['acknowledged']:
            return True
        return False

    def cat_indices(self):
        return self.es.cat.indices()

    def _sigle_doc_create(self, index, doc_type, id, body):
        '''
        :param index(string): index name
        :param doc_type(string): document name
        :param id(string): document ID
        :param body(dict) document content
        :return(boolean): True or False
        '''
        if not self.es.exists(index=index, doc_type=doc_type, id=id):
            self.es.create(index=index, doc_type=doc_type, body=body, id=id)
            return True
        return False

    def multi_doc_create(self):
        pass

    def _sigle_doc_update(self, index, doc_type, id, body):
        '''
        :param index(string):
        :param doc_type(string):
        :param id(string):
        :param body(dict):
        :return(boolean):
        '''
        if  self.es.exists(index=index, doc_type=doc_type, id=id):
            temp_dict = defaultdict(dict)
            temp_dict['doc'].update(body)
            cur_version = self.es.get(index=index, doc_type=doc_type, id=id)['_version']
            self.es.update(index=index, doc_type=doc_type, id=id, body=temp_dict, version=cur_version)
            return True
        return False

    def multi_doc_update(self):
        pass

es = EsOperator(hosts='101.251.243.242')
# print es.get_index_info(index='info')
# print es.cat_indices()
body={'age': 35}
print es._sigle_doc_update(index='info', doc_type='person_info', id='134', body=body)

