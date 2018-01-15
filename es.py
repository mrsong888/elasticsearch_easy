# !/usr/bin/env python
# *-* coding:utf-8 *-*

import six
from collections import defaultdict
from elasticsearch import Elasticsearch
from elasticsearch import helpers

def decorator(func):
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except:
            pass
    return wrapper

class Exist(Exception):
    pass

class NotExist(Exception):
    pass

class CatchException(type):
    pass


class EsOperator(object):

    Es = Elasticsearch

    #a part of field type that often appear
    type_set = {'text', 'keyword', 'long', 'integer', 'short', 'byte', 'double', 'float', 'half_float', 'scaled_float',
                'date', 'boolean', 'ip'}

    def __init__(self, hosts=None, **kwargs):
        self.es = self.Es(hosts, **kwargs)
        self.version = 0

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
            raise Exist('index or doc_type exist')

    def _mappings_update(self, index, doc_type, fields):
        '''
        :param index(string): index name
        :param doc_type(string): document name
        :param fields(dict): field map type {name:type}
        :return(boolean): True or False
        '''
        if self.es.indices.exists(index=index) and self.es.indices.exists_type(index=index, doc_type=doc_type):
            temp_dict = {'properties': {}}
            for item in fields.items():
                temp_dict['properties'].update({item[0]: {'type': item[1]}})
            result = self.es.indices.put_mapping(index=index, doc_type=doc_type, body=temp_dict)
            if 'acknowledged' in result and result['acknowledged']:
                return True
            return False
        else:
            raise NotExist('index or doc_type do not exist')

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
        if self.es.indices.exists(index=index) and self.es.indices.exists_type(index=index, doc_type=doc_type):
            if not self.es.exists(index=index, doc_type=doc_type, id=id):
                self.es.create(index=index, doc_type=doc_type, body=body, id=id)
                return True
            return False
        else:
            raise NotExist('index or doc_type do not exist')

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
        if self.es.indices.exists(index=index) and self.es.indices.exists_type(index=index, doc_type=doc_type):
            if not self.es.exists(index=index, doc_type=doc_type, id=id):
                temp_dict = defaultdict(dict)
                temp_dict['doc'].update(body)
                self.es.update(index=index, doc_type=doc_type, id=id, body=temp_dict)
        else:
            raise NotExist('index or doc_type do not exist')


    def multi_doc_update(self):
        pass
