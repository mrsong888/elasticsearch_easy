# !/usr/bin/env python
# *-* coding:utf-8 *-*

import six
from time import sleep
from functools import wraps
from types import FunctionType
from elasticsearch import helpers
from collections import defaultdict
from elasticsearch import Elasticsearch
from elasticsearch.helpers import BulkIndexError
from elasticsearch.exceptions import NotFoundError
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
            if attr.startswith('es'):
                attrs[attr] = check_index_or_type(val)
            if not attr.startswith('__') and isinstance(val, FunctionType):
                attrs[attr] = handle_time_out(val)
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
            temp_dict = {'mappings': {doc_type: {'properties': {}}}}
            for item in fields.items():
                temp_dict['mappings'][doc_type]['properties'].update({item[0]: {'type': item[1]}})
            result = self.es.indices.create(index=index, body=temp_dict)
            if 'acknowledged' in result and result['acknowledged']:
                return True
            return False
        else:
            Exist('index or doc_type may exist')

    def es_mappings_update(self, index, doc_type, fields):
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

    def es_sigle_doc_create(self, index, doc_type, id, body):
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

    def es_multi_doc_create(self, index, doc_type, body):
        '''
        :param index(string): index name
        :param doc_type(string): document type
        :param body(dict): {id: data} #data dict
        :return(boolean): True
        :exception: BulkIndexError
        '''
        actions = []
        source_dict = {'_op_type': 'create', '_index': index, '_type': doc_type}
        for id, data in body.items():
            temp_dict = source_dict.copy()
            temp_dict.update({'_id':  id, '_source': data})
            actions.append(temp_dict)
        result = helpers.bulk(self.es, actions=actions)
        if result[0] == len(body):
            return True

    def es_sigle_doc_update(self, index, doc_type, id, body):
        '''
        :param index(string): index name
        :param doc_type(string): document type
        :param id(string): document ID
        :param body(dict): {filed: value}
        :return(boolean): True or False
        '''
        if  self.es.exists(index=index, doc_type=doc_type, id=id):
            temp_dict = defaultdict(dict)
            temp_dict['doc'].update(body)
            cur_version = self.es.get(index=index, doc_type=doc_type, id=id)['_version']
            self.es.update(index=index, doc_type=doc_type, id=id, body=temp_dict, version=cur_version)
            return True
        return False

    def es_multi_doc_update(self, index, doc_type, body):
        '''
        :param index(string): index name
        :param doc_type(string): document type
        :param body(dict):{id: data} #data dict
        :return(boolean): True
        :exception: BulkIndexError
        '''
        actions= []
        source_dict = {'_op_type': 'update', '_index': index, '_type': doc_type}
        for id, data in body.items():
            temp_dict = source_dict.copy()
            temp_dict.update({'_id': id, 'doc': data})
            actions.append(temp_dict)
        result = helpers.bulk(self.es, actions=actions)
        if result[0] == len(body):
            return True

    def es_get_doc_counts(self,  index, doc_type, body=None):
        '''
        :param index(string): index name
        :param doc_type(string): document type
        :param body(dict): query body
        :return(int): counts
        '''
        counts = self.es.count(index=index, doc_type=doc_type, body=body)['count'] if body  \
            else self.es.count(index=index, doc_type=doc_type)['count']
        return counts

    def es_sigle_doc_delete(self, index, doc_type, id):
        '''
        :param index(string): index name
        :param doc_type(string): document type
        :param id(string): document ID
        :return(boolean): True or False
        '''
        if self.es.exists(index=index, doc_type=doc_type, id=id):
            cur_version = self.es.get(index=index, doc_type=doc_type, id=id)['_version']
            self.es.delete(index=index, doc_type=doc_type, id=id, version=cur_version)
            return True
        return False

    def es_multi_doc_delete(self, index, doc_type, ids):
        '''
        :param index(string): index name
        :param doc_type(string): document type
        :param ids(list): [id1, id2, id3]
        :return(boolean): True
        :exception: BulkIndexError
        '''
        actions = []
        source_dict = {'_op_type': 'delete', '_index': index, '_type': doc_type}
        for id in ids:
            temp_dict = source_dict.copy()
            temp_dict.update({'_id': id})
            actions.append(temp_dict)
        result = helpers.bulk(self.es, actions=actions)
        if result[0] == len(ids):
            return True

    def es_sigle_doc_info(self, index, doc_type, id):
        '''
        :param index(string): index name
        :param doc_type(string): document type
        :param id(string): document ID
        :return(dict): query result
        '''
        try:
            result = self.es.get(index=index, doc_type=doc_type, id=id)
            result['_source'].update({'id': id})
        except NotFoundError:
            return False
        return result['_source']

    def es_multi_doc_info(self, index, doc_type, ids):
        '''
        :param index(string):
        :param doc_type(string):
        :param ids(list):
        :return(list):[data1, data2]
        '''
        data_list = []
        body = {'ids': ids}
        result = self.es.mget(index=index, doc_type=doc_type, body=body)['docs']
        for info in result:
            try:
                if info['_id'] in set(ids):
                    info['_source'].update({'id': info['_id']})
                    data_list.append(info['_source'])
            except KeyError:
                continue
        return data_list

    def es_doc_search(self):
        pass

es = EsOperator(hosts='101.251.243.242')
# print es.get_index_info(index='info')
# print es.cat_indices()
# body={'age': 35}
# print es._sigle_doc_update(index='info', doc_type='person_info', id='134', body=body)
# print es.es_multi_doc_info(index='info', doc_type='person_info', ids=['13', '23'])
# print es.es_sigle_doc_info(index='info', doc_type='person_info', id='123')
# print es.es_sigle_doc_delete(index='info', doc_type='person_info', id='123')
# print es.es_multi_doc_delete(index='ticket_index', doc_type='tickets', ids=['0', '1', '2', '3'])
# body={'0': {'age': 18, 'num': 1}, '1': {'age': 18, 'num': 1}, '2': {'age': 18, 'num': 1}, '3': {'age': 18, 'num': 1}}
# print es.es_multi_doc_create(index='ticket_index', doc_type='tickets', body=body)
# print es.es_multi_doc_update(index='ticket_index', doc_type='tickets', body=body)
# print es.es_sigle_doc_create(index='info', doc_type='person_info', id='123', body={'name': 'mrsong', 'age': 30, 'address': 'hebei'})
# print es.es_sigle_doc_update(index='ticket_index', doc_type='tickets', id='0', body={'num': 2})
# print es.es_get_doc_counts(index='info', doc_type='person_info')
# print es.index_create(index='info5', doc_type='person_info5', fields={'name': 'text', 'age': 'integer'})
# print es.es_mappings_update(index='info5', doc_type='person_info5', fields={'address': 'text'})