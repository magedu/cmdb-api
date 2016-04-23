import os
import socket
import requests
from tornado.web import RequestHandler
from tornado.web import HTTPError
from tornado.options import options
from kazoo.exceptions import NodeExistsError
from .mixins import RestMixin
from .schema import SchemaHandler


class EntityError(Exception):
    pass


def is_ip(s):
    try:
        socket.inet_aton(s)
        return True
    except OSError:
        return False


type_mapping = {
    'string': lambda x: isinstance(x, str),
    'long': lambda x: isinstance(x, int),
    'double': lambda x: isinstance(x, float),
    'date': lambda x: isinstance(x, int),
    'ip': is_ip
}


class EntityHandler(RestMixin, RequestHandler):
    @staticmethod
    def get_entity(schema, key, source=True):
        resp = requests.get('{0}/{1}/entity/{2}'.format(options.es, schema['name'], key))
        if resp.status_code >= 300:
            raise EntityError('get entity {0} error: {1}'.format(key, resp.text))
        if source:
            return resp.json().get('_source')
        return resp.json()

    @staticmethod
    def validate_type(tp, value):
        return type_mapping.get(tp)(value)

    @staticmethod
    def validate_unique(schema, field, value, key):
        query = {
            'term': {field['name']: value}
        }
        resp = requests.get('{0}/{1}/entity'.format(options.es, schema['name']), json={'query': query})
        if resp.status_code >= 300:
            raise HTTPError(status_code=500, reason=resp.text)
        total = resp.json().get('hits', {}).get('total', 0)
        if total == 1:
            entity = EntityHandler.get_entity(schema, key, False)
            o = resp.json().get('hits').get('hits')[0]
            if o['_id'] == entity['_id']:
                return
        if total <= 0:
            return
        raise EntityError('{0}:{1} is exist'.format(field['name'], value))

    @staticmethod
    def validate_field(schema, field, value, key):
        if field['multi']:
            if not isinstance(value, list):
                raise EntityError('{0} not multi'.format(field['name']))
            if not all((EntityHandler.validate_type(field['type'], v) for v in value)):
                raise EntityError('{0} type check error, require {1}, but {2}'.format(field['name'],
                                                                                      field['type'],
                                                                                      type(value)))
            for v in value:
                EntityHandler.validate_unique(schema, field, v, key)
        else:
            if not EntityHandler.validate_type(field['type'], value):
                raise EntityError('{0} type check error, require {1}, but {2}'.format(field['name'],
                                                                                      field['type'],
                                                                                      type(value)))
            EntityHandler.validate_unique(schema, field, value)


    @staticmethod
    def validate_entity(schema, entity):
        entity.pop('_meta')
        if set(entity.keys()) != {field['name'] for field in schema['fields']}:
            raise EntityError('field list not match')
        for field in schema['fields']:
            EntityHandler.validate_field(schema, field, entity[field['name']], entity[schema['pk']])

    def post(self, schema_name):
        node = os.path.join(options.root, schema_name)
        try:
            self.application.zk.create(node)
            schema = SchemaHandler.get_schema(schema_name)
            payload = self.get_payload()
            EntityHandler.validate_entity(schema, payload)
        except NodeExistsError:
            node = None
            raise HTTPError(status_code=422, reason='schema {0} is locked'.format(schema_name))
        finally:
            if node is not None:
                self.application.zk.delete(node)
