import requests
import string
import logging
from os import path as os_path
from tornado.web import RequestHandler
from tornado.web import HTTPError
from tornado.options import options
from kazoo.exceptions import NodeExistsError
from .mixins import RestMixin


class SchemaError(Exception):
    pass


class SchemaHandler(RestMixin, RequestHandler):
    @staticmethod
    def is_schema_exist(schema):
        resp = requests.head('{0}/{1}'.format(options.es, schema['name']))
        return resp.status_code == 200

    @staticmethod
    def is_same_field(o, n):
        return o == n

    @staticmethod
    def get_schema(name):
        resp = requests.get('{0}/{1}/schema/{2}'.format(options.es, name, name))
        return resp.json()

    @staticmethod
    def check_conflict(schema):
        origin = SchemaHandler.get_schema(schema['name'])
        if not {field['name'] for field in schema['fields']}.issuperset(
                {field['name'] for field in origin.get('fields', [])}):
            raise SchemaError('conflict check fail')
        fields = {field['name']: field for field in schema['fields']}
        for field in origin.get('fields', []):
            if not SchemaHandler.is_same_field(field, fields[field.name]):
                raise SchemaError('{0} not same origin'.format(field.name))
        if schema['pk'] != origin['pk']:
            raise SchemaError('pk not same origin')

    @staticmethod
    def validate_name(name):
        if len(name) <= 0:
            raise SchemaError('name is require')
        charts = set(string.ascii_letters)
        charts.update(string.digits)
        if not set(name).issubset(charts):
            raise SchemaError('name error')

    @staticmethod
    def validate_field(field):
        SchemaHandler.validate_name(field['name'])
        for key in ('require', 'multi', 'unique'):
            if field.get(key) is None or not isinstance(field.get(key), bool):
                field[key] = False
                logging.warning('field {0} {1} is nut set or is not a bool, set False'.format(field['name'], key))
        if field['type'] not in ('string', 'long', 'double', 'datetime', 'ip'):
            raise SchemaError('type error')
        if field.get('ref'):
            schema_name, field_name = field.get('ref').split('::')
            if not SchemaHandler.is_schema_exist(schema_name):
                raise SchemaError('reference schema {0} is not exist'.format(schema_name))
            schema = SchemaHandler.get_schema(schema_name)
            if field_name not in schema['fields']:
                raise SchemaError('reference field {0} is not exist'.format(field_name))
        return field

    @staticmethod
    def validate_schema(schema):
        SchemaHandler.validate_name(schema['name'])
        if len({field['name'] for field in schema['fields']}) != len(schema['fields']):
            raise SchemaError('field name must be unique')
        if SchemaHandler.is_schema_exist(schema):
            SchemaHandler.check_conflict(schema)
        for field in schema['fields']:
            SchemaHandler.validate_field(field)

    def post(self, *args, **kwargs):
        payload = self.get_payload()
        node = None
        try:
            node = os_path.join(options.root, payload['name'])
            self.application.zk.create(node)
            SchemaHandler.validate_schema(payload)
        except KeyError:
            raise HTTPError(status_code=400, reason='schema name require')
        except NodeExistsError:
            raise HTTPError(status_code=503, reason='schema {0} is locked'.format(payload['name']))
        except SchemaError as e:
            raise HTTPError(status_code=400, reason=str(e))
        finally:
            if node is not None:
                self.application.zk.delete(node)
