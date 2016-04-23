import os
from tornado.ioloop import IOLoop
from tornado.options import parse_config_file, parse_command_line, options
from cmdb import make_app
from cmdb.schema import SchemaHandler

routes = [
    (r'/schema', SchemaHandler),
    (r'/schema/(.*)', SchemaHandler)
]

if __name__ == '__main__':
    if os.path.exists('/etc/cmdb.conf'):
        parse_config_file('/etc/cmdb.conf')
    if os.path.exists('./application.conf'):
        parse_config_file('./application.conf')
    parse_command_line()
    app = make_app(routes, debug=True)
    app.listen(options.port, address=options.bind)
    try:
        app.zk.start()
        IOLoop().current().start()
    except KeyboardInterrupt:
        IOLoop().current().stop()