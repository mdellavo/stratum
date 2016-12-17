import logging

from wsgiref.simple_server import make_server
from pyramid.config import Configurator
from pyramid.view import view_config

from stratum.client import Connection

logging.basicConfig(level=logging.DEBUG)

log = logging.getLogger("stratum-server")

ADDRESS = '0.0.0.0'
PORT = 8080


def get_connection():
    return Connection()


def ok(**kwargs):
    kwargs["status"] = "ok"
    return dict(kwargs)


def call(method, params):
    with get_connection() as conn:
        response = conn.call(method, *params)
    return response["result"]


@view_config(route_name="execute", renderer="json", request_method="POST")
def execute(request):
    method = request.POST.get("method")
    params = request.POST.get("params", [])
    result = call(method, params)
    return ok(result=result)


if __name__ == '__main__':
    address = ADDRESS
    port = PORT

    config = Configurator()
    config.add_route("execute", "/execute")
    config.scan()
    app = config.make_wsgi_app()

    server = make_server(ADDRESS, PORT, app)
    log.info("serving on %s:%s", address, port)
    server.serve_forever()
