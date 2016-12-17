import ssl
import json
import time
import socket
import logging
import random
import Queue

RECV_SIZE = 2 ** 16
CLIENT_VERSION = "0.0"
PROTO_VERSION = "1.0"

DEFAULT_HOST = "ecdsa.net"
DEFAULT_PORT = 50001

TIMEOUT = 5

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger("stratum-client")


class Connection(object):
    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, ssl=False):
        self.host = host
        self.port = port
        self.ssl = ssl
        self.call_count = 0

        self.socket = None
        self.file = None
        self.server_version = None

        self.connect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def create_socket(self):
        sock = socket.create_connection((self.host, self.port), timeout=TIMEOUT)
        return ssl.wrap_socket(sock) if self.ssl else sock

    def connect(self):
        log.debug("connecting to %s:%s...", self.host, self.port)
        self.socket = self.create_socket()
        self.file = self.socket.makefile()
        log.info("connected to %s:%s", self.host, self.port)
        self.version()

    def version(self):
        self.server_version = self.call("server.version")["result"]

    def close(self):
        self.socket.close()
        log.info("disconnected from %s:%s", self.host, self.port)
        self.socket = self.file = None

    def encode(self, payload):
        return json.dumps(payload).encode() + b"\n"

    def send(self, method, params):
        payload = {"id": self.call_count, "method": method, "params": params}
        #log.debug("<<< %s", payload)
        self.call_count += 1
        self.socket.send(self.encode(payload))

    def call(self, method, *params):
        t1 = time.time()
        self.send(method, params)
        # FIXME this will not work if the response is multiline
        line = self.file.readline().strip()
        t2 = time.time()
        delta = (t2 - t1) * 1000
        #log.debug(">>> %s", line)
        log.debug("%s(%s) took %sms", method, params, delta)
        return json.loads(line)


class Peer(object):

    PORT_TYPE_TCP = "t"
    PORT_TYPE_SSL = "s"
    PORT_TYPE_HTTP = "h"
    PORT_TYPE_HTTPS = "g"
    PORT_TYPES = (PORT_TYPE_TCP, PORT_TYPE_SSL, PORT_TYPE_HTTP, PORT_TYPE_HTTPS)

    def __init__(self, addresses, params):
        self.addresses = addresses
        self.params = params
        self.verison = params[0]
        self.prune = None
        self.ports = []
        self.parse(params)

    def parse(self, params):
        for param in params:
            if param[0] == "p":
                self.prune = int(param[1:])
            elif param[0] in self.PORT_TYPES:
                peer_type = param[0]
                if param[1:]:
                    port = int(param[1:])
                elif peer_type == self.PORT_TYPE_TCP:
                    port = DEFAULT_PORT
                elif peer_type == self.PORT_TYPE_SSL:
                    port = 50002
                elif peer_type == self.PORT_TYPE_HTTP:
                    port = 8081
                elif peer_type == self.PORT_TYPE_HTTPS:
                    port = 8082

                if port:
                    self.ports.append((peer_type, port))

    def __repr__(self):
        return "Peer(addresses={}, params={})".format(self.addresses, self.params)

    @classmethod
    def discover(cls):
        with Connection() as conn:
            result = conn.call("server.peers.subscribe")
        peers = result["result"]
        return [Peer(peer[0:-1], peer[-1]) for peer in peers]

    @property
    def clearnet_addresses(self):
        return [address for address in self.addresses if not is_onion(address)]

    @property
    def onion_addresses(self):
        return [address for address in self.addresses if is_onion(address)]

    def get_ports_by_type(self, port_type):
        return [port for pt, port in self.ports if port_type == pt]

    tcp_ports = property(lambda self: self.get_ports_by_type(self.PORT_TYPE_TCP))
    ssl_ports = property(lambda self: self.get_ports_by_type(self.PORT_TYPE_SSL))
    http_ports = property(lambda self: self.get_ports_by_type(self.PORT_TYPE_HTTP))
    https_ports = property(lambda self: self.get_ports_by_type(self.PORT_TYPE_HTTPS))


def is_onion(address):
    return address.endswith(".onion")


class ConnectionHandler(object):
    def __init__(self, connection_pool):
        self.connection_pool = connection_pool
        self.connection = None

    def __enter__(self):
        self.connection = self.connection_pool.take()
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection_pool.release(self.connection)


class ConnectionPool(object):
    def __init__(self, max_size):
        self.connections = Queue.Queue()
        self.peers = Peer.discover()
        self.max_size = max_size
        self.count = 0

    def get(self):
        return ConnectionHandler(self)

    def close(self):
        while not self.connections.empty():
            connection = self.connections.get_nowait()
            connection.close()

    def release(self, connection):
        self.connections.put(connection)

    def take(self):
        try:
            if self.count == self.max_size:
                connection = self.connections.get(block=True)
            else:
                connection = self.connections.get_nowait()
        except Queue.Empty:
            connection = self.connect()
        return connection

    def connect(self):
        if self.count >= self.max_size:
            return None

        for _ in range(100):
            peer = random.choice(self.peers)
            addresses = peer.clearnet_addresses
            if not addresses:
                continue

            ports = peer.ssl_ports
            has_ssl = bool(ports)
            if not has_ssl:
                ports = peer.tcp_ports

            if not ports:
                continue

            address = addresses[0]
            port = ports[0]
            try:
                conn = Connection(host=address, port=port, ssl=has_ssl)
                self.count += 1
                return conn
            except socket.error as e:
                log.error("could not connect to %s: %s", address, e)

if __name__ == "__main__":

    pool = ConnectionPool(1)

    for _ in range(3):
        with pool.get() as conn:
            rv = conn.call("server.banner")
            print conn.host, conn.server_version
            print rv["result"]
        time.sleep(1)

    pool.close()

