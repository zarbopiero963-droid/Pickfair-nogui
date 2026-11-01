"""F-33 netguard: nel sottoprocesso dell'harness ogni connessione di rete non locale e' un errore.

Caricato via PYTHONPATH (sitecustomize). Invariante 11 del piano: nessun ordine reale da SIM, nessuna uscita verso
Betfair dai test. Consentiti solo loopback. Il patch e' su ``socket.socket`` (``connect``/``connect_ex``/``sendto``,
TCP e UDP) e su ``socket.create_connection``: copre http.client, urllib3/requests, ssl, asyncio. Limite dichiarato:
il tipo C ``_socket.socket`` e' immutabile e non si puo' patchare, quindi un uso DIRETTO di ``_socket`` non e'
intercettato (nessun modulo di Pickfair lo usa). I sottoprocessi ereditano PYTHONPATH e quindi lo stesso guard.
"""
import socket as _sock

_socket = _sock  # i metodi si patchano sulla sottoclasse Python ``socket.socket``

_ALLOWED_HOSTS = {"127.0.0.1", "localhost", "::1", ""}


def _host_of(address):
    if isinstance(address, (bytes, str)):      # AF_UNIX
        return "127.0.0.1"
    try:
        return str(address[0])
    except Exception:  # noqa: BLE001
        return str(address)


def _deny(address):
    host = _host_of(address)
    if host not in _ALLOWED_HOSTS:
        raise OSError(f"PF_NETGUARD: connessione di rete vietata nell'harness F-33 verso {host!r}")


_real_connect = _socket.socket.connect
_real_connect_ex = _socket.socket.connect_ex
_real_sendto = _socket.socket.sendto
_real_create_connection = _sock.create_connection


def _connect(self, address):
    _deny(address)
    return _real_connect(self, address)


def _connect_ex(self, address):
    _deny(address)
    return _real_connect_ex(self, address)


def _sendto(self, *args):
    # sendto(data, address) | sendto(data, flags, address)
    if args:
        _deny(args[-1])
    return _real_sendto(self, *args)


def _create_connection(address, *args, **kwargs):
    _deny(address)
    return _real_create_connection(address, *args, **kwargs)


_socket.socket.connect = _connect
_socket.socket.connect_ex = _connect_ex
_socket.socket.sendto = _sendto
_sock.create_connection = _create_connection
