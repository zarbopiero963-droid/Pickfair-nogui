"""F-33 netguard: nel sottoprocesso dell'harness ogni connessione di rete non locale e' un errore.

Caricato via PYTHONPATH (sitecustomize). Invariante 11 del piano: nessun ordine reale da SIM, nessuna uscita verso
Betfair dai test. Consentiti solo loopback (nessun servizio e' comunque atteso).
"""
import socket as _socket

_ALLOWED_HOSTS = {"127.0.0.1", "localhost", "::1"}
_real_connect = _socket.socket.connect
_real_connect_ex = _socket.socket.connect_ex
_real_create_connection = _socket.create_connection


def _host_of(address):
    try:
        return str(address[0])
    except Exception:  # noqa: BLE001
        return str(address)


def _deny(address):
    host = _host_of(address)
    if host not in _ALLOWED_HOSTS:
        raise OSError(f"PF_NETGUARD: connessione di rete vietata nell'harness F-33 verso {host!r}")


def _connect(self, address):
    _deny(address)
    return _real_connect(self, address)


def _connect_ex(self, address):
    _deny(address)
    return _real_connect_ex(self, address)


def _create_connection(address, *args, **kwargs):
    _deny(address)
    return _real_create_connection(address, *args, **kwargs)


_socket.socket.connect = _connect
_socket.socket.connect_ex = _connect_ex
_socket.create_connection = _create_connection
