# Audit guard: nega connessioni di rete non-loopback durante i test (PF-AUDIT fase A).
import sys, ipaddress
def _g(event, args):
    if event == "socket.connect":
        addr = args[1]
        if isinstance(addr, tuple) and addr:
            host = str(addr[0])
            try:
                if ipaddress.ip_address(host).is_loopback:
                    return
            except ValueError:
                if host in ("localhost",):
                    return
            raise ConnectionRefusedError("PF_AUDIT_NETWORK_DENIED:" + host)
    if event == "socket.getaddrinfo":
        host = str(args[0])
        if host not in ("localhost", "127.0.0.1", "::1", "None", ""):
            raise OSError("PF_AUDIT_DNS_DENIED:" + host)
sys.addaudithook(_g)
