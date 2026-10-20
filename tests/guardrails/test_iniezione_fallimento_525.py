"""#525 — FALLIMENTO INIETTATO DI PROPOSITO, commit temporaneo.

Serve solo a provare su run GitHub reali che un test rosso arriva fino alla
conclusione degli orchestratori ci-master-gate / ci-dynamic-intelligent.
Viene rimosso dal commit successivo: non deve mai arrivare su main.
"""


def test_fallimento_iniettato_525():
    assert False, "fallimento iniettato #525: deve propagarsi al gate"
