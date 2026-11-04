"""Ponte Pickfair → Control API v1 di sola lettura (proposta per Claude B).

Il server, l'autenticazione e il contratto sono nel package ``pickfair_control_api`` del
repository pickfair-mcp- (solo libreria standard). Qui stanno soltanto:
- ``views``: letture pure del ``RuntimeController`` (nessun effetto, mai ``get_status()``);
- ``bridge``: collegamento delle viste al server;
- ``launcher``: avvio OPZIONALE da ``headless_main``, disattivato per impostazione predefinita.

Questo package NON importa ``pickfair_control_api`` a livello di modulo: senza i flag
``--control-api-*`` Pickfair si avvia identico, anche se il package non è installato.
"""
