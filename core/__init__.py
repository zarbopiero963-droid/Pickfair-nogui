"""XTrader Signal Bridge — package.

Separazione (PR-03):
- `parser`       : parsing dei messaggi Telegram P.Bet. (nessuna dipendenza GUI)
- `csv_writer`   : contratto CSV XTrader, costruzione/scrittura righe
- `config_store` : caricamento/salvataggio configurazione (funzioni pure)
- `app`          : GUI CustomTkinter + listener Telegram (unica parte con GUI)

`main.py` nella root è solo l'entrypoint.
"""

# Versione dell'app (PR-18): UNICA fonte di verità. Mostrata nel titolo della GUI
# e usata dalla build per nominare l'artifact (l'EXE resta `XTrader-Signal-Bridge.exe`).
# Schema semantico MAJOR.MINOR.PATCH; pre-1.0 = prototipo/sviluppo (roadmap in corso),
# salirà a 1.0.0 alla release candidate (PR-20).
__version__ = "0.1.0"
