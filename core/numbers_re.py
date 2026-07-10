"""Frammenti regex condivisi per i numeri decimali (fonte unica — anti-drift, audit L4).

Quattro moduli (`parser`, `validator`, `custom_pipeline`, `csv_writer`) ripetevano lo
stesso frammento ``[0-9]+(?:[.,][0-9]+)?``: una modifica in uno e non negli altri avrebbe
fatto divergere il riconoscimento dei numeri (quota/Handicap/Price). Qui sta una sola volta;
ogni modulo compone àncore (``^…$``) e segno come gli serve. Modulo foglia: non importa
nulla, nessun rischio di ciclo.

Le cifre sono ASCII ``[0-9]`` e NON ``\\d`` (vedi `DECIMAL`): `\\d` matcherebbe le cifre
Unicode, aprendo il fail-open #318 L2-1.
"""

# Numero decimale "puro": cifre con AL PIÙ una parte decimale separata da `.` o `,`
# (niente "1.2.3"/esponenti). Senza segno.
#
# Cifre ASCII SOLTANTO (`[0-9]`, NON `\d`): #318 L2-1 (fail-OPEN). In Python `\d` matcha
# TUTTE le cifre Unicode (arabo-indiane «١٩», devanagari «१९», fullwidth «１９»); poiché
# `float("١٩") == 19.0`, un Price/Handicap/Points scritto con cifre non-ASCII superava la
# validazione ed entrava nel CSV letto da XTrader (raggiungibile da un vero messaggio
# Telegram via parser custom). `[0-9]` chiude il buco fail-closed su TUTTI i consumer
# (validator/custom_pipeline/csv_writer/parser) da questa fonte unica.
DECIMAL = r"[0-9]+(?:[.,][0-9]+)?"

# Come `DECIMAL` ma con segno opzionale (es. Handicap "-1"/"+1,5", Price "1.85").
# COMPOSTO da `DECIMAL` (non riscritto a mano) così un futuro cambio a `DECIMAL` non può
# divergere qui — l'anti-drift vale anche dentro il modulo (review Fable 5 su #318).
SIGNED_DECIMAL = r"[+-]?" + DECIMAL
