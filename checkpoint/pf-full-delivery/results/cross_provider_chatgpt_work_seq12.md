# Prova cross-provider Claude → ChatGPT Work

- Checkpoint sorgente: `agent-state/pf-full-delivery@a14adea2abd90262cdf2e29a7be15766e197bd09`, schema `pf-checkpoint/3`, sequenza 11.
- Bootstrap pulito: `pfckpt.py bootstrap` ha restituito `READY`; `pfckpt.py verify` ha restituito `VALID`.
- Manifest checkpoint: 41/41 file verificati SHA-256.
- Codice recuperato: `ccr-bcc81683-ud0hgc@82f499c852f4bdb38db2183c4fae8cb099910c9a`.
- Gate-A concorrente, sola lettura: `checkpoint/gate-a-close02@1734dee1f170bd8e5ea3c7eff6c602bf7e8c306f`; manifest 459/459 OK.
- Governance ricostruita senza cronologia Claude: `main@028cee9`, PR protetta #505 `618884e`, Gate A NON PASS, LIVE NO-GO, niente merge/deploy/spese.
- `next_atomic_action` recuperata da STATE e completata: riconciliazione A06, poi lotto indipendente F011/F025.
- Ambiente: Python 3.12.14; rete non-loopback negata durante i test; Xvfb non disponibile nell'immagine e non installabile dai repository configurati.
- Limitazioni: test GUI/X11 saltati; test Telegram LIVE saltati per credenziali assenti e divieto LIVE. Grok non è stato provato: `NOT VERIFIED`.

Verdetto handoff ChatGPT Work: **PASS**. Non sono state usate informazioni necessarie provenienti dalla conversazione Claude.
