"""PR-13b/13c + PR-2: controller dell'editor delle sorgenti multi-chat (logica pura, testabile).

Gestisce dalla GUI la lista `source_chats` (PR-13b) e i **parser per chat**: singolo override
`parser_by_chat` (PR-13c) o **lista ordinata di più parser** `parser_list_by_chat` (PR-2, router
multi-parser). Per ogni sorgente si possono scegliere uno o più Parser Personalizzati (o "nessuno").
Niente widget: la vista (`source_chats_gui`) crea le righe e chiama questi metodi.

Retro-compatibilità: una riga con UN solo parser scrive esattamente come prima
(`parser_by_chat[chat] = nome`, nessuna voce lista). Solo con 2+ parser si usa
`parser_list_by_chat[chat] = [nomi]`, mantenendo `parser_by_chat[chat]` sincronizzato al PRIMO
nome (così l'autorizzazione chat e i lettori "single" restano invariati). Le sorgenti DISATTIVATE
parcheggiano la selezione fuori dalle mappe attive (`parser_by_chat_disabled` singolo /
`parser_list_by_chat_disabled` multi) così riabilitandole non si perde.
"""

import copy

from . import parser_manager, source_manager


def _disabled_overrides(container: dict) -> dict:
    """Legge `parser_by_chat_disabled` (singolo) in modo ROBUSTO: coerce a dict e chiavi a str.
    Una config manomessa può avere un valore non-dict (crash su `.get()`) o chiavi non-stringa
    (che non combacerebbero col `chat_id` stringa, perdendo in silenzio la selezione)."""
    raw = (container or {}).get("parser_by_chat_disabled")
    if not isinstance(raw, dict):
        return {}
    return {str(k): v for k, v in raw.items()}


def _disabled_list_overrides(container: dict) -> dict:
    """PR-2: come `_disabled_overrides` ma per la LISTA multi-parser parcheggiata
    (`parser_list_by_chat_disabled`). Valori normalizzati a liste di nomi non vuoti,
    deduplicati preservando l'ordine; voci non-lista ignorate (fail-safe)."""
    raw = (container or {}).get("parser_list_by_chat_disabled")
    if not isinstance(raw, dict):
        return {}
    out = {}
    for k, v in raw.items():
        if not isinstance(v, list):
            continue
        names = _clean_names(v)
        if names:
            out[str(k)] = names
    return out


def _clean_names(values) -> list:
    """Lista di nomi parser non vuoti, strippati, deduplicati preservando l'ordine."""
    seen = set()
    out = []
    for n in (values or []):
        name = str(n or "").strip()
        if name and name not in seen:
            seen.add(name)
            out.append(name)
    return out


class SourceEditor:
    """Stato e operazioni dell'editor delle sorgenti. Solo dati e logica.

    Ogni riga porta i 5 campi di `source_chats` (name/chat_id/enabled/mode/provider) più
    `parsers` (LISTA ordinata di parser per la chat, PR-2) e `parser` (il PRIMO della lista,
    "" = nessuno — retro-compat coi lettori single).
    """

    def __init__(self, cfg: dict = None):
        cfg = cfg or {}
        by_chat = parser_manager.parser_by_chat(cfg)              # {chat: nome} attivo singolo
        list_by_chat = parser_manager.parser_list_by_chat(cfg)   # {chat: [nomi]} attivo multi (PR-2)
        # Selezioni "parcheggiate" delle sorgenti DISATTIVATE: NON stanno nelle mappe attive
        # (non autorizzano né bloccano l'avvio), ma vanno mostrate così riabilitando la sorgente
        # non si perde il parser scelto (#47, Codex P2). Singolo + lista (PR-2).
        disabled_by_chat = _disabled_overrides(cfg)
        disabled_list = _disabled_list_overrides(cfg)
        self.sources = []
        for s in source_manager.source_chats(cfg):
            cid = s["chat_id"]
            # Sorgente ATTIVA: consulta SOLO le mappe attive (lista multi vince sul singolo).
            names = list_by_chat.get(cid) or _single_list(by_chat.get(cid))
            # Sorgente DISATTIVATA senza override attivo: consulta il parcheggio (lista poi singolo).
            if not names and not s["enabled"]:
                names = disabled_list.get(cid) or _single_list(disabled_by_chat.get(cid))
            names = _clean_names(names)
            s["parsers"] = names
            # `parser` = primo della lista (retro-compat: la vista/i test single leggono questo).
            s["parser"] = names[0] if names else ""
            self.sources.append(s)

    # ── opzioni per i menu della GUI ───────────────────────────────────────
    def mode_options(self) -> list:
        """Modalità ammesse per una sorgente (PRE/LIVE)."""
        return list(source_manager.MODES)

    def parser_options(self, parsers_dir: str = None) -> list:
        """Nomi dei Parser Personalizzati disponibili (per i menu). La vista antepone una
        voce "nessuno" (= "" = parser hardcoded/attivo globale)."""
        return parser_manager.available_parser_names(parsers_dir)

    # ── gestione righe ─────────────────────────────────────────────────────
    @staticmethod
    def _row(name="", chat_id="", enabled=True, mode=None, provider="", parser="", parsers=None) -> dict:
        """Riga canonica: i 5 campi di source_chats + `parsers` (lista) + `parser` (primo).

        Accetta `parsers` (lista, PR-2) OPPURE `parser` (singolo, retro-compat). Se `parsers`
        è passato ha la precedenza; altrimenti `parser` non vuoto diventa `[parser]`."""
        if parsers is not None:
            names = _clean_names(parsers)
        else:
            p = str(parser or "").strip()
            names = [p] if p else []
        return {
            "name": str(name or "").strip(),
            "chat_id": str(chat_id or "").strip(),
            # C7 #259: coercizione FAIL-CLOSED pubblica di `source_manager` (una stringa non
            # vuota tipo «false» non deve riabilitare la sorgente).
            "enabled": source_manager.as_enabled_bool(enabled),
            "mode": source_manager.normalize_mode(mode),
            "provider": str(provider or "").strip(),
            "parsers": names,
            "parser": names[0] if names else "",
        }

    @staticmethod
    def _source_only(row: dict) -> dict:
        """Solo i 5 campi di `source_chats` (esclude `parser`/`parsers`, che vanno nelle mappe)."""
        return {k: row[k] for k in ("name", "chat_id", "enabled", "mode", "provider")}

    def add_source(self, name="", chat_id="", enabled=True, mode=None, provider="",
                   parser="", parsers=None) -> dict:
        row = self._row(name, chat_id, enabled, mode, provider, parser, parsers)
        self.sources.append(row)
        return row

    def update_source(self, index: int, **kwargs) -> None:
        cur = self.sources[index]
        merged = {k: kwargs.get(k, cur[k]) for k in
                  ("name", "chat_id", "enabled", "mode", "provider")}
        # Parser: `parsers` (lista) vince; altrimenti `parser` singolo; altrimenti invariati.
        if "parsers" in kwargs:
            merged["parsers"] = kwargs["parsers"]
        elif "parser" in kwargs:
            p = str(kwargs["parser"] or "").strip()
            merged["parsers"] = [p] if p else []
        else:
            merged["parsers"] = cur.get("parsers", [])
        self.sources[index] = self._row(**merged)

    def remove_source(self, index: int) -> None:
        del self.sources[index]

    # ── validazione + applicazione alla config ─────────────────────────────
    def validate(self) -> tuple:
        """`(errori, avvisi)` sulle sole sorgenti (i 5 campi)."""
        sources = [self._source_only(s) for s in self.sources]
        return source_manager.validate_sources(sources), source_manager.duplicate_name_warnings(sources)

    def apply(self, cfg: dict) -> tuple:
        """Fonde sorgenti **e** parser-per-chat (singolo o lista) su una COPIA di `cfg` →
        `(nuova_cfg, errori, avvisi)`.

        - `source_chats`: i 5 campi delle righe.
        - `parser_by_chat` / `parser_list_by_chat`: per ogni riga ATTIVA con parser:
          * 1 solo parser → `parser_by_chat[chat] = nome` (retro-compat, nessuna voce lista);
          * 2+ parser → `parser_list_by_chat[chat] = [nomi]` **e** `parser_by_chat[chat] = primo`
            (sync per autorizzazione chat e lettori single).
        - Sorgenti DISATTIVATE: la selezione è parcheggiata (`parser_by_chat_disabled` singolo /
          `parser_list_by_chat_disabled` multi), fuori dalle mappe attive.
        - Le voci di chat NON mostrate qui (es. il `chat_id` globale) sono preservate.

        Se ci sono errori bloccanti, `nuova_cfg` è la config di partenza **invariata**."""
        base = copy.deepcopy(cfg) if isinstance(cfg, dict) else {}
        errors, warnings = self.validate()
        if errors:
            return base, errors, warnings
        global_chat = str(base.get("chat_id", "") or "").strip()
        row_ids = {s["chat_id"] for s in self.sources}
        old_source_ids = {s["chat_id"] for s in source_manager.source_chats(base)}
        base["source_chats"] = [self._source_only(s) for s in self.sources]
        by_chat = parser_manager.parser_by_chat(base)
        list_by_chat = parser_manager.parser_list_by_chat(base)
        disabled_by_chat = _disabled_overrides(base)
        disabled_list = _disabled_list_overrides(base)

        def _drop(chat):
            by_chat.pop(chat, None)
            list_by_chat.pop(chat, None)
            disabled_by_chat.pop(chat, None)
            disabled_list.pop(chat, None)

        # Sorgenti RIMOSSE/rinominate: togli l'override da TUTTE le mappe (così la chat non resta
        # autorizzata via parser_by_chat dopo la rimozione, Codex P1), MA conserva l'override
        # della chat `chat_id` GLOBALE (resta autorizzata di suo, P2-a).
        for chat in (old_source_ids - row_ids):
            if chat and chat != global_chat:
                _drop(chat)
        # Righe correnti: azzera i vecchi override in tutte le mappe, poi riscrivi.
        for s in self.sources:
            chat = s["chat_id"]
            _drop(chat)
            names = _clean_names(s.get("parsers", []))
            if not names:
                continue
            if s["enabled"]:
                if len(names) == 1:
                    by_chat[chat] = names[0]            # singolo: identico a prima
                else:
                    list_by_chat[chat] = names          # multi (PR-2)
                    by_chat[chat] = names[0]            # sync per autorizzazione/lettori single
            else:
                # Disattivata → SOLO parcheggio (non autorizza, non blocca `_start`, Codex P2-b).
                if len(names) == 1:
                    disabled_by_chat[chat] = names[0]
                else:
                    disabled_list[chat] = names
        base["parser_by_chat"] = by_chat
        _set_or_pop(base, "parser_list_by_chat", list_by_chat)
        _set_or_pop(base, "parser_by_chat_disabled", disabled_by_chat)
        _set_or_pop(base, "parser_list_by_chat_disabled", disabled_list)
        return base, [], warnings


def _single_list(value) -> list:
    """`[value]` (strippato) se `value` è un nome non vuoto, altrimenti `[]`. `or ""` coercia i
    falsy (es. `null` da config a mano) evitando di stringificarli a "None"/"0" (Codex P2)."""
    name = str(value or "").strip()
    return [name] if name else []


def _set_or_pop(container: dict, key: str, value: dict) -> None:
    """Scrive `value` sotto `key` se non vuoto, altrimenti rimuove la chiave (config pulita)."""
    if value:
        container[key] = value
    else:
        container.pop(key, None)
