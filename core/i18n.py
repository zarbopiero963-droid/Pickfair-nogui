"""#343 slice 4a: catalogo i18n minimale della GUI, stile gettext.

L'ITALIANO è la lingua di RIFERIMENTO: le chiavi del catalogo SONO le stringhe
italiane verbatim usate nella GUI, così non si inventano key sintetiche e il
sorgente resta leggibile. `tr(testo)` ritorna la traduzione nella lingua attiva
oppure il testo stesso — fail-safe: mai stringa vuota, mai KeyError, una
traduzione mancante mostra l'italiano (il bridge resta usabile, mai rotto).

La lingua attiva viene impostata all'avvio da `app_language` (#343 slice 3,
selettore al primo avvio) PRIMA di costruire la UI; un cambio lingua a runtime
ha effetto al riavvio (le etichette sono lette in `_build_ui`).

Scope di QUESTO slice: etichette STATICHE della finestra principale (tab,
bottoni, nomi campo). Restano in italiano per ora — slice successivi:
- gli stati dinamici «⬤ ATTIVO/OFFLINE/RICONNESSIONE…»: il pannello 🚦 Salute
  fa il parsing del TESTO di `_status_lbl` (`_refresh_health_inner`), quindi la
  loro localizzazione richiede prima di sostituire il text-parsing con uno stato
  canonico tracciato a parte (fail-closed: meglio IT che un semaforo rotto);
- banner/log/finestre secondarie (Strumenti, Parser, Wizard, …).
"""

import threading

LANGUAGES = ("IT", "EN", "ES")
_DEFAULT = "IT"

_lang = _DEFAULT
_lock = threading.Lock()


def set_language(code) -> str:
    """Imposta la lingua attiva della UI. Fail-safe: valore non supportato o
    vuoto (lingua mai scelta) → italiano, il comportamento storico."""
    global _lang
    lang = code.strip().upper() if isinstance(code, str) else ""
    if lang not in LANGUAGES:
        lang = _DEFAULT
    with _lock:
        _lang = lang
    return lang


def get_language() -> str:
    with _lock:
        return _lang


def tr(text: str) -> str:
    """Traduce `text` nella lingua attiva; senza traduzione ritorna `text`
    (l'italiano è il riferimento: per IT il catalogo non serve affatto)."""
    lang = get_language()
    if lang == _DEFAULT:
        return text
    return _CATALOG.get(lang, {}).get(text, text)


# Cataloghi: SOLO le stringhe che cambiano rispetto all'italiano (una stringa
# identica — «🐞 Debug», «📊 Dashboard», «■  STOP» in EN — si omette: il
# fallback la restituisce già). Le chiavi devono esistere VERBATIM in un sorgente
# GUI (`app.py`, `dashboard_stats.py`, o una finestra secondaria localizzata come
# `provider_gui.py`): il test anti-drift le estrae via AST e fallisce se una chiave
# non è più usata nel codice (mai traduzioni orfane). I messaggi con variabili
# usano il TEMPLATE come chiave (es. «➕ Provider «{name}» salvato.») e il chiamante
# fa `i18n.tr(template).format(...)`; le traduzioni devono conservare gli stessi
# segnaposto (test di parità).
_CATALOG = {
    "EN": {
        # Tab configurazione + monitoraggio
        "⚙️ Generale": "⚙️ General",
        "🎯 Riconoscimento": "🎯 Recognition",
        "🛡️ Sicurezza": "🛡️ Safety",
        "✅ Conferme XTrader": "✅ XTrader confirmations",
        "📡 Chat ascoltate": "📡 Monitored chats",
        "🚦 Salute": "🚦 Health",
        "📡 Stato": "📡 Status",
        # Bottoni principali
        "📁 Sfoglia…": "📁 Browse…",
        "📄 Crea CSV": "📄 Create CSV",
        "▶  AVVIA": "▶  START",
        "🗑️  Svuota CSV ora": "🗑️  Clear CSV now",
        "💾  Salva Config": "💾  Save Config",
        "🧰  Strumenti": "🧰  Tools",
        "🧙 Wizard prima configurazione": "🧙 First-setup wizard",
        "📋 Copia diagnostica": "📋 Copy diagnostics",
        "📂 Apri cartella log": "📂 Open log folder",
        "🧾 Esporta audit reale": "🧾 Export real-mode audit",
        "🔄 Aggiorna": "🔄 Refresh",
        "🧹 Svuota log": "🧹 Clear log",
        # Campi ed etichette («🔑 Bot Token», «💬 Chat ID», «📄 CSV Path»,
        # «🏷️ Provider», «🐞 Debug», «📊 Dashboard», «📋 Log», «■  STOP» sono
        # già inglese/universali: fallback)
        "Contatori dall'avvio": "Counters since start",
        "Mostra:": "Show:",
        "Conserva:": "Keep:",
        # Impostazioni avanzate (tab Riconoscimento/Sicurezza/Conferme — CodeRabbit
        # #357: i contenuti dei tab tradotti non devono restare in italiano)
        "🎯 Modalità riconoscimento": "🎯 Recognition mode",
        "🚦 Modalità bridge": "🚦 Bridge mode",
        "📅 Limite segnali al giorno": "📅 Daily signal limit",
        "🧮 Modalità coda segnali": "🧮 Signal queue mode",
        "▶️ Avvio automatico all'apertura (in modalità REALE chiede conferma)":
            "▶️ Auto-start on launch (REAL mode asks for confirmation)",
        "🕵️ Logga il testo completo dei messaggi (debug; OFF = solo hash + 1ª riga)":
            "🕵️ Log full message text (debug; OFF = hash + 1st line only)",
        "🔢 Max segnali attivi (modalità coda multi-riga)":
            "🔢 Max active signals (multi-row queue mode)",
        "💬 Chat notifiche XTrader": "💬 XTrader notifications chat",
        "⏳ Timeout conferma (sec)": "⏳ Confirmation timeout (sec)",
        "✅ Parole conferma (separate da virgola)":
            "✅ Confirmation words (comma-separated)",
        "❌ Parole rifiuto (separate da virgola)":
            "❌ Rejection words (comma-separated)",
        # Stato listener (#343 slice 4b: la logica usa lo stato CANONICO
        # health_check.LISTENER_*, questi sono solo display; «⬤  OFFLINE» è
        # universale → fallback)
        "⬤  ATTIVO": "⬤  ACTIVE",
        "⬤  RICONNESSIONE…": "⬤  RECONNECTING…",
        # Contatori Dashboard (chiavi in dashboard_stats.COUNTERS)
        "📥 Ricevuti": "📥 Received",
        "✅ Scritti": "✅ Written",
        "⚠️ Scartati": "⚠️ Discarded",
        "♻️ Duplicati": "♻️ Duplicates",
        "🚦 Limitati": "🚦 Limited",
        "🧪 Simulati": "🧪 Simulated",
        "❌ Errori": "❌ Errors",
        # Finestra Anagrafica Provider (#343 slice 4c; i messaggi con variabili
        # usano il template tradotto + .format(...), chiave = template)
        "📇  Anagrafica Provider": "📇  Provider registry",
        "Nomi Provider riutilizzabili nel Parser Personalizzato "
        "(colonna Provider). Valgono per tutti i parser.":
            "Provider names reusable in the Custom Parser "
            "(Provider column). They apply to all parsers.",
        "Nome del nuovo Provider": "New provider name",
        "➕  Aggiungi": "➕  Add",
        "Provider salvati": "Saved providers",
        "Nessun provider salvato.": "No provider saved.",
        "🗑  Rimuovi": "🗑  Remove",
        "Anagrafica Provider": "Provider registry",
        "⛔ Nome vuoto: provider non aggiunto.": "⛔ Empty name: provider not added.",
        "❌ Config illeggibile: {exc}": "❌ Config unreadable: {exc}",
        "ℹ️ «{name}» è già nell'anagrafica.": "ℹ️ «{name}» is already in the registry.",
        "➕ Provider «{name}» salvato.": "➕ Provider «{name}» saved.",
        "❌ Salvataggio FALLITO: «{name}» non salvato (andrebbe perso al riavvio). "
        "Controlla permessi/spazio del file config.":
            "❌ Save FAILED: «{name}» not saved (would be lost on restart). "
            "Check config file permissions/space.",
        "🗑 Provider «{name}» rimosso.": "🗑 Provider «{name}» removed.",
        "❌ Salvataggio FALLITO: «{name}» non rimosso (ricomparirebbe al riavvio). "
        "Controlla permessi/spazio del file config.":
            "❌ Save FAILED: «{name}» not removed (would reappear on restart). "
            "Check config file permissions/space.",
        # Finestra Profili impostazioni (#343 slice 4d)
        "📁  Profili impostazioni": "📁  Settings profiles",
        "Salva la configurazione corrente come profilo con un nome e "
        "ricaricala quando vuoi. Il token Telegram NON viene salvato nei "
        "profili e resta invariato al caricamento.":
            "Save the current configuration as a named profile and reload it "
            "whenever you like. The Telegram token is NOT saved in profiles and "
            "stays unchanged on load.",
        "Nome profilo (es. Prematch)": "Profile name (e.g. Prematch)",
        "💾  Salva profilo": "💾  Save profile",
        "Profili salvati": "Saved profiles",
        "(impossibile elencare i profili)": "(cannot list profiles)",
        "(nessun profilo salvato)": "(no profile saved)",
        "↺ Carica": "↺ Load",
        "🗑 Elimina": "🗑 Delete",
        "Profili impostazioni": "Settings profiles",
        "❌ Elenco profili non leggibile: {exc}": "❌ Profile list unreadable: {exc}",
        "⚠️ Ferma il bridge (STOP) prima di caricare un profilo: "
        "le impostazioni live cambiano solo al prossimo AVVIA.":
            "⚠️ Stop the bridge (STOP) before loading a profile: live settings "
            "change only on the next START.",
        "❌ Salvataggio profilo fallito: {exc}": "❌ Profile save failed: {exc}",
        "✅ Profilo {name!r} salvato (senza token).":
            "✅ Profile {name!r} saved (without token).",
        "✅ Profilo {name!r} caricato e applicato (token invariato).":
            "✅ Profile {name!r} loaded and applied (token unchanged).",
        "❌ Eliminazione fallita: {exc}": "❌ Delete failed: {exc}",
        "🗑 Profilo {name!r} eliminato.": "🗑 Profile {name!r} deleted.",
        "⚠️ Profilo {name!r} non trovato.": "⚠️ Profile {name!r} not found.",
        # Finestra Chat sorgenti (#343 slice 4e; solo chrome — sentinella
        # «(predefinito)», chip Traduzioni e errori di dominio restano IT)
        "📡  Chat sorgenti (multi-chat)": "📡  Source chats (multi-chat)",
        "Chat sorgenti (multi-chat)": "Source chats (multi-chat)",
        "Ogni sorgente è una chat/canale da cui accettare segnali. "
        "chat_id obbligatorio e univoco; una sorgente disattivata viene ignorata.":
            "Each source is a chat/channel to accept signals from. chat_id is "
            "required and unique; a disabled source is ignored.",
        "Attiva": "Enabled",
        "Nome": "Name",
        "Modalità": "Mode",
        "Traduzioni": "Translations",
        "➕  Aggiungi sorgente": "➕  Add source",
        "Parser della chat (in ordine di priorità)": "Chat parsers (in priority order)",
        "Il messaggio va a ogni parser in ordine; scattano TUTTI quelli le cui condizioni "
        "combaciano (una riga CSV per parser che scatta).":
            "The message is passed to each parser in order; ALL parsers whose conditions match "
            "fire (one CSV row per firing parser).",
        "➕ Aggiungi parser": "➕ Add parser",
        "Nessun parser: la chat usa il parser globale (predefinito).":
            "No parser: the chat uses the global (default) parser.",
        "💾  Salva": "💾  Save",
        "Niente salvato: correggi gli errori.": "Nothing saved: fix the errors.",
        "❌ Salvataggio su disco FALLITO: sorgenti NON salvate (andrebbero "
        "perse al riavvio). Controlla permessi/spazio del file config.":
            "❌ Disk save FAILED: sources NOT saved (would be lost on restart). "
            "Check config file permissions/space.",
        "✅ Salvate {n} sorgenti in config.json.":
            "✅ Saved {n} sources to config.json.",
        # Finestra Diario (#343 slice 4f; «🔄 Aggiorna» riusa la chiave già a catalogo)
        "📒  Diario eventi (locale, sola lettura)":
            "📒  Event journal (local, read-only)",
        "(tutti i tipi)": "(all types)",
        "Tutti": "All",
        "Tipo": "Type",
        "Ultimi": "Last",
        "📂 Apri cartella": "📂 Open folder",
        "Eventi del diario": "Journal events",
        "⚠️ Errore lettura diario: {kind}": "⚠️ Journal read error: {kind}",
        "Diario: {tot} eventi totali (mostrati {shown}).":
            "Journal: {tot} total events (showing {shown}).",
        "Quando": "When",
        "Dati (redatti)": "Data (redacted)",
        # Finestra Parser Personalizzato (#343 slice 4g; SOLO chrome puro). Non tradotti
        # per sicurezza: gli interruttori «MultiMarket (più mercati)»/«MultiSelection (più
        # selezioni)» e i VALORI delle tendine (Modalità/Sport/Mercato/Trasformazione/
        # Value-map) restano IT perché fungono da chiavi di config/confronto, e
        # `title="Provider"` è confrontato come `rule.target`. «Sport:» e «➕ Provider» sono
        # identici in EN → fallback, nessuna entry. «🗑 Elimina», «Attiva», «📋 Copia
        # diagnostica» riusano le chiavi già a catalogo.
        "Parser Personalizzato": "Custom Parser",
        "Nome parser:": "Parser name:",
        "Modalità:": "Mode:",
        "Parser salvati:": "Saved parsers:",
        "Catalogo XTrader:": "XTrader catalog:",
        "➕ Inserisci regole fisse": "➕ Insert fixed rules",
        "🔗 Traduzioni attive per questo parser": "🔗 Active translations for this parser",
        "Nomi squadra · separatore:": "Team names · separator:",
        "🗺️ Dizionario nomi": "🗺️ Names dictionary",
        "Mercati:": "Markets:",
        "🎯 Dizionario mercati": "🎯 Markets dictionary",
        "⚙️ Avanzate (Trasformazione · Value-map)": "⚙️ Advanced (Transform · Value-map)",
        "💾 Salva": "💾 Save",
        "🧪 Prova messaggio": "🧪 Test message",
        "🧪🧪 Prova più messaggi (separati da ---)":
            "🧪🧪 Test multiple messages (separated by ---)",
        "Messaggio di prova:": "Test message:",
        "Anteprima righe generate (#192):": "Preview of generated rows (#192):",
        "Diagnostica (una riga per colonna):": "Diagnostics (one row per column):",
        "Output multi-riga (un messaggio → più righe CSV)":
            "Multi-row output (one message → several CSV rows)",
        "Condizioni di gate (il parser scatta solo se il messaggio le soddisfa)":
            "Gate conditions (the parser fires only if the message satisfies them)",
        "Soddisfa:": "Satisfy:",
        "➕ Aggiungi condizione": "➕ Add condition",
        "testo da cercare nel messaggio": "text to search for in the message",
        "💡 «contiene»/«NON contiene» un testo; confronto senza maiuscole e "
        "tollerante agli spazi. Nessuna condizione = nessun filtro. Righe a "
        "testo vuoto sono ignorate.":
            "💡 «contains»/«does NOT contain» a text; case-insensitive, "
            "space-tolerant match. No condition = no filter. Empty-text rows "
            "are ignored.",
        "➕ Aggiungi mercato": "➕ Add market",
        "➕ Aggiungi selezione": "➕ Add selection",
        "🗑 Rimuovi": "🗑 Remove",
        "🆕 Nuovo": "🆕 New",
        "📂 Carica": "📂 Load",
        "📑 Duplica": "📑 Duplicate",
        "— nessuna": "— none",
        "✓ 1 attiva": "✓ 1 active",
        "✓ {count} attive": "✓ {count} active",
        "Nome del nuovo Provider:": "New provider name:",
        "⛔ Provider non aggiunto (nome vuoto).": "⛔ Provider not added (empty name).",
        "🆕 Nuovo parser (non ancora salvato).": "🆕 New parser (not yet saved).",
        "⛔ Nessun parser selezionato.": "⛔ No parser selected.",
        "Nuovo nome per la copia di {src!r}:": "New name for the copy of {src!r}:",
        "Duplica parser": "Duplicate parser",
        "⛔ Duplica annullata (nome vuoto).": "⛔ Duplication cancelled (empty name).",
        "❌ Non salvato:\n- ": "❌ Not saved:\n- ",
        "⛔ Nessun messaggio: incolla uno o più messaggi separati da una "
        "riga «---».":
            "⛔ No message: paste one or more messages separated by a «---» line.",
        "⛔ Premi prima «Prova messaggio».": "⛔ Press «Test message» first.",
        "❌ Copia non riuscita (appunti non disponibili).":
            "❌ Copy failed (clipboard unavailable).",
        "📋 Diagnostica copiata negli appunti.": "📋 Diagnostics copied to clipboard.",
        # Messaggi di STATO delle azioni Parser (#343 slice 4g; template + .format(...),
        # chiave = template IT). Il DATO interpolato ({exc}/{path}/{name}/…) resta invariato;
        # il testo di dominio bollato in {exc} (ParserBuilder/config) resta IT (slice separato).
        # «➕ Provider «{name}» salvato.» riusa la chiave già a catalogo (Anagrafica Provider).
        "❌ Errore salvataggio provider: {exc}": "❌ Error saving provider: {exc}",
        "⚠️ Provider «{name}» aggiunto solo in memoria (salvataggio fallito).":
            "⚠️ Provider «{name}» added in memory only (save failed).",
        "⛔ Non salvato: profili di mappatura nomi mancanti ({names}). "
        "Ricreali nel «Dizionario nomi» o togli la spunta prima di salvare.":
            "⛔ Not saved: missing name-mapping profiles ({names}). "
            "Recreate them in «Names dictionary» or untick before saving.",
        "⛔ Non salvato: profili di mappatura mercati mancanti ({names}). "
        "Ricreali nel «Dizionario mercati» o togli la spunta prima di salvare.":
            "⛔ Not saved: missing market-mapping profiles ({names}). "
            "Recreate them in «Markets dictionary» or untick before saving.",
        "❌ Errore salvataggio: {exc}": "❌ Save error: {exc}",
        "💾 Salvato in {path}": "💾 Saved to {path}",
        "➕ Regole fisse inserite: {market} · {selection}":
            "➕ Fixed rules inserted: {market} · {selection}",
        "❌ Errore caricamento: {exc}": "❌ Load error: {exc}",
        "📂 Caricato {name!r}.": "📂 Loaded {name!r}.",
        "❌ Errore duplica: {exc}": "❌ Duplicate error: {exc}",
        "📑 Duplicato in {new_name!r}.": "📑 Duplicated to {new_name!r}.",
        "❌ Errore eliminazione: {exc}": "❌ Delete error: {exc}",
        "🗑 Eliminato {name!r}.": "🗑 Deleted {name!r}.",
        "⛔ {name!r} non trovato.": "⛔ {name!r} not found.",
    },
    "ES": {
        "⚙️ Generale": "⚙️ General",
        "🎯 Riconoscimento": "🎯 Reconocimiento",
        "🛡️ Sicurezza": "🛡️ Seguridad",
        "✅ Conferme XTrader": "✅ Confirmaciones XTrader",
        "📡 Chat ascoltate": "📡 Chats escuchados",
        "🚦 Salute": "🚦 Salud",
        "📡 Stato": "📡 Estado",
        "📁 Sfoglia…": "📁 Examinar…",
        "📄 Crea CSV": "📄 Crear CSV",
        "▶  AVVIA": "▶  INICIAR",
        "■  STOP": "■  DETENER",
        "🗑️  Svuota CSV ora": "🗑️  Vaciar CSV ahora",
        "💾  Salva Config": "💾  Guardar config",
        "🧰  Strumenti": "🧰  Herramientas",
        "🧙 Wizard prima configurazione": "🧙 Asistente de primera configuración",
        "📋 Copia diagnostica": "📋 Copiar diagnóstico",
        "📂 Apri cartella log": "📂 Abrir carpeta de logs",
        "🧾 Esporta audit reale": "🧾 Exportar auditoría real",
        "🔄 Aggiorna": "🔄 Actualizar",
        "🧹 Svuota log": "🧹 Vaciar log",
        "📄 CSV Path": "📄 Ruta CSV",
        "⏱️ Timeout (sec)": "⏱️ Timeout (seg)",
        "🏷️ Provider": "🏷️ Proveedor",
        "Contatori dall'avvio": "Contadores desde el inicio",
        "Mostra:": "Mostrar:",
        "Conserva:": "Conservar:",
        # Impostazioni avanzate (CodeRabbit #357)
        "🎯 Modalità riconoscimento": "🎯 Modo de reconocimiento",
        "🚦 Modalità bridge": "🚦 Modo del bridge",
        "📅 Limite segnali al giorno": "📅 Límite de señales al día",
        "🧮 Modalità coda segnali": "🧮 Modo de cola de señales",
        "▶️ Avvio automatico all'apertura (in modalità REALE chiede conferma)":
            "▶️ Inicio automático al abrir (en modo REAL pide confirmación)",
        "🕵️ Logga il testo completo dei messaggi (debug; OFF = solo hash + 1ª riga)":
            "🕵️ Registrar el texto completo de los mensajes (debug; OFF = solo hash + 1ª línea)",
        "🔢 Max segnali attivi (modalità coda multi-riga)":
            "🔢 Máx. señales activas (modo cola multi-fila)",
        "💬 Chat notifiche XTrader": "💬 Chat de notificaciones XTrader",
        "⏳ Timeout conferma (sec)": "⏳ Timeout de confirmación (seg)",
        "✅ Parole conferma (separate da virgola)":
            "✅ Palabras de confirmación (separadas por comas)",
        "❌ Parole rifiuto (separate da virgola)":
            "❌ Palabras de rechazo (separadas por comas)",
        # Stato listener (#343 slice 4b)
        "⬤  ATTIVO": "⬤  ACTIVO",
        "⬤  RICONNESSIONE…": "⬤  RECONEXIÓN…",
        # Contatori Dashboard (chiavi in dashboard_stats.COUNTERS)
        "📥 Ricevuti": "📥 Recibidos",
        "✅ Scritti": "✅ Escritos",
        "⚠️ Scartati": "⚠️ Descartados",
        "♻️ Duplicati": "♻️ Duplicados",
        "🚦 Limitati": "🚦 Limitados",
        "🧪 Simulati": "🧪 Simulados",
        "❌ Errori": "❌ Errores",
        # Finestra Anagrafica Provider (#343 slice 4c)
        "📇  Anagrafica Provider": "📇  Registro de proveedores",
        "Nomi Provider riutilizzabili nel Parser Personalizzato "
        "(colonna Provider). Valgono per tutti i parser.":
            "Nombres de proveedor reutilizables en el Parser Personalizado "
            "(columna Provider). Valen para todos los parsers.",
        "Nome del nuovo Provider": "Nombre del nuevo proveedor",
        "➕  Aggiungi": "➕  Añadir",
        "Provider salvati": "Proveedores guardados",
        "Nessun provider salvato.": "Ningún proveedor guardado.",
        "🗑  Rimuovi": "🗑  Eliminar",
        "Anagrafica Provider": "Registro de proveedores",
        "⛔ Nome vuoto: provider non aggiunto.":
            "⛔ Nombre vacío: proveedor no añadido.",
        "❌ Config illeggibile: {exc}": "❌ Config ilegible: {exc}",
        "ℹ️ «{name}» è già nell'anagrafica.": "ℹ️ «{name}» ya está en el registro.",
        "➕ Provider «{name}» salvato.": "➕ Proveedor «{name}» guardado.",
        "❌ Salvataggio FALLITO: «{name}» non salvato (andrebbe perso al riavvio). "
        "Controlla permessi/spazio del file config.":
            "❌ Guardado FALLIDO: «{name}» no guardado (se perdería al reiniciar). "
            "Comprueba permisos/espacio del archivo config.",
        "🗑 Provider «{name}» rimosso.": "🗑 Proveedor «{name}» eliminado.",
        "❌ Salvataggio FALLITO: «{name}» non rimosso (ricomparirebbe al riavvio). "
        "Controlla permessi/spazio del file config.":
            "❌ Guardado FALLIDO: «{name}» no eliminado (reaparecería al reiniciar). "
            "Comprueba permisos/espacio del archivo config.",
        # Finestra Profili impostazioni (#343 slice 4d)
        "📁  Profili impostazioni": "📁  Perfiles de configuración",
        "Salva la configurazione corrente come profilo con un nome e "
        "ricaricala quando vuoi. Il token Telegram NON viene salvato nei "
        "profili e resta invariato al caricamento.":
            "Guarda la configuración actual como un perfil con un nombre y "
            "recárgala cuando quieras. El token de Telegram NO se guarda en los "
            "perfiles y permanece sin cambios al cargar.",
        "Nome profilo (es. Prematch)": "Nombre del perfil (p. ej. Prematch)",
        "💾  Salva profilo": "💾  Guardar perfil",
        "Profili salvati": "Perfiles guardados",
        "(impossibile elencare i profili)": "(no se pueden listar los perfiles)",
        "(nessun profilo salvato)": "(ningún perfil guardado)",
        "↺ Carica": "↺ Cargar",
        "🗑 Elimina": "🗑 Eliminar",
        "Profili impostazioni": "Perfiles de configuración",
        "❌ Elenco profili non leggibile: {exc}":
            "❌ Lista de perfiles ilegible: {exc}",
        "⚠️ Ferma il bridge (STOP) prima di caricare un profilo: "
        "le impostazioni live cambiano solo al prossimo AVVIA.":
            "⚠️ Detén el bridge (STOP) antes de cargar un perfil: los ajustes en "
            "vivo cambian solo en el próximo INICIAR.",
        "❌ Salvataggio profilo fallito: {exc}": "❌ Guardado del perfil fallido: {exc}",
        "✅ Profilo {name!r} salvato (senza token).":
            "✅ Perfil {name!r} guardado (sin token).",
        "✅ Profilo {name!r} caricato e applicato (token invariato).":
            "✅ Perfil {name!r} cargado y aplicado (token sin cambios).",
        "❌ Eliminazione fallita: {exc}": "❌ Eliminación fallida: {exc}",
        "🗑 Profilo {name!r} eliminato.": "🗑 Perfil {name!r} eliminado.",
        "⚠️ Profilo {name!r} non trovato.": "⚠️ Perfil {name!r} no encontrado.",
        # Finestra Chat sorgenti (#343 slice 4e)
        "📡  Chat sorgenti (multi-chat)": "📡  Chats de origen (multi-chat)",
        "Chat sorgenti (multi-chat)": "Chats de origen (multi-chat)",
        "Ogni sorgente è una chat/canale da cui accettare segnali. "
        "chat_id obbligatorio e univoco; una sorgente disattivata viene ignorata.":
            "Cada origen es un chat/canal del que aceptar señales. chat_id es "
            "obligatorio y único; un origen desactivado se ignora.",
        "Attiva": "Activa",
        "Nome": "Nombre",
        "Modalità": "Modo",
        "Provider": "Proveedor",
        "Traduzioni": "Traducciones",
        "➕  Aggiungi sorgente": "➕  Añadir origen",
        "Parser della chat (in ordine di priorità)": "Parsers del chat (en orden de prioridad)",
        "Il messaggio va a ogni parser in ordine; scattano TUTTI quelli le cui condizioni "
        "combaciano (una riga CSV per parser che scatta).":
            "El mensaje se pasa a cada parser en orden; se activan TODOS los que cumplen sus "
            "condiciones (una fila CSV por parser activado).",
        "➕ Aggiungi parser": "➕ Añadir parser",
        "Nessun parser: la chat usa il parser globale (predefinito).":
            "Sin parser: el chat usa el parser global (predeterminado).",
        "💾  Salva": "💾  Guardar",
        "Niente salvato: correggi gli errori.": "Nada guardado: corrige los errores.",
        "❌ Salvataggio su disco FALLITO: sorgenti NON salvate (andrebbero "
        "perse al riavvio). Controlla permessi/spazio del file config.":
            "❌ Guardado en disco FALLIDO: orígenes NO guardados (se perderían al "
            "reiniciar). Comprueba permisos/espacio del archivo config.",
        "✅ Salvate {n} sorgenti in config.json.":
            "✅ Guardados {n} orígenes en config.json.",
        # Finestra Diario (#343 slice 4f)
        "📒  Diario eventi (locale, sola lettura)":
            "📒  Diario de eventos (local, solo lectura)",
        "(tutti i tipi)": "(todos los tipos)",
        "Tutti": "Todos",
        "Tipo": "Tipo",
        "Ultimi": "Últimos",
        "📂 Apri cartella": "📂 Abrir carpeta",
        "Eventi del diario": "Eventos del diario",
        "⚠️ Errore lettura diario: {kind}": "⚠️ Error de lectura del diario: {kind}",
        "Diario: {tot} eventi totali (mostrati {shown}).":
            "Diario: {tot} eventos en total (mostrados {shown}).",
        "Quando": "Cuándo",
        "Dati (redatti)": "Datos (redactados)",
        # Finestra Parser Personalizzato (#343 slice 4g; SOLO chrome puro). Come per EN,
        # gli interruttori MultiMarket/MultiSelection, i VALORI delle tendine e
        # `title="Provider"` restano IT (config/confronto). «🗑 Elimina», «Attiva»,
        # «📋 Copia diagnostica» riusano le chiavi già a catalogo. In ES «Sport:»/«➕ Provider»
        # differiscono, quindi hanno una entry.
        "Parser Personalizzato": "Parser Personalizado",
        "Nome parser:": "Nombre del parser:",
        "Modalità:": "Modo:",
        "Sport:": "Deporte:",
        "Parser salvati:": "Parsers guardados:",
        "Catalogo XTrader:": "Catálogo XTrader:",
        "➕ Provider": "➕ Proveedor",
        "➕ Inserisci regole fisse": "➕ Insertar reglas fijas",
        "🔗 Traduzioni attive per questo parser": "🔗 Traducciones activas para este parser",
        "Nomi squadra · separatore:": "Nombres de equipo · separador:",
        "🗺️ Dizionario nomi": "🗺️ Diccionario de nombres",
        "Mercati:": "Mercados:",
        "🎯 Dizionario mercati": "🎯 Diccionario de mercados",
        "⚙️ Avanzate (Trasformazione · Value-map)": "⚙️ Avanzadas (Transformación · Value-map)",
        "💾 Salva": "💾 Guardar",
        "🧪 Prova messaggio": "🧪 Probar mensaje",
        "🧪🧪 Prova più messaggi (separati da ---)":
            "🧪🧪 Probar varios mensajes (separados por ---)",
        "Messaggio di prova:": "Mensaje de prueba:",
        "Anteprima righe generate (#192):": "Vista previa de filas generadas (#192):",
        "Diagnostica (una riga per colonna):": "Diagnóstico (una fila por columna):",
        "Output multi-riga (un messaggio → più righe CSV)":
            "Salida multifila (un mensaje → varias filas CSV)",
        "Condizioni di gate (il parser scatta solo se il messaggio le soddisfa)":
            "Condiciones de activación (el parser se dispara solo si el mensaje las cumple)",
        "Soddisfa:": "Cumple:",
        "➕ Aggiungi condizione": "➕ Añadir condición",
        "testo da cercare nel messaggio": "texto a buscar en el mensaje",
        "💡 «contiene»/«NON contiene» un testo; confronto senza maiuscole e "
        "tollerante agli spazi. Nessuna condizione = nessun filtro. Righe a "
        "testo vuoto sono ignorate.":
            "💡 «contiene»/«NO contiene» un texto; comparación sin mayúsculas y "
            "tolerante a espacios. Sin condiciones = sin filtro. Las filas con "
            "texto vacío se ignoran.",
        "➕ Aggiungi mercato": "➕ Añadir mercado",
        "➕ Aggiungi selezione": "➕ Añadir selección",
        "🗑 Rimuovi": "🗑 Eliminar",
        "🆕 Nuovo": "🆕 Nuevo",
        "📂 Carica": "📂 Cargar",
        "📑 Duplica": "📑 Duplicar",
        "— nessuna": "— ninguna",
        "✓ 1 attiva": "✓ 1 activa",
        "✓ {count} attive": "✓ {count} activas",
        "Nome del nuovo Provider:": "Nombre del nuevo proveedor:",
        "⛔ Provider non aggiunto (nome vuoto).": "⛔ Proveedor no añadido (nombre vacío).",
        "🆕 Nuovo parser (non ancora salvato).": "🆕 Nuevo parser (aún no guardado).",
        "⛔ Nessun parser selezionato.": "⛔ Ningún parser seleccionado.",
        "Nuovo nome per la copia di {src!r}:": "Nuevo nombre para la copia de {src!r}:",
        "Duplica parser": "Duplicar parser",
        "⛔ Duplica annullata (nome vuoto).": "⛔ Duplicación cancelada (nombre vacío).",
        "❌ Non salvato:\n- ": "❌ No guardado:\n- ",
        "⛔ Nessun messaggio: incolla uno o più messaggi separati da una "
        "riga «---».":
            "⛔ Ningún mensaje: pega uno o más mensajes separados por una línea «---».",
        "⛔ Premi prima «Prova messaggio».": "⛔ Pulsa «Probar mensaje» primero.",
        "❌ Copia non riuscita (appunti non disponibili).":
            "❌ Copia fallida (portapapeles no disponible).",
        "📋 Diagnostica copiata negli appunti.": "📋 Diagnóstico copiado al portapapeles.",
        # Messaggi di STATO delle azioni Parser (#343 slice 4g). Come EN: DATO interpolato
        # invariato e testo di dominio in {exc} resta IT. «➕ Provider «{name}» salvato.»
        # riusa la chiave già a catalogo.
        "❌ Errore salvataggio provider: {exc}": "❌ Error al guardar el proveedor: {exc}",
        "⚠️ Provider «{name}» aggiunto solo in memoria (salvataggio fallito).":
            "⚠️ Proveedor «{name}» añadido solo en memoria (guardado fallido).",
        "⛔ Non salvato: profili di mappatura nomi mancanti ({names}). "
        "Ricreali nel «Dizionario nomi» o togli la spunta prima di salvare.":
            "⛔ No guardado: faltan perfiles de mapeo de nombres ({names}). "
            "Vuelve a crearlos en «Diccionario de nombres» o desmárcalos antes de guardar.",
        "⛔ Non salvato: profili di mappatura mercati mancanti ({names}). "
        "Ricreali nel «Dizionario mercati» o togli la spunta prima di salvare.":
            "⛔ No guardado: faltan perfiles de mapeo de mercados ({names}). "
            "Vuelve a crearlos en «Diccionario de mercados» o desmárcalos antes de guardar.",
        "❌ Errore salvataggio: {exc}": "❌ Error al guardar: {exc}",
        "💾 Salvato in {path}": "💾 Guardado en {path}",
        "➕ Regole fisse inserite: {market} · {selection}":
            "➕ Reglas fijas insertadas: {market} · {selection}",
        "❌ Errore caricamento: {exc}": "❌ Error al cargar: {exc}",
        "📂 Caricato {name!r}.": "📂 Cargado {name!r}.",
        "❌ Errore duplica: {exc}": "❌ Error al duplicar: {exc}",
        "📑 Duplicato in {new_name!r}.": "📑 Duplicado en {new_name!r}.",
        "❌ Errore eliminazione: {exc}": "❌ Error al eliminar: {exc}",
        "🗑 Eliminato {name!r}.": "🗑 Eliminado {name!r}.",
        "⛔ {name!r} non trovato.": "⛔ {name!r} no encontrado.",
    },
}
