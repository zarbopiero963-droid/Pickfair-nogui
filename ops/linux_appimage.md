# Pickfair GUI su Linux — AppImage ("vero programma Linux")

La GUI di Pickfair su Linux è distribuita come **AppImage**: un singolo file
`Pickfair-x86_64.AppImage` che si comporta come un vero programma desktop
(icona, voce nel menu applicazioni, doppio click), senza installazione né
dipendenze di sistema. È l'equivalente Linux del `pickfair.exe` Windows
(vedi `ops/windows_exe.md`).

> È **lo stesso identico programma** del `.exe`: stesso codice
> (`main.py` → `mini_gui.py` + `ui_panels/*` + `customtkinter`), stessi
> pannelli e pulsanti. L'AppImage impacchetta il binario PyInstaller onefile
> `dist/pickfair`, non reimplementa nulla.

L'**headless** (`pickfair-headless`) NON è un AppImage: è un servizio/CLI da
server, distribuito come binario nudo (`dist/pickfair-headless`). Per il deploy
headless su VPS Linux vedi la relativa doc di servizio (systemd).

## Come si ottiene

Il workflow `.github/workflows/build-linux.yml` (runner `ubuntu-latest`):

1. verifica **fail-closed** che Tk sia disponibile (`import tkinter`) — senza Tk
   la GUI sarebbe rotta, meglio fallire prima del build;
2. builda i binari con `pyinstaller pickfair.spec` (`dist/pickfair` +
   `dist/pickfair-headless`);
3. impacchetta i **binari nudi in `.tar.gz`** (`pickfair` + `pickfair-headless`)
   — `upload-artifact` ZIPpa e **perde il bit `+x`** sui file grezzi, mentre il
   tar preserva mode `755`;
4. impacchetta la GUI in **AppImage** con `packaging/build_appimage.sh`;
5. carica due artifact con **nome versionato+datato** (dalla version di
   `pyproject.toml`): `pickfair-linux-binary`
   (`Pickfair-Linux-bin-v<ver>-<AAAAMMGG>.tar.gz`) e `pickfair-linux-appimage`
   (`Pickfair-v<ver>-x86_64.AppImage`). I binari sono caricati **prima**
   dell'AppImage, così un fallimento di packaging non li tocca.

Si lancia:

- **on demand** dalla tab *Actions* → *Build Linux Binary* → *Run workflow*;
- su **tag `v*`**: gli artifact vengono anche allegati alla **GitHub Release**
  (permanenti; gli artifact di Actions scadono con la retention).

## Come si usa (utente finale)

**GUI (AppImage):**

```bash
chmod +x Pickfair-v*-x86_64.AppImage
./Pickfair-v*-x86_64.AppImage         # doppio click nel file manager equivale
```

**Binari (GUI + headless) dal `.tar.gz`** — l'archivio preserva il bit `+x`:

```bash
tar -xzf Pickfair-Linux-bin-v*.tar.gz     # estrae pickfair + pickfair-headless (già eseguibili)
./pickfair                                 # GUI
./pickfair-headless --telegram-login       # headless (VPS)
```

Per l'integrazione nel menu applicazioni (icona + voce), usa uno strumento come
`Gear Lever` o `appimaged`, oppure copia il `.desktop` manualmente. L'AppImage
resta comunque eseguibile con doppio click ovunque lo metti.

> Runtime: alcune distro richiedono `libfuse2` per montare le AppImage. In
> alternativa: `./Pickfair-x86_64.AppImage --appimage-extract-and-run` (nessun
> FUSE).

## Build locale (test / offline)

Serve solo il binario già costruito e `appimagetool` (scaricato in automatico):

```bash
pyinstaller --noconfirm --clean pickfair.spec     # produce dist/pickfair
packaging/build_appimage.sh dist/pickfair Pickfair-x86_64.AppImage
```

`packaging/build_appimage.sh` assembla l'AppDir (`AppRun`, `.desktop`, icona,
`usr/bin/pickfair`) e invoca `appimagetool` in modalità FUSE-less
(`APPIMAGE_EXTRACT_AND_RUN=1`). È la **fonte unica** usata sia dalla CI sia in
locale, così la logica di packaging è verificabile fuori da GitHub Actions.

### Integrità di `appimagetool` (supply-chain)

`appimagetool` viene scaricato dal canale ufficiale `continuous` (upstream **non
pubblica checksum** per le release stabili, quindi non c'è un tag immutabile con
hash pubblicato). Per non eseguire in CI un binario non verificato, si pinna un
hash **nostro vetted**:

- lo script **stampa sempre** lo `sha256` del binario scaricato;
- se `APPIMAGETOOL_SHA256` è impostato (env, o `APPIMAGETOOL_SHA256` nel workflow),
  il download viene **verificato** e in caso di mismatch lo script si ferma
  **fail-closed** (`exit 3`), senza produrre l'AppImage.

Quando upstream aggiorna `continuous`, l'hash cambia e la CI fallisce
volutamente: si **ri-verifica** il nuovo binario e si aggiorna il valore pinnato
in `.github/workflows/build-linux.yml` (step *Package GUI as AppImage*, env
`APPIMAGETOOL_SHA256`). L'URL è sovrascrivibile con `APPIMAGETOOL_URL`.

## Asset di packaging

| File | Ruolo |
|---|---|
| `packaging/build_appimage.sh` | assembla l'AppDir + genera l'AppImage |
| `packaging/AppRun` | entrypoint dell'AppImage (lancia `usr/bin/pickfair`) |
| `packaging/pickfair.desktop` | voce menu applicazioni (Name/Exec/Icon) |
| `packaging/pickfair.png` | icona 256×256 (**placeholder** — sostituibile) |
| `packaging/make_icon.py` | rigenera l'icona placeholder |

L'icona è un placeholder generato: per sostituirla con il logo vero, rimpiazza
`packaging/pickfair.png` (256×256) — o aggiorna `make_icon.py` e rigenera con
`python3 packaging/make_icon.py`.
