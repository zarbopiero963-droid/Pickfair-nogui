#!/usr/bin/env bash
# Assembla un AppImage della GUI Pickfair a partire dal binario PyInstaller
# onefile gia' costruito (`dist/pickfair`). Fonte unica usata sia dalla CI
# (build-linux.yml) sia dai test locali, cosi' la logica di packaging e'
# eseguibile e verificabile fuori da GitHub Actions.
#
# Uso:
#   packaging/build_appimage.sh <path-binario> [output.AppImage]
# Esempio:
#   packaging/build_appimage.sh dist/pickfair Pickfair-x86_64.AppImage
#
# Requisiti: bash, curl (per scaricare appimagetool se assente). Su runner
# senza FUSE si usa APPIMAGE_EXTRACT_AND_RUN=1 (niente mount FUSE).
set -euo pipefail

BIN="${1:-dist/pickfair}"
OUT="${2:-Pickfair-x86_64.AppImage}"
ARCH="${ARCH:-x86_64}"
HERE="$(cd "$(dirname "${0}")" && pwd)"
APPDIR="$(pwd)/Pickfair.AppDir"

if [ ! -x "${BIN}" ]; then
  echo "ERRORE: binario non trovato o non eseguibile: ${BIN}" >&2
  exit 2
fi

echo ">> Assemblo AppDir in ${APPDIR}"
rm -rf "${APPDIR}"
mkdir -p "${APPDIR}/usr/bin"
install -m 0755 "${BIN}" "${APPDIR}/usr/bin/pickfair"

# AppRun + .desktop + icona: sia a root dell'AppDir (richiesto da appimagetool)
# sia nelle dir standard (per l'integrazione desktop una volta installato).
install -m 0755 "${HERE}/AppRun" "${APPDIR}/AppRun"
install -m 0644 "${HERE}/pickfair.desktop" "${APPDIR}/pickfair.desktop"
install -m 0644 "${HERE}/pickfair.png" "${APPDIR}/pickfair.png"
mkdir -p "${APPDIR}/usr/share/applications" \
         "${APPDIR}/usr/share/icons/hicolor/256x256/apps"
install -m 0644 "${HERE}/pickfair.desktop" "${APPDIR}/usr/share/applications/pickfair.desktop"
install -m 0644 "${HERE}/pickfair.png" "${APPDIR}/usr/share/icons/hicolor/256x256/apps/pickfair.png"

# appimagetool: usa quello in PATH se presente, altrimenti scaricalo dal canale
# ufficiale. `continuous` è il canale di distribuzione ufficiale (upstream NON
# pubblica checksum per le release stabili), quindi pinniamo l'integrità con un
# hash NOSTRO vetted: se APPIMAGETOOL_SHA256 è impostato, il download viene
# verificato e in caso di mismatch ci si ferma FAIL-CLOSED (drift/compromissione
# a monte → re-vet cosciente). L'URL è sovrascrivibile con APPIMAGETOOL_URL.
APPIMAGETOOL_URL="${APPIMAGETOOL_URL:-https://github.com/AppImage/appimagetool/releases/download/continuous/appimagetool-${ARCH}.AppImage}"
APPIMAGETOOL_SHA256="${APPIMAGETOOL_SHA256:-}"
TOOL="$(command -v appimagetool || true)"
if [ -z "${TOOL}" ]; then
  TOOL="$(pwd)/appimagetool-${ARCH}.AppImage"
  if [ ! -x "${TOOL}" ]; then
    echo ">> Scarico appimagetool da ${APPIMAGETOOL_URL}"
    curl -fsSL -o "${TOOL}" "${APPIMAGETOOL_URL}"
  fi
  got="$(sha256sum "${TOOL}" | awk '{print $1}')"
  echo ">> appimagetool sha256: ${got}"
  if [ -n "${APPIMAGETOOL_SHA256}" ] && [ "${got}" != "${APPIMAGETOOL_SHA256}" ]; then
    echo "ERRORE: sha256 appimagetool inatteso (atteso ${APPIMAGETOOL_SHA256}, ottenuto ${got}). Fermo fail-closed." >&2
    rm -f "${TOOL}"
    exit 3
  fi
  chmod +x "${TOOL}"
fi

echo ">> Genero ${OUT} con ${TOOL}"
# APPIMAGE_EXTRACT_AND_RUN evita FUSE (assente sui runner GitHub e in sandbox);
# --no-appstream salta la validazione appstream (non abbiamo metainfo.xml).
ARCH="${ARCH}" APPIMAGE_EXTRACT_AND_RUN=1 "${TOOL}" --no-appstream "${APPDIR}" "${OUT}"

echo ">> Fatto: ${OUT}"
ls -lh "${OUT}"
