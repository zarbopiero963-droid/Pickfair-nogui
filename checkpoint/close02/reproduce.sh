#!/usr/bin/env bash
# close02 checkpoint — riproduzione dei controlli completati (solo continuita').
# Uso (dalla radice del repo, branch checkpoint/gate-a-close02):
#   PY=python3.11 bash checkpoint/close02/reproduce.sh [v13|v14]
# Richiede: Python 3.11, pip install -r requirements-test.txt (pytest>=9, pytest-xdist).
set -euo pipefail
WHICH="${1:-v13}"
PY="${PY:-python3.11}"
R="$(git rev-parse --show-toplevel)"; CK="$R/checkpoint/close02"
W="$(mktemp -d)"; BF2="$W/bf2"; mkdir -p "$BF2"
cp -r "$CK/bf2/." "$BF2/"; cp -r "$CK/artifacts" "$BF2/close02"
git -C "$R" worktree add --detach "$BF2/close02/wt_base" 028cee9 >/dev/null
git -C "$R" worktree add --detach "$BF2/close02/wt" 028cee9 >/dev/null
P="$BF2/close02/patches"
git -C "$BF2/close02/wt" apply --check "$P/close02_findings_proposal_v13.patch"
git -C "$BF2/close02/wt" apply "$P/close02_findings_proposal_v13.patch"
if [ "$WHICH" = "v14" ]; then
  git -C "$BF2/close02/wt" apply --check "$P/close02_v13_to_v14_delta.patch"
  git -C "$BF2/close02/wt" apply "$P/close02_v13_to_v14_delta.patch"   # v14 NON VERIFICATO
fi
PYABS="$(command -v "$PY")"
for f in "$BF2"/close02/*.py "$BF2"/repair01/*.py; do
  sed -i "s#/workspace/pickfair/venv311/bin/python#$PYABS#g; s#/workspace/pickfair/bf2/#$BF2/#g" "$f"
done
echo "== sha256 patch"; (cd "$P" && sha256sum close02_findings_proposal_v13.patch close02_v13_to_v14_delta.patch close02_findings_proposal_v14.patch)
echo "== tests/close02 ($WHICH)"; (cd "$BF2/close02/wt" && "$PY" -m pytest -q -p no:cacheprovider -o addopts="" tests/close02 | tail -1)
cd "$BF2/close02"
echo "== preflight L7b (base)";   "$PY" preflight_L7b.py record_specific_L7b.json | tail -1
echo "== preflight L9 (base)";    "$PY" preflight_L9.py record_specific_L9.json | head -1
echo "== indep L7b (patched)";    "$PY" indep_check_L7b.py record_specific_L7b_patched.json | tail -2
echo "== indep L9 (patched)";     "$PY" indep_check_L9.py record_specific_L9_patched.json | tail -1
echo "== test preflight/indep";   "$PY" -m pytest -q -p no:cacheprovider -o addopts="" test_preflight_L7b.py test_preflight_L9.py test_t80u.py | tail -1
echo "WORKDIR=$W"
