# Workflow privilegiati in quarantena

Questa directory conserva workflow rimossi temporaneamente da `.github/workflows/` in seguito all'audit di sicurezza del 2026-07-11. GitHub Actions carica i workflow soltanto dalla directory attiva `.github/workflows/`; l'estensione `.yml.disabled` e la posizione corrente impediscono quindi trigger automatici o manuali.

## Motivo del contenimento

I workflow qui archiviati combinavano uno o più dei seguenti elementi: input controllabile da pull request, esecuzione su runner self-hosted persistenti, credenziali di scrittura, checkout o installazione di contenuto non fidato, invocazione di agenti automatici e push verso branch remoti. Alcuni supervisor potevano inoltre rilanciare automaticamente il percorso privilegiato.

| Workflow | Stato | Condizione minima per la riattivazione |
|---|---|---|
| `auto-pr-codex-fix.yml.disabled` | Quarantena | Separazione completa tra analisi non fidata e applicazione privilegiata; nessun token o rete nel job di analisi. |
| `auto-pr-codex-supervisor.yml.disabled` | Quarantena | Nessun rerun automatico del workflow privilegiato; dispatch con approvazione umana verificabile. |
| `auto-pr-e2e-autofix.yml.disabled` | Quarantena | Eliminazione di loop e stato persistente cross-run; nessun push automatico guidato da input PR. |
| `new-task-codex-pr.yml.disabled` | Quarantena | YAML valido, prompt delimitato e nessuna automazione full-auto con credenziali di scrittura. |
| `pr-autofix-safe-supervisor.yml.disabled` | Quarantena | Nessuna esecuzione di script o dipendenze dalla PR con segreti disponibili. |
| `pr-autofix-selfhosted.yml.disabled` | Quarantena | Runner effimero e isolato; token di scrittura assente durante l'elaborazione del contenuto PR. |
| `pr-automation-controller.yml.disabled` | Quarantena | YAML valido e controller read-only senza esecuzione o dispatch guidati da contenuto non fidato. |
| `pr-automation-controller-v2.yml.disabled` | Quarantena | Controller read-only e incapace di attivare percorsi privilegiati senza approvazione. |

## Regole di riattivazione

Una riattivazione deve avvenire in una PR dedicata e includere un threat model aggiornato, test red-team, permessi minimi, action fissate a commit SHA, runner effimeri e una separazione strutturale fra job non fidati e job con capacità di scrittura. Non è sufficiente rinominare o spostare nuovamente un file.

Il controllo `CI Quarantine Guard` impedisce che questi nomi tornino nella directory attiva o che un workflow attivo li ridispatchi accidentalmente.
