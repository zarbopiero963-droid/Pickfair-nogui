# D03 v2 lotto 1 — ERRATA 03 (rilievi Sol/Grok senza risposta, mandato PF-PR-RECOVERY-DECISIONS-02 §5)

> Revisione indipendente BF2/D03 (OD-01, #426). Tre rilievi di contenuto dei revisori pagati erano rimasti senza risposta nei documenti; qui sono analizzati, corretti o motivatamente respinti, con la prova.

| ID | Revisore (commento PR #508) | Rilievo | Esito | Correzione |
|---|---|---|---|---|
| DIFF-PF-BF-16-STATE (`20_D03_LOTTO01_A_04.md`) | GPT-6.1 Sol c.6127064719 | la formula «matched(old) + remaining(new)» riserva 0,04 invece di 0,10 se i nuovi 0,06 sono abbinati subito | **ACCETTATO** | esposizione = matched(old) + matched(new) + remaining(new) = matched(old) + size(new); oracle esteso al caso «nuovo ordine abbinato subito» con negativo «riservare solo 0,04» |
| DIFF-PF-BF-09-ERROR (`21_D03_LOTTO01_B_04.md`) | Grok 4.7 c.6127153553 | Fonte A vieta il retry monetario dopo timeout post-send ma l'oracle copriva solo OPEN/CLOSED; l'atteso NO_ENTRY era orfano in `B_05` | **ACCETTATO** | oracle riscritto: errore/timeout ⇒ NO_ENTRY (MARKET_DATA_UNRELIABLE), lettura ripetibile ma mai un ordine su dati vecchi; l'oracle OPEN/CLOSED resta in DIFF-PF-BF-09-STATE; l'atteso di `B_05` è ancorato qui |
| DIFF-PF-BF-S02-SCHEMA (`21_D03_LOTTO01_B_06.md`) | Grok 4.7 c.6127153553 | codice Order Stream MISSING ma conformità alla firma 2026-05-11 affermata e «decisione owner non necessaria» | **ACCETTATO** | interpretazione riscritta come requisito DOCUMENTED con implementazione assente, nessuna conformità dichiarata; decisione owner necessaria: SÌ (implementazione scheda Order Stream #461 r.22, sorgente autorevole di fill/ledger); stato DOCUMENTED_IMPLEMENTATION_MISSING |

Nessun test eseguito da questa errata (le schede restano `NOT_RUN`); nessun codice toccato; la PR resta documentale e **non va mergiata**. Le tre schede restano `REVIEW_PENDING` fino al verdetto per ID dell'owner su #506.
