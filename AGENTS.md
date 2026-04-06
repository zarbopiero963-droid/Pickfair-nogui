AGENTS.md

🎯 OBJECTIVE

Enforce a clean, deterministic Git workflow when using Codex.
Prevent branch fragmentation, PR duplication, and merge conflicts.

---

🔒 HARD RULES (MANDATORY)

1. NEVER WORK ON CODEX BRANCH

- Do NOT open PRs targeting "Codex"
- Do NOT use "Codex" as base branch
- ALWAYS target "main"

---

2. SINGLE PR PER FEATURE

- One feature = ONE PR
- Do NOT open follow-up PRs for the same files
- If changes are needed → UPDATE existing branch

---

3. NO PARALLEL EDITS ON SAME FILES

If any open PR modifies a file:

- DO NOT create another PR touching the same file
- STOP and report conflict risk

---

4. STRICT FILE SCOPE

When task specifies files:

- ONLY modify those files
- DO NOT touch unrelated modules
- DO NOT reformat entire repo

If additional files are required:

- STOP and report

---

5. NO CASCADE PRs

Forbidden pattern:

- PR15 → PR16 → PR17 → PR19 on same files

Allowed pattern:

- PR15 → updated (same branch) → merged

---

6. ALWAYS CHECK BASE BEFORE WORK

Before making changes:

- Ensure branch is up to date with "main"
- If not → rebase or stop

---

7. SMALL FIXES → SMALL PRs

For hotfixes (like forensics rules):

- Modify only minimal files
- Keep PR atomic and isolated

---

8. NO HIDDEN STATE ASSUMPTIONS

- Do not rely on previous Codex runs
- Always work from current repo state

---

🧠 PREFERRED WORKFLOW

STEP 1 — INITIAL PATCH (Codex)

- Generate patch
- Create PR → target "main"

STEP 2 — REFINEMENT (LOCAL)

- Checkout branch locally
- Apply fixes
- Push to SAME branch

STEP 3 — MERGE

- Merge ONLY when:
  - CI green
  - no conflicts
  - scope respected

---

⚠️ CONFLICT PREVENTION POLICY

If conflict is detected or likely:

- DO NOT auto-resolve blindly
- DO NOT duplicate logic blocks
- KEEP ONLY final deterministic version

---

🧪 TEST POLICY

- Run ONLY relevant tests for the change
- Do NOT trigger full test suite unless requested

---

🚫 FORBIDDEN ACTIONS

- Creating new branches for same task
- Opening multiple PRs for same fix
- Mixing unrelated changes
- Editing files outside scope
- Rebasing without explicit instruction

---

✅ SUCCESS CRITERIA

A correct Codex task MUST:

- Modify ONLY allowed files
- Produce ONE clean PR
- Avoid conflicts
- Keep logic deterministic
- Pass targeted tests

---

🧾 FINAL OUTPUT FORMAT

Codex must report:

- branch used
- changed files
- confirmation of scope lock
- summary of fix
- test results
- push status

---

🔥 GOLDEN RULE

👉 ONE TASK = ONE BRANCH = ONE PR = ONE MERGE

If this rule is violated → STOP.