import json
import re
import sys


def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# --- LOAD CHANGED FILES ---
data = load_json("pr_files_raw.json")
changed_files = [x["filename"] for x in data if "filename" in x]

# --- LOAD PR METADATA ---
pr = load_json("pr_meta.json")
title = pr.get("title", "") or ""
body = pr.get("body", "") or ""
text = f"{title}\n{body}"

# --- EXTRACT TASK FROM PR TITLE/BODY ---
match = re.search(r"\[TASK:\s*([^\]]+)\]", text)

task = None

if match:
    task = match.group(1).strip()
else:
    # --- SMART FALLBACK ---
    if any("core/trading_engine.py" == f or "trading_engine" in f for f in changed_files):
        task = "trading_engine"
    elif any("order_manager.py" == f or "order_manager" in f for f in changed_files):
        task = "order_manager"
    elif any("core/reconciliation_engine.py" == f or "reconciliation_engine" in f for f in changed_files):
        task = "reconciliation"
    elif any(f == "database.py" for f in changed_files):
        task = "database"
    elif any("core/runtime_controller.py" == f or "runtime_controller" in f for f in changed_files):
        task = "runtime"

if not task:
    print("❌ Cannot determine TASK")
    print("Add [TASK: ...] to PR title/body or extend fallback rules.")
    sys.exit(1)

# --- LOAD CONFIG ---
config = load_json(".guardrails/allowed_scope.json")

task_cfg = config.get("tasks", {}).get(task)
if not task_cfg:
    print(f"❌ Unknown task: {task}")
    sys.exit(1)

allowed_files = set(task_cfg.get("files", []))
allow_tests = bool(task_cfg.get("allow_tests", config["default"].get("allow_tests", True)))
max_files = int(task_cfg.get("max_files", config["default"].get("max_files", 8)))

# --- DEBUG OUTPUT ---
print(f"Detected task: {task}")
print(f"Max files allowed: {max_files}")
print(f"Allow tests: {allow_tests}")
print("Allowed production files:")
for f in sorted(allowed_files):
    print("-", f)

print("\nChanged files:")
for f in changed_files:
    print("-", f)

# --- RULE 1: FILE COUNT LIMIT ---
if len(changed_files) > max_files:
    print(f"\n❌ Too many files changed for task '{task}' (max={max_files})")
    sys.exit(1)

# --- RULE 2: SCOPE VALIDATION ---
invalid = []

for f in changed_files:
    if f in allowed_files:
        continue
    if allow_tests and f.startswith("tests/"):
        continue
    invalid.append(f)

# --- RULE 3: SINGLE-FILE STRICT MODE ---
if len(allowed_files) == 1:
    non_test_files = [f for f in changed_files if not f.startswith("tests/")]
    if len(non_test_files) > 1:
        print("\n❌ Single-file task violated")
        for f in non_test_files:
            print("-", f)
        sys.exit(1)

# --- FINAL RESULT ---
if invalid:
    print("\n❌ INVALID FILES DETECTED:")
    for f in invalid:
        print("-", f)
    sys.exit(1)

print("\n✅ Scope valid")