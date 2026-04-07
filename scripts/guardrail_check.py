import json
import re
import sys


def load_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def fail(message: str):
    print(f"❌ {message}")
    sys.exit(1)


# --- LOAD CHANGED FILES ---
data = load_json("pr_files_raw.json")
changed_files = [x["filename"] for x in data if "filename" in x]

# --- LOAD PR METADATA ---
pr = load_json("pr_meta.json")
title = pr.get("title", "") or ""
body = pr.get("body", "") or ""
text = f"{title}\n{body}"

# --- EXTRACT TASK FROM PR TITLE/BODY (FAIL-CLOSED) ---
match = re.search(r"\[TASK:\s*([^\]]+)\]", text)
if not match:
    fail("Missing [TASK: ...] tag in PR title/body")

task = match.group(1).strip()
if not task:
    fail("TASK tag is empty")

# --- LOAD CONFIG ---
config = load_json(".guardrails/allowed_scope.json")

task_cfg = config.get("tasks", {}).get(task)
if not task_cfg:
    fail(f"Unknown task: {task}")

allowed_files = set(task_cfg.get("files", []))
allow_tests = bool(task_cfg.get("allow_tests", config["default"].get("allow_tests", True)))
max_files = int(task_cfg.get("max_files", config["default"].get("max_files", 8)))

# --- DEBUG OUTPUT ---
print(f"Detected task: {task}")
print(f"Max files allowed: {max_files}")
print(f"Allow tests: {allow_tests}")
print("Allowed files:")
for f in sorted(allowed_files):
    print("-", f)

print("\nChanged files:")
for f in changed_files:
    print("-", f)

# --- RULE 1: FILE COUNT LIMIT ---
if len(changed_files) > max_files:
    fail(f"Too many files changed for task '{task}' (max={max_files})")

# --- RULE 2: SCOPE VALIDATION (ALWAYS ENFORCED) ---
invalid = []

for f in changed_files:
    if f in allowed_files:
        continue
    if allow_tests and f.startswith("tests/"):
        continue
    invalid.append(f)

if invalid:
    print("\n❌ INVALID FILES DETECTED:")
    for f in invalid:
        print("-", f)
    sys.exit(1)

print("\n✅ Scope valid")
