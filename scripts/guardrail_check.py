import json
import sys
import re

# --- LOAD PR FILES ---
with open("pr_files_raw.json") as f:
    data = json.load(f)

changed_files = [x["filename"] for x in data if "filename" in x]

# --- LOAD PR META ---
with open("pr_meta.json") as f:
    pr = json.load(f)

title = pr.get("title", "")
body = pr.get("body", "")

text = title + "\n" + body

# --- EXTRACT TASK ---
match = re.search(r"\[TASK:\s*(.*?)\]", text)

if not match:
    print("❌ Missing [TASK: ...] in PR title/body")
    sys.exit(1)

task = match.group(1).strip()

# --- LOAD CONFIG ---
with open(".guardrails/allowed_scope.json") as f:
    config = json.load(f)

task_cfg = config["tasks"].get(task, config["default"])

allowed_files = set(task_cfg.get("files", []))
allow_tests = task_cfg.get("allow_tests", True)
max_files = task_cfg.get("max_files", config["default"]["max_files"])

# --- RULE 1: FILE COUNT LIMIT ---
if len(changed_files) > max_files:
    print(f"❌ Too many files changed for task '{task}' (max={max_files})")
    for f in changed_files:
        print("-", f)
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
        print("❌ Single-file task violated")
        for f in non_test_files:
            print("-", f)
        sys.exit(1)

# --- OUTPUT ---
print(f"✅ Task: {task}")
print("Changed files:")
for f in changed_files:
    print("-", f)

if invalid:
    print("\n❌ INVALID FILES:")
    for f in invalid:
        print("-", f)
    sys.exit(1)
else:
    print("\n✅ Scope valid")