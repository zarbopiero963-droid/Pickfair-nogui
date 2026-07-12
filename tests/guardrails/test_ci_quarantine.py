from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from types import ModuleType

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
GUARD_PATH = REPOSITORY_ROOT / "scripts" / "ci" / "check_ci_quarantine.py"
WORKFLOW_PATH = REPOSITORY_ROOT / ".github" / "workflows" / "ci-quarantine-guard.yml"


def _load_guard() -> ModuleType:
    spec = importlib.util.spec_from_file_location("check_ci_quarantine", GUARD_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Impossibile caricare il guardrail da {GUARD_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


GUARD = _load_guard()


class CIQuarantineGuardTests(unittest.TestCase):
    def _make_repository(self, root: Path) -> None:
        active_dir = root / ".github" / "workflows"
        quarantine_dir = root / ".github" / "quarantined-workflows"
        active_dir.mkdir(parents=True)
        quarantine_dir.mkdir(parents=True)
        (active_dir / "safe.yml").write_text(
            "name: Safe\non: [pull_request]\njobs: {}\n",
            encoding="utf-8",
        )
        for workflow_name in GUARD.QUARANTINED_WORKFLOWS:
            (quarantine_dir / f"{workflow_name}.disabled").write_text(
                "name: Archived\n",
                encoding="utf-8",
            )

    def test_current_repository_passes(self) -> None:
        self.assertEqual(GUARD.check_repository(REPOSITORY_ROOT), [])

    def test_guard_workflow_is_read_only_and_pinned(self) -> None:
        content = WORKFLOW_PATH.read_text(encoding="utf-8")
        self.assertIn("contents: read", content)
        self.assertIn("runs-on: ubuntu-latest", content)
        self.assertNotIn("self-hosted", content)
        self.assertNotIn("${{ secrets.", content)
        self.assertNotIn("pull-requests: write", content)

        action_lines = [line for line in content.splitlines() if "uses:" in line]
        self.assertTrue(action_lines)
        for line in action_lines:
            reference = line.rsplit("@", 1)[-1].split("#", 1)[0].strip()
            self.assertEqual(len(reference), 40, line)
            self.assertTrue(all(character in "0123456789abcdef" for character in reference), line)

    def test_rejects_reactivated_workflow(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._make_repository(root)
            workflow_name = GUARD.QUARANTINED_WORKFLOWS[0]
            (root / ".github" / "workflows" / workflow_name).write_text(
                "name: Reactivated\n",
                encoding="utf-8",
            )

            errors = GUARD.check_repository(root)

            self.assertTrue(
                any("nuovamente attivo" in error for error in errors),
                errors,
            )

    def test_rejects_missing_archived_copy(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._make_repository(root)
            workflow_name = GUARD.QUARANTINED_WORKFLOWS[0]
            (root / ".github" / "quarantined-workflows" / f"{workflow_name}.disabled").unlink()

            errors = GUARD.check_repository(root)

            self.assertTrue(
                any("copia di quarantena mancante" in error for error in errors),
                errors,
            )

    def test_rejects_dispatch_from_active_workflow(self) -> None:
        workflow_name = GUARD.QUARANTINED_WORKFLOWS[0]
        dispatch_snippets = (
            f"      - run: gh workflow run {workflow_name}\n",
            f"      - run: curl /actions/workflows/{workflow_name}/dispatches\n",
            f"    uses: ./.github/workflows/{workflow_name}\n",
        )

        for dispatch_snippet in dispatch_snippets:
            with self.subTest(dispatch_snippet=dispatch_snippet):
                with tempfile.TemporaryDirectory() as temp_dir:
                    root = Path(temp_dir)
                    self._make_repository(root)
                    (root / ".github" / "workflows" / "dispatcher.yml").write_text(
                        "name: Dispatcher\n"
                        "on: workflow_dispatch\n"
                        "jobs:\n"
                        "  dispatch:\n"
                        "    runs-on: ubuntu-latest\n"
                        "    steps:\n"
                        f"{dispatch_snippet}",
                        encoding="utf-8",
                    )

                    errors = GUARD.check_repository(root)

                    self.assertTrue(
                        any(
                            "dispatch verso workflow quarantinato" in error
                            for error in errors
                        ),
                        errors,
                    )

    def test_rejects_reactivated_workflow_yaml_extension(self) -> None:
        # GitHub Actions esegue anche i workflow `.yaml`: la riattivazione con
        # l'estensione alternativa NON deve sfuggire al guard (Greptile P1).
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._make_repository(root)
            stem = Path(GUARD.QUARANTINED_WORKFLOWS[0]).stem
            (root / ".github" / "workflows" / f"{stem}.yaml").write_text(
                "name: Reactivated via yaml\n",
                encoding="utf-8",
            )

            errors = GUARD.check_repository(root)

            self.assertTrue(
                any("nuovamente attivo" in error for error in errors),
                errors,
            )

    def test_rejects_dispatch_with_flags_or_alt_extension(self) -> None:
        # Il dispatch verso un workflow quarantinato non deve evadere il controllo
        # con flag prima del nome (`--ref main`), con estensione `.yaml`, o senza
        # estensione (Qodo).
        workflow_name = GUARD.QUARANTINED_WORKFLOWS[0]
        stem = Path(workflow_name).stem
        dispatch_snippets = (
            f"      - run: gh workflow run --ref main {workflow_name}\n",
            f"      - run: gh workflow run -R owner/repo {stem}.yaml\n",
            f"      - run: gh workflow run {stem}\n",
            f"      - run: curl /actions/workflows/{stem}.yaml/dispatches\n",
            f"    uses: ./.github/workflows/{stem}.yaml\n",
        )

        for dispatch_snippet in dispatch_snippets:
            with self.subTest(dispatch_snippet=dispatch_snippet):
                with tempfile.TemporaryDirectory() as temp_dir:
                    root = Path(temp_dir)
                    self._make_repository(root)
                    (root / ".github" / "workflows" / "dispatcher.yml").write_text(
                        "name: Dispatcher\n"
                        "on: workflow_dispatch\n"
                        "jobs:\n"
                        "  dispatch:\n"
                        "    runs-on: ubuntu-latest\n"
                        "    steps:\n"
                        f"{dispatch_snippet}",
                        encoding="utf-8",
                    )

                    errors = GUARD.check_repository(root)

                    self.assertTrue(
                        any(
                            "dispatch verso workflow quarantinato" in error
                            for error in errors
                        ),
                        errors,
                    )


    def test_main_inspects_arbitrary_target_root(self) -> None:
        # Fail-closed (Fugu): il checker deve poter ispezionare una root ARBITRARIA
        # passata come argomento (l'albero della PR), non solo il proprio repo —
        # così il guard CI esegue lo script FIDATO del branch base contro l'albero
        # della PR, senza mai eseguire il codice (potenzialmente manomesso) della PR.
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._make_repository(root)
            self.assertEqual(GUARD.main([str(root)]), 0)

            stem = Path(GUARD.QUARANTINED_WORKFLOWS[0]).stem
            (root / ".github" / "workflows" / f"{stem}.yaml").write_text(
                "name: sneaky reactivation\n",
                encoding="utf-8",
            )
            self.assertEqual(GUARD.main([str(root)]), 1)


if __name__ == "__main__":
    unittest.main()
