from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from types import ModuleType

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CHECKER_PATH = REPOSITORY_ROOT / "scripts" / "check_merge_markers.py"


def _load_checker() -> ModuleType:
    spec = importlib.util.spec_from_file_location("check_merge_markers", CHECKER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Impossibile caricare il checker da {CHECKER_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CHECKER = _load_checker()


class CheckMergeMarkersTests(unittest.TestCase):
    """Test unitari del rilevatore di marker di conflitto git."""

    def test_decorative_equals_line_is_not_a_marker(self) -> None:
        # BLOCK: una riga decorativa di soli `=` (più lunga di 7) NON è un
        # marker di conflitto — falso positivo storico su betfair_market_api.py:3.
        self.assertFalse(CHECKER.is_conflict_marker("=" * 52))
        self.assertFalse(CHECKER.is_conflict_marker("=" * 8))
        self.assertFalse(CHECKER.is_conflict_marker("<" * 20))
        self.assertFalse(CHECKER.is_conflict_marker(">" * 20))
        self.assertFalse(CHECKER.is_conflict_marker("|" * 20))

    def test_real_conflict_markers_are_detected(self) -> None:
        # I veri marker git (7 char esatti, o 7 + spazio + etichetta) restano rilevati.
        self.assertTrue(CHECKER.is_conflict_marker("======="))
        self.assertTrue(CHECKER.is_conflict_marker("<<<<<<<"))
        self.assertTrue(CHECKER.is_conflict_marker(">>>>>>>"))
        self.assertTrue(CHECKER.is_conflict_marker("<<<<<<< HEAD"))
        self.assertTrue(CHECKER.is_conflict_marker(">>>>>>> feature/x"))
        # Marker della modalità diff3/zdiff3 (base comune).
        self.assertTrue(CHECKER.is_conflict_marker("|||||||"))
        self.assertTrue(CHECKER.is_conflict_marker("||||||| merged common ancestors"))

    def test_scan_ignores_decorative_line_but_flags_real_marker(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "decorativo.py").write_text(
                '"""\nTitolo\n' + ("=" * 52) + '\nCorpo\n"""\n',
                encoding="utf-8",
            )
            self.assertEqual(CHECKER.scan_for_markers(root), [])

            (root / "conflitto.txt").write_text(
                "riga\n<<<<<<< HEAD\nmia\n=======\ntua\n>>>>>>> branch\n",
                encoding="utf-8",
            )
            found = CHECKER.scan_for_markers(root)
            flagged_lines = {line for _, _, line in found}
            self.assertIn("<<<<<<< HEAD", flagged_lines)
            self.assertIn("=======", flagged_lines)
            self.assertIn(">>>>>>> branch", flagged_lines)
            # Il file decorativo non deve comparire tra i flag.
            self.assertTrue(all(path.name == "conflitto.txt" for path, _, _ in found))

    def test_main_status_codes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "ok.py").write_text("x = 1\n" + ("=" * 30) + "\n", encoding="utf-8")
            self.assertEqual(CHECKER.main([str(root)]), 0)

            (root / "bad.py").write_text("=======\n", encoding="utf-8")
            self.assertEqual(CHECKER.main([str(root)]), 1)

    def test_repository_has_no_real_markers(self) -> None:
        # Il repo reale (inclusa la riga decorativa di betfair_market_api.py:3)
        # non deve contenere marker di conflitto.
        found = [
            (path, lineno, line)
            for path, lineno, line in CHECKER.scan_for_markers(REPOSITORY_ROOT)
            if ".venv" not in str(path) and "site-packages" not in str(path)
        ]
        self.assertEqual(found, [])


if __name__ == "__main__":
    unittest.main()
