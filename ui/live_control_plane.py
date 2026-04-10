from __future__ import annotations

from dataclasses import dataclass, field
import tkinter as tk
from tkinter import ttk
from typing import Any, Callable, Protocol


MODE_SIMULATION = "SIMULATION"
MODE_LIVE = "LIVE"

STATUS_SAFE_MODE = "SAFE_MODE"
STATUS_LIVE_ACTIVE = "LIVE_ACTIVE"
STATUS_LIVE_REQUESTED_BLOCKED = "LIVE_REQUESTED_BLOCKED"
STATUS_LIVE_BLOCKED = "LIVE_BLOCKED"
STATUS_UNKNOWN = "UNKNOWN"

DECISION_GO = "GO"
DECISION_NO_GO = "NO-GO"


class SettingsService(Protocol):
    def get(self, key: str, default: Any = None) -> Any:
        ...

    def set(self, key: str, value: Any) -> Any:
        ...


@dataclass(frozen=True)
class LiveReadinessReport:
    ready: bool
    level: str
    blockers: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class LiveControlState:
    execution_mode: str = MODE_SIMULATION
    live_enabled: bool = False
    kill_switch: bool = False
    readiness: LiveReadinessReport = field(
        default_factory=lambda: LiveReadinessReport(
            ready=False,
            level=STATUS_UNKNOWN,
            blockers=["READINESS_PROVIDER_MISSING"],
            details={"reason": "provider_missing"},
        )
    )


@dataclass(frozen=True)
class LiveDecision:
    outcome: str
    reason_code: str


class DictSettingsService:
    def __init__(self, initial: dict[str, Any] | None = None) -> None:
        self._store = dict(initial or {})

    def get(self, key: str, default: Any = None) -> Any:
        return self._store.get(key, default)

    def set(self, key: str, value: Any) -> Any:
        self._store[key] = value
        return value


def compute_effective_status(state: LiveControlState) -> str:
    if bool(state.kill_switch):
        return STATUS_LIVE_BLOCKED

    if str(state.execution_mode).upper() != MODE_LIVE:
        return STATUS_SAFE_MODE

    if not bool(state.live_enabled):
        return STATUS_LIVE_REQUESTED_BLOCKED

    readiness = state.readiness
    if str(getattr(readiness, "level", STATUS_UNKNOWN)).upper() == STATUS_UNKNOWN:
        return STATUS_LIVE_BLOCKED

    if not bool(getattr(readiness, "ready", False)):
        return STATUS_LIVE_BLOCKED

    return STATUS_LIVE_ACTIVE


def decide_live_action(state: LiveControlState, effective_status: str) -> LiveDecision:
    if effective_status == STATUS_LIVE_ACTIVE:
        return LiveDecision(outcome=DECISION_GO, reason_code="LIVE_READY")

    if state.kill_switch:
        return LiveDecision(outcome=DECISION_NO_GO, reason_code="KILL_SWITCH_ACTIVE")

    if str(state.execution_mode).upper() != MODE_LIVE:
        return LiveDecision(outcome=DECISION_NO_GO, reason_code="SIMULATION_MODE")

    if not state.live_enabled:
        return LiveDecision(outcome=DECISION_NO_GO, reason_code="LIVE_NOT_ENABLED")

    if str(state.readiness.level).upper() == STATUS_UNKNOWN:
        return LiveDecision(outcome=DECISION_NO_GO, reason_code="READINESS_UNKNOWN")

    if not state.readiness.ready:
        return LiveDecision(outcome=DECISION_NO_GO, reason_code="READINESS_BLOCKED")

    return LiveDecision(outcome=DECISION_NO_GO, reason_code="FAIL_CLOSED")


class LiveControlPlane(ttk.Frame):
    COLOR_BY_STATUS = {
        STATUS_SAFE_MODE: "#2e7d32",
        STATUS_LIVE_ACTIVE: "#2e7d32",
        STATUS_LIVE_REQUESTED_BLOCKED: "#f9a825",
        STATUS_LIVE_BLOCKED: "#c62828",
        STATUS_UNKNOWN: "#757575",
    }

    def __init__(
        self,
        parent: tk.Misc,
        *,
        settings_service: SettingsService,
        readiness_provider: Callable[[], LiveReadinessReport] | None = None,
    ) -> None:
        super().__init__(parent)
        self.settings_service = settings_service
        self.readiness_provider = readiness_provider

        self.execution_mode_var = tk.StringVar(value=MODE_SIMULATION)
        self.live_enabled_var = tk.BooleanVar(value=False)
        self.kill_switch_var = tk.BooleanVar(value=False)

        self.requested_mode_var = tk.StringVar(value=MODE_SIMULATION)
        self.effective_status_var = tk.StringVar(value=STATUS_UNKNOWN)
        self.readiness_level_var = tk.StringVar(value=STATUS_UNKNOWN)
        self.reason_var = tk.StringVar(value="INIT")
        self.last_decision_var = tk.StringVar(value=f"{DECISION_NO_GO}: INIT")

        self._build()
        self._load_from_settings()
        self.refresh_status()

    def _build(self) -> None:
        mode_box = ttk.LabelFrame(self, text="Mode Selector")
        mode_box.grid(row=0, column=0, sticky="ew", padx=8, pady=8)

        ttk.Radiobutton(
            mode_box,
            text=MODE_SIMULATION,
            value=MODE_SIMULATION,
            variable=self.execution_mode_var,
            command=self._on_controls_changed,
        ).grid(row=0, column=0, sticky="w", padx=6, pady=4)
        ttk.Radiobutton(
            mode_box,
            text=MODE_LIVE,
            value=MODE_LIVE,
            variable=self.execution_mode_var,
            command=self._on_controls_changed,
        ).grid(row=0, column=1, sticky="w", padx=6, pady=4)

        toggle_box = ttk.LabelFrame(self, text="Toggles")
        toggle_box.grid(row=1, column=0, sticky="ew", padx=8, pady=8)
        ttk.Checkbutton(
            toggle_box,
            text="live_enabled",
            variable=self.live_enabled_var,
            command=self._on_controls_changed,
        ).grid(row=0, column=0, sticky="w", padx=6, pady=4)
        ttk.Checkbutton(
            toggle_box,
            text="kill_switch",
            variable=self.kill_switch_var,
            command=self._on_controls_changed,
        ).grid(row=0, column=1, sticky="w", padx=6, pady=4)

        status_box = ttk.LabelFrame(self, text="Status Display")
        status_box.grid(row=2, column=0, sticky="nsew", padx=8, pady=8)

        ttk.Label(status_box, text="Requested Mode").grid(row=0, column=0, sticky="w", padx=6, pady=2)
        ttk.Label(status_box, textvariable=self.requested_mode_var).grid(row=0, column=1, sticky="w", padx=6, pady=2)

        ttk.Label(status_box, text="Effective Status").grid(row=1, column=0, sticky="w", padx=6, pady=2)
        self.effective_status_label = tk.Label(status_box, textvariable=self.effective_status_var, fg="white", bg="#757575")
        self.effective_status_label.grid(row=1, column=1, sticky="w", padx=6, pady=2)

        ttk.Label(status_box, text="Readiness Level").grid(row=2, column=0, sticky="w", padx=6, pady=2)
        ttk.Label(status_box, textvariable=self.readiness_level_var).grid(row=2, column=1, sticky="w", padx=6, pady=2)

        ttk.Label(status_box, text="Reason").grid(row=3, column=0, sticky="w", padx=6, pady=2)
        ttk.Label(status_box, textvariable=self.reason_var).grid(row=3, column=1, sticky="w", padx=6, pady=2)

        ttk.Label(status_box, text="Last Decision").grid(row=4, column=0, sticky="w", padx=6, pady=2)
        ttk.Label(status_box, textvariable=self.last_decision_var).grid(row=4, column=1, sticky="w", padx=6, pady=2)

        blockers_box = ttk.LabelFrame(self, text="Blockers")
        blockers_box.grid(row=3, column=0, sticky="nsew", padx=8, pady=8)

        self.blockers_listbox = tk.Listbox(blockers_box, height=6)
        self.blockers_listbox.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(3, weight=1)

    def _coerce_mode(self, value: Any) -> str:
        return MODE_LIVE if str(value).upper() == MODE_LIVE else MODE_SIMULATION

    def _coerce_bool(self, value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            norm = value.strip().lower()
            if norm in {"1", "true", "yes", "on"}:
                return True
            if norm in {"0", "false", "no", "off"}:
                return False
            return default
        if isinstance(value, (int, float)):
            return bool(value)
        return default

    def _load_from_settings(self) -> None:
        mode = self._coerce_mode(self.settings_service.get("execution_mode", MODE_SIMULATION))
        live_enabled = self._coerce_bool(self.settings_service.get("live_enabled", False), default=False)
        kill_switch = self._coerce_bool(self.settings_service.get("kill_switch", False), default=False)

        self.execution_mode_var.set(mode)
        self.live_enabled_var.set(live_enabled)
        self.kill_switch_var.set(kill_switch)

        self._persist_controls()

    def reload_settings(self) -> None:
        self._load_from_settings()
        self.refresh_status()

    def _persist_controls(self) -> None:
        self.settings_service.set("execution_mode", self.execution_mode_var.get())
        self.settings_service.set("live_enabled", bool(self.live_enabled_var.get()))
        self.settings_service.set("kill_switch", bool(self.kill_switch_var.get()))

    def _resolve_readiness(self) -> LiveReadinessReport:
        if self.readiness_provider is None:
            return LiveReadinessReport(
                ready=False,
                level=STATUS_UNKNOWN,
                blockers=["READINESS_PROVIDER_MISSING"],
                details={"error": "missing_provider"},
            )
        try:
            result = self.readiness_provider()
            if not isinstance(result, LiveReadinessReport):
                return LiveReadinessReport(
                    ready=False,
                    level=STATUS_UNKNOWN,
                    blockers=["READINESS_PROVIDER_INVALID"],
                    details={"error": "invalid_report"},
                )
            return result
        except Exception as exc:
            return LiveReadinessReport(
                ready=False,
                level=STATUS_UNKNOWN,
                blockers=["READINESS_PROVIDER_ERROR"],
                details={"error": str(exc)},
            )

    def get_state(self) -> LiveControlState:
        return LiveControlState(
            execution_mode=self._coerce_mode(self.execution_mode_var.get()),
            live_enabled=bool(self.live_enabled_var.get()),
            kill_switch=bool(self.kill_switch_var.get()),
            readiness=self._resolve_readiness(),
        )

    def refresh_status(self) -> None:
        state = self.get_state()
        effective = compute_effective_status(state)
        decision = decide_live_action(state, effective)

        self.requested_mode_var.set(state.execution_mode)
        self.effective_status_var.set(effective)
        self.readiness_level_var.set(state.readiness.level)
        self.reason_var.set(decision.reason_code)
        self.last_decision_var.set(f"{decision.outcome}: {decision.reason_code}")
        self._paint_status(effective)
        self._render_blockers(state.readiness.blockers)

    def _paint_status(self, effective_status: str) -> None:
        color = self.COLOR_BY_STATUS.get(effective_status, self.COLOR_BY_STATUS[STATUS_UNKNOWN])
        self.effective_status_label.configure(bg=color)

    def _render_blockers(self, blockers: list[str]) -> None:
        self.blockers_listbox.delete(0, tk.END)
        normalized = list(blockers or [])
        if not normalized:
            normalized = ["NONE"]
        for blocker in normalized:
            self.blockers_listbox.insert(tk.END, blocker)

    def _on_controls_changed(self) -> None:
        self._persist_controls()
        self.refresh_status()


def build_example_window() -> tk.Tk:
    root = tk.Tk()
    root.title("Live/SIM Control Plane")

    settings = DictSettingsService()

    def demo_readiness() -> LiveReadinessReport:
        return LiveReadinessReport(
            ready=False,
            level="NOT_READY",
            blockers=["LIVE_DEPENDENCY_MISSING", "RUNTIME_NOT_INITIALIZED"],
            details={"source": "demo"},
        )

    panel = LiveControlPlane(root, settings_service=settings, readiness_provider=demo_readiness)
    panel.pack(fill="both", expand=True)
    return root


if __name__ == "__main__":
    app = build_example_window()
    app.mainloop()
