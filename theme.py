"""
theme.py

Tema centralizzato per Pickfair UI.
Usato da:
- mini_gui.py
- telegram_tab_ui.py
- telegram_module.py

Obiettivo:
- Unificare colori e font
- Evitare hardcode sparsi
"""

from __future__ import annotations

# =========================================================
# COLORS
# =========================================================

COLORS = {
    # Background
    "bg_dark": "#111827",
    "bg_panel": "#1f2937",
    "bg_card": "#374151",
    "bg_hover": "#4b5563",

    # Borders
    "border": "#6b7280",

    # Text
    "text_primary": "#f9fafb",
    "text_secondary": "#d1d5db",
    "text_tertiary": "#9ca3af",

    # States
    "success": "#22c55e",
    "error": "#ef4444",
    "loss": "#ef4444",
    "warning": "#f59e0b",

    # Betting colors
    "back": "#2563eb",
    "back_hover": "#1d4ed8",
    "lay": "#f43f5e",
    "lay_hover": "#e11d48",

    # Buttons
    "button_primary": "#2563eb",
    "button_secondary": "#475569",
    "button_success": "#16a34a",
    "button_danger": "#dc2626",

    # Telegram specific
    "telegram": "#229ED9",
    "telegram_dark": "#1b8cc9",
}

# =========================================================
# FONTS
# =========================================================

FONTS = {
    "heading": ("Segoe UI", 14, "bold"),
    "subheading": ("Segoe UI", 12, "bold"),
    "body": ("Segoe UI", 11),
    "small": ("Segoe UI", 9),
    "mono": ("Consolas", 10),
}

# =========================================================
# HELPERS
# =========================================================

def get_color(name: str, fallback: str = "#ffffff") -> str:
    """Safe color getter"""
    return COLORS.get(name, fallback)


def get_font(name: str, fallback=("Segoe UI", 11)):
    """Safe font getter"""
    return FONTS.get(name, fallback)


# =========================================================
# OPTIONAL: STATUS COLOR HELPERS
# =========================================================

def status_color(status: str) -> str:
    status = str(status or "").upper()

    if status in ("OK", "CONNECTED", "ACTIVE", "MATCHED"):
        return COLORS["success"]

    if status in ("WARNING", "PARTIAL", "BORDERLINE"):
        return COLORS["warning"]

    if status in ("ERROR", "FAILED", "DISCONNECTED"):
        return COLORS["error"]

    return COLORS["text_secondary"]