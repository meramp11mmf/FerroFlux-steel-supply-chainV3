# ============================================================
# DASHBOARD CONFIG - Database Connection
# ============================================================
import os as _os

# All connection details sourced from env vars — no hardcoded credentials
DB_CONFIG = {
    "host":     _os.getenv("PG_HOST",     "127.0.0.1"),
    "port":     int(_os.getenv("PG_PORT", "5432")),
    "dbname":   _os.getenv("PG_DB",       "steel_db"),
    "user":     _os.getenv("PG_USER",     _os.getenv("POSTGRES_USER",     "steel_admin")),
    "password": _os.getenv("PG_PASSWORD", _os.getenv("POSTGRES_PASSWORD", "")),
}

COLORS = {
    "primary":   "#00AEEF",   # Electric Blue — accent
    "secondary": "#C7CDD4",   # Steel Silver
    "accent":    "#FF4D6D",   # Error / alert red
    "success":   "#00D084",
    "warning":   "#FFB547",
    "dark":      "#080B10",
    "card":      "#171E27",
    "hover":     "#1F2833",
    "chrome":    "#E5E8EC",
    "gunmetal":  "#8A929D",
}

COLORS_SEQ = [
    "#00AEEF", "#3BC8FF", "#7DE2FF",
    "#C7CDD4", "#8A929D", "#E5E8EC",
    "#00D084", "#FFB547", "#FF4D6D",
]
