from pathlib import Path
from .config import ROOT

def load_sql(rel_path: str) -> str:
    p = ROOT / "sql_scripts" / rel_path
    return p.read_text(encoding="utf-8")
