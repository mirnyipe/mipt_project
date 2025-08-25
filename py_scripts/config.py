import json, re
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]  # корень проекта
DATA_DIR = ROOT / "data"
ARCHIVE_DIR = ROOT / "archive"

RE_TRANS = re.compile(r"^transactions_(\d{2})(\d{2})(\d{4})\.txt$")
RE_PBL   = re.compile(r"^passport_blacklist_(\d{2})(\d{2})(\d{4})\.xlsx$")
RE_TERM  = re.compile(r"^terminals_(\d{2})(\d{2})(\d{4})\.xlsx$")

cred = json.load(open(ROOT / "cred.json", "r", encoding="utf-8"))
ENGINE: Engine = create_engine(
    f"postgresql://{cred['user']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['database']}",
    future=True
)
