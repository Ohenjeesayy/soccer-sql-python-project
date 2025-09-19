import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from pathlib import Path 
import re

# Define folders 
RAW_DIR = Path("data_raw")
CLEAN_DIR = Path("data_clean")
IMG_DIR = Path("images")
SQL_FILE = Path("sql/queries.sql")
OUT_DIR = Path("output")

for folder in (CLEAN_DIR, IMG_DIR, OUT_DIR):
    folder.mkdir(parents=True, exist_ok=True)

def season_from_name(path: Path) -> str:
    m = re.search(r"(\d{4})[_-]\d{4}", path.stem)
    return f"{m.group(1)} ={m.group(2)}" if m else "unknown"

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (df.columns.str.lower().str.strip().str.replace(r"\s+","_", regex=True))
    return df.rename(columns={"hometeam": "home_team", "awayteam": "away_team" })

# def load_and_clean(df: pd.DataFrame) -> pd.DataFrame: