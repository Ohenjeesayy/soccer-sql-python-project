import pandas as pd
import numpy as np
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

def load_and_clean() -> pd.DataFrame:
    files = sorted(RAW_DIR.glob("*.csv"))
    if not files:
        raise SystemExit("No csv files in data_raw")
    frames = []
    needed = ["date", "home_team", "away_team", "fthg", "ftag","ftr"]
    for f in files:
        df = pd.read_csv(f)
        df = normalize_columns(df)
        missing = [c for c in needed if c not in df.columns]
        if missing:
            raise SystemExit(f"{f} missing columns: {missing}")
        df = df[needed].copy()
        df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
        for c in ["home_team", "away_team", "ftr"]:
            df[c] = df[c].astype(str).str.strip()

        df = df.dropna(subset=["date", "home_team", "away_team", "ftag", "fthg"])
        df["ftag"] = df["ftag"].astype(int)
        df["fthg"] = df["fthg"].astype(int)

        df['total_goals'] = df["ftag"] + df["fthg"]
        df["is_draw"] = (df["ftr"].str.upper() == "D").astype(int) #this will return 1 if the result is draw, else 0
        df["season"] = season_from_name(f)

        frames.append(df)
    matches = pd.concat(frames, ignore_index= True).sort_values("date")
    (CLEAN_DIR / "matches_clean.csv").parent.mkdir(parents=True, exist_ok=True)
    matches.to_csv(CLEAN_DIR / "matches_clean.csv", index=False)
    return matches