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
DB_FILE = Path("soccer.db")

for folder in (CLEAN_DIR, IMG_DIR, OUT_DIR):
    folder.mkdir(parents=True, exist_ok=True)

def season_from_name(path: Path) -> str:
    # filename without extension, e.g. "2022-2023" or "E0_2022_2023"
    stem = path.stem.strip()
    # normalize any funky dashes to a hyphen
    stem = stem.replace("–", "-").replace("—", "-")
    # grab ALL 4-digit numbers in the name
    years = re.findall(r"\d{4}", stem)
    # if we found at least two, use the first two
    if len(years) >= 2:
        return f"{years[0]}-{years[1]}"
    return "unknown"

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


def load_to_sqlite(df: pd.DataFrame, db_path: Path = DB_FILE, table: str = "matches")->None:
    if db_path.exists():
        db_path.unlink()
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table, conn, if_exists="replace", index = False)
    print(f"[OK] SQLite -> {db_path} (table: {table})")


def run_queries(sql_path: Path = SQL_FILE, db_path: Path = DB_FILE) -> None:
    """Execute semicolon-separated queries in sql/queries.sql, save outputs/q*.csv."""
    if not sql_path.exists():
        print("[WARNING] sql/queries.sql not found; skipping SQL.")
        return
    text = sql_path.read_text(encoding="utf-8")
    stmts = [s.strip() for s in text.split(";") if s.strip()]
    with sqlite3.connect(db_path) as conn:
        for i, stmt in enumerate(stmts, 1):
            try:
                df = pd.read_sql(stmt, conn)
                out = OUT_DIR / f"q{i}.csv"
                df.to_csv(out, index=False)
                print(f"[OK] Query {i} -> {out}")
            except Exception as e:
                print(f"[WARN] Query {i} failed: {e}")

def make_charts(df: pd.DataFrame) -> None:
    """Produce two charts and save to images/."""
    # Avg goals by month (line)
    temp = df.copy()
    temp["month"] = temp["date"].dt.to_period("M").dt.to_timestamp()
    by_month = temp.groupby("month")["total_goals"].mean().reset_index()
    plt.figure()
    plt.plot(by_month["month"], by_month["total_goals"])
    plt.title("Average Goals per Match by Month")
    plt.xlabel("Month"); plt.ylabel("Avg Goals"); plt.tight_layout()
    plt.savefig(IMG_DIR / "avg_goals_by_month.png", dpi=200)

    # Top-10 teams by aggregate goals (bar)
    tg = (df.groupby("home_team")["fthg"].sum().rename("gf_home").to_frame()
            .join(df.groupby("away_team")["ftag"].sum().rename("gf_away"), how="outer").fillna(0))
    tg["total"] = tg["gf_home"] + tg["gf_away"]
    top10 = tg.sort_values("total", ascending=False).head(10)
    plt.figure()
    top10["total"].plot(kind="bar")
    plt.title("Top 10 Teams by Goals")
    plt.ylabel("Goals"); plt.tight_layout()
    plt.savefig(IMG_DIR / "top_teams_goals.png", dpi=200)
    print(f"[OK] Charts -> {IMG_DIR/'avg_goals_by_month.png'}, {IMG_DIR/'top_teams_goals.png'}")

def main() -> None:
    df = load_and_clean()
    load_to_sqlite(df)
    run_queries()
    make_charts(df)
    print("[DONE]")

if __name__ == "__main__":
    main()
