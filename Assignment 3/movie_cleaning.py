import os
import ast
from typing import List, Optional

import numpy as np
import pandas as pd

# ---- Load raw CSV files ----
df_credits = pd.read_csv("data/credits.csv")
df_keywords = pd.read_csv("data/keywords.csv")
df_movies_metadata = pd.read_csv("data/movies_metadata.csv", low_memory=False)
df_links_merged = pd.read_csv("data/links_merged.csv")
df_ratings_merged = pd.read_csv("data/merged_ratings.csv")


# ------------------------------- Utilities -------------------------------- #

def _find_id_column(df: pd.DataFrame) -> Optional[str]:
    """Return a likely id column name (case-sensitive match first), else None."""
    for c in ("id", "movie_id", "movieId"):
        if c in df.columns:
            return c
    for c in df.columns:
        if str(c).lower().endswith("id"):
            return c
    return None


def _drop_empty_list_like(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Drop rows where column is NaN or '[]'."""
    if column not in df.columns:
        return df.copy()
    out = df.copy()
    out = out[~out[column].isna()]
    out = out[out[column].astype(str).str.strip() != "[]"]
    return out


def _drop_duplicates_by_id(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate rows by a likely id column, if one exists."""
    out = df.copy()
    id_col = _find_id_column(out)
    if id_col:
        out = out.drop_duplicates(subset=id_col)
    return out


def _parse_genre_names(cell: object) -> List[str]:
    """Parse TMDB-style genres JSON string into a list of names."""
    if pd.isna(cell):
        return []
    try:
        parsed = ast.literal_eval(cell) if isinstance(cell, str) else cell
        if isinstance(parsed, list):
            return [d.get("name") for d in parsed if isinstance(d, dict) and d.get("name")]
    except Exception:
        pass
    return []


# ------------------------------- Cleaners -------------------------------- #

def clean_credits(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with empty crews and duplicates."""
    out = _drop_empty_list_like(df, "crew")
    out = _drop_duplicates_by_id(out)
    return out.reset_index(drop=True)


def clean_keywords(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with empty keywords and duplicates."""
    out = _drop_empty_list_like(df, "keywords")
    out = _drop_duplicates_by_id(out)
    return out.reset_index(drop=True)


def clean_movies_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Clean movies metadata and impute runtime by genre."""
    out = _drop_duplicates_by_id(df)
    if "status" in out.columns:
        out = out[out["status"] == "Released"]

    if "runtime" not in out.columns:
        return out.reset_index(drop=True)

    out["runtime"] = pd.to_numeric(out["runtime"], errors="coerce")
    out.loc[out["runtime"] == 0, "runtime"] = np.nan
    out["genre_list"] = out.get("genres", pd.Series([[]] * len(out))).apply(_parse_genre_names)

    overall_median = out["runtime"].median(skipna=True)
    exploded = out.explode("genre_list")
    genre_medians = exploded.groupby("genre_list")["runtime"].median().to_dict()

    def _impute(row: pd.Series) -> float:
        if pd.notna(row["runtime"]):
            return row["runtime"]
        gens = row["genre_list"] or []
        vals = [genre_medians.get(g) for g in gens if genre_medians.get(g) is not None]
        if vals:
            return float(np.nanmedian(vals))
        return float(overall_median) if pd.notna(overall_median) else np.nan

    out["runtime"] = out.apply(_impute, axis=1)
    return out.drop(columns=["genre_list"]).reset_index(drop=True)


# ------------------------------- Save cleaned versions -------------------------------- #

def clean_links_merged(df: pd.DataFrame) -> pd.DataFrame:
    """Clean links merged dataset."""
    return df

def clean_ratings_merged(df: pd.DataFrame) -> pd.DataFrame:
    """Clean ratings merged dataset."""
    return df

# Create folder if it doesn’t exist
os.makedirs("cleaned_data", exist_ok=True)

# Clean and save each relevant dataset
clean_datasets = {
    "credits": clean_credits(df_credits),
    "keywords": clean_keywords(df_keywords),
    "movies_metadata": clean_movies_metadata(df_movies_metadata),
    "links_merged": clean_links_merged(df_links_merged),
    "ratings_merged": clean_ratings_merged(df_ratings_merged),
}

# Save cleaned DataFrames
for name, df_cleaned in clean_datasets.items():
    output_path = f"cleaned_data/{name}_cleaned.csv"
    df_cleaned.to_csv(output_path, index=False)
    print(f"Saved cleaned {name} → {output_path}")

# Copy untouched datasets
for name, df_raw in {
    "links_merged": df_links_merged,
    "ratings_merged": df_ratings_merged,
}.items():
    output_path = f"cleaned_data/{name}.csv"
    df_raw.to_csv(output_path, index=False)
    print(f"Copied raw {name} → {output_path}")
