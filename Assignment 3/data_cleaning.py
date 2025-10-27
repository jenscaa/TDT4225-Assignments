import os
from ast import literal_eval
import pandas as pd


# HELPER FUNCTIONS
def parse_collection(x):
    if isinstance(x, str):
        try:
            return literal_eval(x)
        except Exception:
            return None
    return None


def convert_to_numeric(df, column, dropna=True, drop_duplicates=False, to_int=False, name=None):
    """Converts a column to numeric, optionally drops NaN and duplicates."""
    df[column] = pd.to_numeric(df[column], errors="coerce")
    before = len(df)
    if dropna:
        df.dropna(subset=[column], inplace=True)
    if to_int:
        df[column] = df[column].astype(int)
    if drop_duplicates:
        df = df.drop_duplicates(subset=column, keep="first")
    after = len(df)
    label = name or column
    print(f"\tRemoved {before - after} invalid or duplicate '{label}' entries. Remaining rows: {after}")
    return df


def ensure_string_column(df, column, fill_empty=False):
    """Ensures a column is string type, optionally fills NaN with empty string."""
    df[column] = df[column].astype("string")
    if fill_empty:
        df[column] = df[column].fillna("")
    return df


def drop_invalid_strings(df, column):
    """Removes rows where column is not a string."""
    df = df[df[column].apply(lambda x: isinstance(x, str))]
    df = df.reset_index(drop=True)
    return df


def drop_invalid_bool(df, column):
    """Removes rows where column is not a boolean."""
    df = df[df[column].apply(lambda x: isinstance(x, bool))]
    df = df.reset_index(drop=True)
    return df


def add_missing_rows(df, df_small, merge_key, label):
    """Adds rows from df_small that don't exist in df based on merge_key."""
    merged = df_small.merge(df[merge_key], on=merge_key, how="left", indicator=True)
    missing_rows = merged[merged["_merge"] == "left_only"].drop(columns="_merge")
    print(f"\tFound {len(missing_rows)} new {label} in df_small not present in df. Adding them.")
    df = pd.concat([df, missing_rows], ignore_index=True)
    print(f"\tFinal row count after merging: {len(df)}")
    return df


# CLEANING FUNCTIONS

def drop_duplicate_ids(df):
    """Ensures the 'id' column is numeric and removes duplicates."""
    df = convert_to_numeric(df, "id", dropna=True, to_int=True, drop_duplicates=True, name="ID")
    return df


def drop_duplicate_imdb(df):
    """Ensures 'imdb_id' is string-based and removes duplicates."""
    if "imdb_id" not in df.columns:
        print("\tColumn 'imdb_id' not found in DataFrame.")
        return df

    df["imdb_id"] = df["imdb_id"].astype(str)
    df["imdb_id"].replace(["", "nan", "None", "NaN"], pd.NA, inplace=True)
    df.dropna(subset=["imdb_id"], inplace=True)

    before = len(df)
    df = df.drop_duplicates(subset="imdb_id", keep="first")
    after = len(df)
    print(f"\tRemoved {before - after} duplicate IMDb IDs. Remaining rows: {after}")
    return df


def clean_credits(df):
    df = drop_duplicate_ids(df)
    # cast and crew untouched
    return df


def clean_keywords(df):
    df = drop_duplicate_ids(df)
    # keywords untouched
    return df


def clean_movies_metadata(df):
    # Column adult
    valid_mask = df["adult"].isin(["True", "False"])
    before = len(df)
    df = df[valid_mask].copy()
    after = len(df)
    df["adult"] = df["adult"].map({"True": True, "False": False})
    print(f"\tRemoved {before - after} invalid 'adult' entries. Remaining rows: {after}")

    # Column belongs_to_collection
    before = len(df)
    df = ensure_string_column(df, "belongs_to_collection", fill_empty=True)
    after = len(df)
    print(f"\tRemoved {before - after} duplicate collection IDs. Remaining rows: {after}")

    # Column budget
    df = convert_to_numeric(df, "budget", dropna=True)

    # Column homepage
    df = ensure_string_column(df, "homepage", fill_empty=True)

    # Column id
    df = convert_to_numeric(df, "id", dropna=True, to_int=True, drop_duplicates=True, name="ID")

    # Column imdbId
    df = drop_invalid_strings(df, "imdb_id")
    df = drop_duplicate_imdb(df)

    # Column original_language
    df = drop_invalid_strings(df, "original_language")

    # Column overview
    df = drop_invalid_strings(df, "overview")

    # Column popularity
    df = convert_to_numeric(df, "popularity", dropna=True)

    # Column poster_path
    df = drop_invalid_strings(df, "poster_path")
    df = convert_to_numeric(df, "poster_path", dropna=True) if False else df  # keep behavior consistent

    # Column release_date
    df = drop_invalid_strings(df, "release_date")

    # Column revenue
    before = len(df)
    df.dropna(subset=["revenue"], inplace=True)
    after = len(df)
    print(f"\tRemoved {before - after} rows with missing 'revenue'. Remaining rows: {after}")

    # Column runtime
    df = convert_to_numeric(df, "runtime", dropna=True)
    before = len(df)
    df = df[df["runtime"] > 0].copy()
    after = len(df)
    print(f"\tRemoved {before - after} rows with missing or zero 'runtime'. Remaining rows: {after}")

    # Column status
    df = drop_invalid_strings(df, "status")
    before = len(df)
    df.dropna(subset=["status"], inplace=True)
    after = len(df)
    print(f"\tRemoved {before - after} rows with missing 'status'. Remaining rows: {after}")

    # Column tagline
    df = ensure_string_column(df, "tagline", fill_empty=True)

    # Column title
    df = drop_invalid_strings(df, "title")
    before = len(df)
    df.dropna(subset=["title"], inplace=True)
    after = len(df)
    print(f"\tRemoved {before - after} rows with missing 'title'. Remaining rows: {after}")

    # Column video
    df = drop_invalid_bool(df, "video")

    # vote_average and vote_count
    for col in ["vote_average", "vote_count"]:
        before = len(df)
        df.dropna(subset=[col], inplace=True)
        after = len(df)
        print(f"\tRemoved {before - after} rows with missing 'title'. Remaining rows: {after}")

    return df


def clean_links(df, df_small):
    for col in ["movieId", "imdbId", "tmdbId"]:
        df = convert_to_numeric(df, col, dropna=True, to_int=True, drop_duplicates=True)
        df_small = convert_to_numeric(df_small, col, dropna=True, to_int=True, drop_duplicates=True, name=f"{col} in df_small")

    df = add_missing_rows(df, df_small, ["movieId"], "rows in df_small")
    return df


def clean_ratings(df, df_small):
    # Drop missing values
    before = len(df)
    df.dropna(subset=["userId", "movieId", "rating", "timestamp"], inplace=True)
    after = len(df)
    print(f"\tRemoved {before - after} rows with missing values from df. Remaining rows: {after}")

    before_small = len(df_small)
    df_small.dropna(subset=["userId", "movieId", "rating", "timestamp"], inplace=True)
    after_small = len(df_small)
    print(f"\tRemoved {before_small - after_small} rows with missing values from df_small. Remaining rows: {after_small}")

    df = add_missing_rows(df, df_small, ["userId", "movieId"], "ratings")
    return df


def clean_data():
    """
    Loads all raw CSV files from the 'movies/' folder.
    Returns a dictionary of pandas DataFrames.
    """
    data_path = "movies"

    print("Loading raw movie dataset files")

    files = {
        "movies_metadata": f"{data_path}/movies_metadata.csv",
        "credits": f"{data_path}/credits.csv",
        "keywords": f"{data_path}/keywords.csv",
        "links": f"{data_path}/links.csv",
        "links_small": f"{data_path}/links_small.csv",
        "ratings": f"{data_path}/ratings.csv",
        "ratings_small": f"{data_path}/ratings_small.csv",
    }

    datasets = {}
    for name, path in files.items():
        if os.path.exists(path):
            print(f"Loading {os.path.basename(path)} ...")
            try:
                df = pd.read_csv(path, low_memory=False)
                datasets[name] = df
                print(f"  Loaded {len(df):,} rows and {len(df.columns)} columns")
            except Exception as e:
                print(f"  Error loading {os.path.basename(path)}: {e}")
        else:
            print(f"  File not found: {os.path.basename(path)}")

    print("\nSummary:")
    for name, df in datasets.items():
        print(f"  {name}: {len(df):,} rows")

    print("All available files loaded successfully.")

    # CLEANING

    print("Cleaning credits.csv")
    datasets["credits"] = clean_credits(datasets["credits"])

    print("Cleaning keywords.csv")
    datasets["keywords"] = clean_keywords(datasets["keywords"])

    print("Cleaning movies_metadata.csv")
    datasets["movies_metadata"] = clean_movies_metadata(datasets["movies_metadata"])

    print("Cleaning links.csv (merging with links_small.csv)")
    datasets["links"] = clean_links(datasets["links"], datasets["links_small"])

    print("Cleaning ratings.csv (merging with ratings_small.csv)")
    datasets["ratings"] = clean_ratings(datasets["ratings"], datasets["ratings_small"])

    print("\nAll cleaning operations completed successfully.")

    return datasets


if __name__ == "__main__":
    data = clean_data()