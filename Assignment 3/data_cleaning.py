from ast import literal_eval
import pandas as pd
from pathlib import Path


def parse_collection(x):
    if isinstance(x, str):
        try:
            return literal_eval(x)
        except Exception:
            return None
    return None


def drop_duplicate_ids(df):
    """
    Cleans a DataFrame by ensuring the 'id' column is numeric
    and dropping duplicate IDs (keeping the first occurrence).
    """

    # Column id - drop duplicates
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df.dropna(subset=["id"], inplace=True)
    df["id"] = df["id"].astype(int)

    before = len(df)
    df = df.drop_duplicates(subset="id", keep="first")
    after = len(df)

    print(f"\tRemoved {before - after} duplicate IDs. Remaining rows: {after}")

    return df


def drop_duplicate_imdb(df):
    """
    Cleans a DataFrame by ensuring the 'imdb_id' column is string-based
    and dropping duplicate IMDb IDs (keeping the first occurrence).
    """

    # Column imdb_id - drop duplicates
    if "imdb_id" not in df.columns:
        print("\tColumn 'imdb_id' not found in DataFrame.")
        return df

    # Ensure all IMDb IDs are strings
    df["imdb_id"] = df["imdb_id"].astype(str)

    # Drop empty or invalid IMDb IDs
    df["imdb_id"].replace(["", "nan", "None", "NaN"], pd.NA, inplace=True)
    df.dropna(subset=["imdb_id"], inplace=True)

    before = len(df)
    df = df.drop_duplicates(subset="imdb_id", keep="first")
    after = len(df)

    print(f"\tRemoved {before - after} duplicate IMDb IDs. Remaining rows: {after}")

    return df



def clean_credits(df):

    # Column id - drop duplicates
    df = drop_duplicate_ids(df)

    # Column cast
    # leave as it is

    # Column crew
    # leave as it is
    return df


def clean_keywords(df):

    # Column id - drop duplicates
    df = drop_duplicate_ids(df)

    # Column keywords
    # leave as it is
    return df


def clean_movies_metadata(df):

    # Column adult - keep only rows where 'adult' is exactly 'True' or 'False'
    valid_mask = df["adult"].isin(["True", "False"])
    before = len(df)
    df = df[valid_mask].copy()
    after = len(df)
    df["adult"] = df["adult"].map({"True": True, "False": False})
    print(f"\tRemoved {before - after} invalid 'adult' entries. Remaining rows: {after}")

    # Column belongs_to_collection - keep NaN, and remove duplicated sub ids
    df["belongs_to_collection"] = df["belongs_to_collection"].apply(parse_collection)
    df["collection_id"] = df["belongs_to_collection"].apply(
        lambda x: x["id"] if isinstance(x, dict) and "id" in x else None
    )
    before = len(df)
    df = df.drop_duplicates(subset="collection_id", keep="first")
    after = len(df)
    print(f"\tRemoved {before - after} duplicate collection IDs. Remaining rows: {after}")

    # Column budget
    df['budget'] = pd.to_numeric(df['budget'], errors='coerce')
    df = df.dropna(subset=['budget'])

    # Column genres
    # leave as it is

    # Column homepage
    df['homepage'] = df['homepage'].astype('string')
    df['homepage'] = df['homepage'].fillna('')

    # Column id - is str, must convert
    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df = df.dropna(subset=['id'])
    df = drop_duplicate_ids(df)

    # Column imdb_id
    df = df[df['imdb_id'].apply(lambda x: isinstance(x, str))]
    df = df.reset_index(drop=True)
    df = drop_duplicate_imdb(df)

    # Column original_language
    df = df[df['original_language'].apply(lambda x: isinstance(x, str))]
    df = df.reset_index(drop=True)

    # Column original_title
    # leave as it is

    # Column overview
    df = df[df['overview'].apply(lambda x: isinstance(x, str))]
    df = df.reset_index(drop=True)

    # Column popularity
    df["popularity"] = pd.to_numeric(df["popularity"], errors="coerce")
    before = len(df)
    df.dropna(subset=["popularity"], inplace=True)
    after = len(df)
    print(f"\tRemoved {before - after} rows with missing 'popularity'. Remaining rows: {after}")

    # Column poster_path
    df = df[df['poster_path'].apply(lambda x: isinstance(x, str))]
    df = df.reset_index(drop=True)
    before = len(df)
    df.dropna(subset=["poster_path"], inplace=True)
    after = len(df)
    print(f"\tRemoved {before - after} rows with missing 'poster_path'. Remaining rows: {after}")

    # Column production_companies
    # leave as it is

    # Column production_countries
    # leave as it is

    # Column release_data
    df = df[df['release_data'].apply(lambda x: isinstance(x, str))]
    df = df.reset_index(drop=True)

    # Column revenue
    before = len(df)
    df.dropna(subset=["revenue"], inplace=True)
    after = len(df)
    print(f"\tRemoved {before - after} rows with missing 'revenue'. Remaining rows: {after}")

    # Column runtime
    df["runtime"] = pd.to_numeric(df["runtime"], errors="coerce")

    before = len(df)
    df = df[df["runtime"] > 0].copy()  # removes 0 and NaN
    after = len(df)

    print(f"\tRemoved {before - after} rows with missing or zero 'runtime'. Remaining rows: {after}")

    # Column spoken_languages
    # leave as it is

    # Column status
    df = df[df['status'].apply(lambda x: isinstance(x, str))]
    df = df.reset_index(drop=True)
    before = len(df)
    df.dropna(subset=["status"], inplace=True)
    after = len(df)
    print(f"\tRemoved {before - after} rows with missing 'status'. Remaining rows: {after}")

    # Column tagline
    df['tagline'] = df['tagline'].astype('string')
    df['tagline'] = df['tagline'].fillna('')

    # Column title
    df = df[df['title'].apply(lambda x: isinstance(x, str))]
    df = df.reset_index(drop=True)
    before = len(df)
    df.dropna(subset=["title"], inplace=True)
    after = len(df)
    print(f"\tRemoved {before - after} rows with missing 'title'. Remaining rows: {after}")

    # Column video
    df = df[df['video'].apply(lambda x: isinstance(x, bool))]
    df = df.reset_index(drop=True)

    # Column vote_average
    before = len(df)
    df.dropna(subset=["title"], inplace=True)
    after = len(df)
    print(f"\tRemoved {before - after} rows with missing 'title'. Remaining rows: {after}")

    # Column vote_count
    before = len(df)
    df.dropna(subset=["title"], inplace=True)
    after = len(df)
    print(f"\tRemoved {before - after} rows with missing 'title'. Remaining rows: {after}")
    return df



def clean_data():
    """
    Loads all raw CSV files from the 'movies/' folder.
    Returns a dictionary of pandas DataFrames.
    """
    data_path = Path(__file__).resolve().parent / "movies"

    if not data_path.exists():
        raise FileNotFoundError(f"Data folder not found: {data_path}")

    print("=" * 60)
    print("Loading raw movie dataset files")
    print("=" * 60)

    files = {
        "movies_metadata": data_path / "movies_metadata.csv",
        "credits": data_path / "credits.csv",
        "keywords": data_path / "keywords.csv",
        "links": data_path / "links.csv",
        "links_small": data_path / "links_small.csv",
        "ratings": data_path / "ratings.csv",
        "ratings_small": data_path / "ratings_small.csv",
    }

    datasets = {}
    for name, path in files.items():
        if path.exists():
            print(f"Loading {path.name} ...")
            try:
                df = pd.read_csv(path, low_memory=False)
                datasets[name] = df
                print(f"  Loaded {len(df):,} rows and {len(df.columns)} columns")
            except Exception as e:
                print(f"  Error loading {path.name}: {e}")
        else:
            print(f"  File not found: {path.name}")

    print("\nSummary:")
    for name, df in datasets.items():
        print(f"  {name}: {len(df):,} rows")

    print("=" * 60)
    print("All available files loaded successfully.")
    print("=" * 60)

    print("=" * 60)
    print("Cleaning credits.csv")
    print("=" * 60)


    return datasets


if __name__ == "__main__":
    data = clean_data()
