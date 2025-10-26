# insert_movies_data.py
from pymongo import MongoClient
import pandas as pd
from ast import literal_eval
from pathlib import Path
import time

# ======== CONFIG ========
DATA_DIR = Path(__file__).resolve().parent / "movies"   # change this to your folder path
DB_NAME = "movieDB"
BATCH_SIZE = 1000
# =========================

def safe_json(x):
    """Try to parse JSON-like string to Python object"""
    try:
        return literal_eval(x) if pd.notnull(x) else None
    except:
        return None


def insert_in_batches(collection, records, batch_size=BATCH_SIZE):
    total = len(records)
    for i in range(0, total, batch_size):
        batch = records[i:i+batch_size]
        if batch:
            collection.insert_many(batch)
            print(f"  Inserted {i+len(batch):,}/{total:,}", end='\r', flush=True)
    print(f"  Inserted {total:,} documents total.")


def main():
    print("="*60)
    print("MongoDB Movie Dataset Loader")
    print("="*60)

    start_time = time.time()

    # ----- Connect to MongoDB -----
    print("Connecting to MongoDB at localhost:27017 ...")
    client = MongoClient("mongodb://localhost:27017")
    db = client[DB_NAME]
    print(f"Connected to database '{DB_NAME}'")

    # Clear old collections if re-running
    for col in ["movies", "ratings", "links"]:
        db[col].drop()
    print("Old collections dropped (if existed)")

    # ----- MOVIES -----
    print("\nLoading movies_metadata.csv ...")
    movies = pd.read_csv(DATA_DIR / "movies_metadata.csv", low_memory=False)
    print(f"  {len(movies):,} movies read")

    print("Loading credits.csv ...")
    credits = pd.read_csv(DATA_DIR / "credits.csv")
    print(f"  {len(credits):,} credits read")

    print("Loading keywords.csv ...")
    keywords = pd.read_csv(DATA_DIR / "keywords.csv")
    print(f"  {len(keywords):,} keywords read")

    # Fix ID types before merging
    for df_name, df in [("movies", movies), ("credits", credits), ("keywords", keywords)]:
        if "id" in df.columns:
            df["id"] = pd.to_numeric(df["id"], errors="coerce")

    movies = movies.dropna(subset=["id"])
    credits = credits.dropna(subset=["id"])
    keywords = keywords.dropna(subset=["id"])

    movies["id"] = movies["id"].astype(int)
    credits["id"] = credits["id"].astype(int)
    keywords["id"] = keywords["id"].astype(int)

    # Merge datasets
    print("\nMerging datasets ...")
    merged = (
        movies.merge(credits, on="id", how="left")
              .merge(keywords, on="id", how="left")
    )
    merged = merged.rename(columns={"id": "tmdbId"})
    print(f"  Merged dataset: {len(merged):,} rows")

    # Convert JSON-like fields
    json_cols = [
        "genres", "production_companies", "production_countries",
        "spoken_languages", "belongs_to_collection", "cast", "crew", "keywords"
    ]
    for col in json_cols:
        if col in merged.columns:
            merged[col] = merged[col].apply(safe_json)

    print("Inserting into MongoDB (movies)...")
    insert_in_batches(db.movies, merged.to_dict("records"))
    print("Finished inserting movies")

    # ----- LINKS -----
    print("\nLoading links.csv ...")
    links = pd.read_csv(DATA_DIR / "links.csv")
    print(f"  {len(links):,} links read")

    print("Inserting links into MongoDB ...")
    insert_in_batches(db.links, links.to_dict("records"))
    print("Finished inserting links")

    # ----- RATINGS -----
    print("\nLoading ratings.csv (or ratings_small.csv) ...")
    ratings_path = DATA_DIR / "ratings_small.csv"  # change to ratings.csv for full data
    chunksize = 5000
    total_inserted = 0

    for chunk in pd.read_csv(ratings_path, chunksize=chunksize):
        db.ratings.insert_many(chunk.to_dict("records"))
        total_inserted += len(chunk)
        print(f"  Inserted {total_inserted:,} ratings", end='\r', flush=True)

    print(f"\nFinished inserting {total_inserted:,} ratings")

    # ----- INDEXES -----
    print("\nCreating indexes ...")
    db.movies.create_index("tmdbId", unique=True)
    db.ratings.create_index("userId")
    db.ratings.create_index("movieId")
    db.ratings.create_index("tmdbId")
    db.links.create_index("movieId", unique=True)
    db.links.create_index("tmdbId")
    print("Indexes created")

    elapsed = time.time() - start_time
    print("\nDone.")
    print(f"Total time: {elapsed:.2f}s")
    print("Collections summary:")
    print(f"  movies:  {db.movies.count_documents({}):,}")
    print(f"  links:   {db.links.count_documents({}):,}")
    print(f"  ratings: {db.ratings.count_documents({}):,}")
    print("="*60)


if __name__ == "__main__":
    main()
