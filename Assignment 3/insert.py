# insert_movies_data.py
from pymongo import MongoClient
import pandas as pd
from ast import literal_eval
import time
from data_cleaning import clean_data

# CONFIG
DB_NAME = "movieDB"
BATCH_SIZE = 1000


def safe_json(x):
    """Try to parse JSON-like string to Python object"""
    try:
        return literal_eval(x) if pd.notnull(x) else None
    except:
        return None


def insert_in_batches(collection, records, batch_size=BATCH_SIZE):
    total = len(records)
    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        if batch:
            collection.insert_many(batch)
            print(f"  Inserted {i + len(batch):,}/{total:,}", end='\r', flush=True)
    print(f"  Inserted {total:,} documents total.")


def main():
    print("MongoDB Movie Dataset Loader")

    start_time = time.time()

    # Step 1: Clean all data first
    print("Cleaning and preparing datasets...")
    datasets = clean_data()
    print("Cleaning complete. Proceeding with insertion.\n")

    # Step 2: Connect to MongoDB
    print("Connecting to MongoDB at localhost:27017 ...")
    client = MongoClient("mongodb://localhost:27017")
    db = client[DB_NAME]
    print(f"Connected to database '{DB_NAME}'")

    # Drop old collections
    for col in ["movies", "ratings", "links"]:
        db[col].drop()
    print("Old collections dropped (if existed)")

    # Step 3: Merge movies + credits + keywords
    print("\nMerging datasets for movies...")
    movies = datasets["movies_metadata"]
    credits = datasets["credits"]
    keywords = datasets["keywords"]

    merged = (
        movies.merge(credits, on="id", how="left")
              .merge(keywords, on="id", how="left")
    )
    merged = merged.rename(columns={"id": "tmdbId"})
    print(f"  Merged dataset: {len(merged):,} rows")

    # Step 4: Convert JSON-like fields
    json_cols = [
        "genres", "production_companies", "production_countries",
        "spoken_languages", "belongs_to_collection", "cast", "crew", "keywords"
    ]
    for col in json_cols:
        if col in merged.columns:
            merged[col] = merged[col].apply(safe_json)

    # Step 5: Insert movies
    print("\nInserting movies into MongoDB...")
    insert_in_batches(db.movies, merged.to_dict("records"))
    print("Finished inserting movies.")

    # Step 6: Insert links
    print("\nInserting links...")
    insert_in_batches(db.links, datasets["links"].to_dict("records"))
    print("Finished inserting links.")

    # Step 7: Insert ratings
    print("\nInserting ratings (full dataset)...")
    ratings = datasets["ratings"]
    insert_in_batches(db.ratings, ratings.to_dict("records"))
    print("Finished inserting ratings.")

    # Step 8: Create indexes
    print("\nCreating indexes...")
    db.movies.create_index("tmdbId", unique=True)
    db.ratings.create_index("userId")
    db.ratings.create_index("movieId")
    db.ratings.create_index("tmdbId")
    db.links.create_index("movieId", unique=True)
    db.links.create_index("tmdbId")
    print("Indexes created.")

    # Step 9: Summary
    elapsed = time.time() - start_time
    print("\nDone.")
    print(f"Total time: {elapsed:.2f}s")
    print("Collections summary:")
    print(f"  movies:  {db.movies.count_documents({}):,}")
    print(f"  links:   {db.links.count_documents({}):,}")
    print(f"  ratings: {db.ratings.count_documents({}):,}")


if __name__ == "__main__":
    main()