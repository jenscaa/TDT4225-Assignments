import sys
import json
import ast
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from pymongo import UpdateOne, ASCENDING, DESCENDING, TEXT
from pymongo.errors import BulkWriteError

# Ensure we can import the provided DbConnector residing one level up
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from DbConnector import DbConnector  # noqa: E402


import logging, time
try:
    from tqdm import tqdm
except Exception:
    tqdm = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("import_pipeline")


def safe_eval(val: Any) -> Optional[Any]:
    if pd.isna(val):
        return None
    if isinstance(val, (dict, list)):
        return val
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            return ast.literal_eval(s)
        except Exception:
            try:
                return json.loads(s)
            except Exception:
                return None
    return None


def to_int(value: Any) -> Optional[int]:
    try:
        if pd.isna(value):
            return None
        # Some tmdbId values come as float; coerce safely
        i = int(float(value))
        return i
    except Exception:
        return None


def to_float(value: Any) -> Optional[float]:
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def to_date(val: Any) -> Optional[datetime]:
    if pd.isna(val):
        return None
    try:
        return pd.to_datetime(val, errors="coerce").to_pydatetime()
    except Exception:
        return None


def compute_year(date_val: Optional[datetime]) -> Optional[int]:
    return date_val.year if isinstance(date_val, datetime) else None


def compute_decade(year: Optional[int]) -> Optional[int]:
    return (year // 10) * 10 if isinstance(year, int) else None


def compact_dict(d: Optional[Dict], keep_keys: List[str]) -> Optional[Dict]:
    if not isinstance(d, dict):
        return None
    return {k: d.get(k) for k in keep_keys if k in d}


def compact_list_of_dicts(lst: Optional[List[Dict]], keep_keys: List[str]) -> Optional[List[Dict]]:
    if not isinstance(lst, list):
        return None
    out = []
    for item in lst:
        if isinstance(item, dict):
            out.append({k: item.get(k) for k in keep_keys if k in item})
    return out or None


def load_links_mapping(data_dir: Path):
    links_path = data_dir / "links_merged_cleaned.csv"
    links = pd.read_csv(links_path)
    # Build mapping between MovieLens movieId and TMDB id, and TMDB→IMDb
    movieId_to_tmdb = {}
    tmdb_to_imdb = {}
    for _, r in links.iterrows():
        ml_id = to_int(r.get("movieId"))
        tmdb = to_int(r.get("tmdbId"))
        imdb = to_int(r.get("imdbId"))
        if ml_id and tmdb:
            movieId_to_tmdb[ml_id] = tmdb
        if tmdb and imdb:
            tmdb_to_imdb[tmdb] = imdb
    return movieId_to_tmdb, tmdb_to_imdb


def load_keywords_map(data_dir: Path):
    kw_path = data_dir / "keywords_cleaned.csv"
    kw = pd.read_csv(kw_path)
    out = {}
    for _, r in kw.iterrows():
        tmdb = to_int(r.get("id"))
        keywords = compact_list_of_dicts(safe_eval(r.get("keywords")), ["id", "name"])
        if tmdb:
            out[tmdb] = keywords or []
    return out


def load_credits_maps(data_dir: Path):
    cred_path = data_dir / "credits_cleaned.csv"
    cred = pd.read_csv(cred_path)
    cast_map, crew_map = {}, {}
    for _, r in cred.iterrows():
        tmdb = to_int(r.get("id"))
        if not tmdb:
            continue
        cast_raw = safe_eval(r.get("cast")) or []
        crew_raw = safe_eval(r.get("crew")) or []
        cast = []
        for c in cast_raw:
            if isinstance(c, dict):
                cast.append({
                    "id": c.get("id"),
                    "name": c.get("name"),
                    "order": c.get("order"),
                    "gender": c.get("gender"),
                })
        crew = []
        for c in crew_raw:
            if isinstance(c, dict):
                crew.append({
                    "id": c.get("id"),
                    "name": c.get("name"),
                    "job": c.get("job"),
                    "gender": c.get("gender"),
                })
        cast_map[tmdb] = cast
        crew_map[tmdb] = crew
    return cast_map, crew_map


def build_movie_docs(data_dir: Path, tmdb_to_imdb: Dict[int, int], cast_map, crew_map, kw_map):
    meta_path = data_dir / "movies_metadata_cleaned.csv"
    meta = pd.read_csv(meta_path, low_memory=False)

    docs = []
    for _, r in meta.iterrows():
        tmdb = to_int(r.get("id"))
        if not tmdb:
            continue

        release_date = to_date(r.get("release_date"))
        year = compute_year(release_date)
        decade = compute_decade(year)

        belongs = compact_dict(safe_eval(r.get("belongs_to_collection")), ["id", "name"])
        genres = compact_list_of_dicts(safe_eval(r.get("genres")), ["id", "name"]) or []
        primary_genre = genres[0]["name"] if genres else None

        spoken_languages = compact_list_of_dicts(safe_eval(r.get("spoken_languages")), ["iso_639_1", "name"])
        prod_companies = compact_list_of_dicts(safe_eval(r.get("production_companies")), ["id", "name"])
        prod_countries = compact_list_of_dicts(safe_eval(r.get("production_countries")), ["iso_3166_1", "name"])

        vote_average = to_float(r.get("vote_average"))
        vote_count = to_int(r.get("vote_count"))
        revenue = to_float(r.get("revenue"))
        runtime = to_float(r.get("runtime"))

        doc = {
            "_id": tmdb,
            "title": r.get("title"),
            "original_title": r.get("original_title"),
            "original_language": r.get("original_language"),
            "overview": r.get("overview"),
            "tagline": r.get("tagline"),
            "homepage": r.get("homepage"),
            "imdb_id": r.get("imdb_id"),
            "release_date": release_date,
            "year": year,
            "decade": decade,
            "vote_average": vote_average,
            "vote_count": vote_count,
            "revenue": revenue,
            "runtime": runtime,
            "belongs_to_collection": belongs,
            "genres": genres,
            "primary_genre": primary_genre,
            "spoken_languages": spoken_languages,
            "production_companies": prod_companies,
            "production_countries": prod_countries,
            "cast": cast_map.get(tmdb, []),
            "crew": crew_map.get(tmdb, []),
            "keywords": kw_map.get(tmdb, []),
            # Resolved numeric IMDb id from links if available
            "imdbId": tmdb_to_imdb.get(tmdb),
        }
        docs.append(doc)
    return docs


def import_movies(db, movie_docs: List[Dict], *, drop_first: bool = True, batch_size: int = 1000):
    coll = db["movies"]
    if drop_first:
        coll.drop()

    ops, total = [], 0
    for doc in movie_docs:
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))
        if len(ops) >= batch_size:
            coll.bulk_write(ops, ordered=False)
            total += len(ops)
            ops = []
    if ops:
        coll.bulk_write(ops, ordered=False)
        total += len(ops)
    print(f"Upserted {total} movie documents.")


def load_ratings(data_dir: Path, movieId_to_tmdb: Dict[int, int]):
    ratings_path = data_dir / "ratings_merged_cleaned.csv"
    ratings = pd.read_csv(ratings_path)

    docs = []
    for _, r in ratings.iterrows():
        ml_id = to_int(r.get("movieId"))
        tmdb = movieId_to_tmdb.get(ml_id)
        if not tmdb:
            continue
        rating = to_float(r.get("rating"))
        ts = to_int(r.get("timestamp"))
        ts_dt = datetime.utcfromtimestamp(ts) if isinstance(ts, int) else None
        user_id = to_int(r.get("userId"))
        if user_id is None or rating is None:
            continue
        docs.append({
            "userId": user_id,
            "movie_tmdb": tmdb,
            "rating": rating,
            "timestamp": ts_dt,
        })
    return docs


def import_ratings(db, rating_docs: List[Dict], *, drop_first: bool = True, batch_size: int = 5000):
    coll = db["ratings"]
    if drop_first:
        coll.drop()

    ops, total = [], 0
    for doc in rating_docs:
        ops.append(UpdateOne(
            {"userId": doc["userId"], "movie_tmdb": doc["movie_tmdb"]},
            {"$set": doc},
            upsert=True
        ))
        if len(ops) >= batch_size:
            coll.bulk_write(ops, ordered=False)
            total += len(ops)
            ops = []
    if ops:
        coll.bulk_write(ops, ordered=False)
        total += len(ops)
    print(f"Upserted {total} rating documents.")


def create_indexes(db):
    movies = db["movies"]
    ratings = db["ratings"]

    # Text search for noir queries
    movies.create_index([("overview", TEXT), ("tagline", TEXT)], name="movies_text")

    # Director / crew queries
    movies.create_index([("crew.job", ASCENDING), ("crew.id", ASCENDING)], name="idx_crew_job_id")

    # Cast-based queries (co-stars, top-5, gender)
    movies.create_index([("cast.id", ASCENDING)], name="idx_cast_id")
    movies.create_index([("cast.order", ASCENDING)], name="idx_cast_order")
    movies.create_index([("cast.gender", ASCENDING)], name="idx_cast_gender")

    # Collections and revenue aggregations
    movies.create_index([("belongs_to_collection.name", ASCENDING)], name="idx_collection_name")
    movies.create_index([("revenue", DESCENDING)], name="idx_revenue_desc")

    # Genres and primary genre
    movies.create_index([("genres.name", ASCENDING)], name="idx_genres_name")
    movies.create_index([("primary_genre", ASCENDING)], name="idx_primary_genre")

    # Time-based aggregations
    movies.create_index([("year", ASCENDING)], name="idx_year")
    movies.create_index([("decade", ASCENDING)], name="idx_decade")
    movies.create_index([("release_date", ASCENDING)], name="idx_release_date")

    # Voting filters and sorts
    movies.create_index([("vote_count", DESCENDING), ("vote_average", DESCENDING)], name="idx_votes_combo")

    # Language / US production filters
    movies.create_index([("original_language", ASCENDING)], name="idx_original_language")
    movies.create_index([("production_companies.name", ASCENDING)], name="idx_prod_companies_name")
    movies.create_index([("production_countries.iso_3166_1", ASCENDING)], name="idx_prod_countries_code")

    # Ratings-side indexes
    ratings.create_index([("userId", ASCENDING)], name="idx_ratings_user")
    ratings.create_index([("movie_tmdb", ASCENDING)], name="idx_ratings_movie")
    ratings.create_index([("userId", ASCENDING), ("movie_tmdb", ASCENDING)], unique=True, name="idx_user_movie_unique")


def main():
    # Resolve paths
    base_dir = ROOT
    data_dir = base_dir / "cleaned_data"

    # Connect to MongoDB using the provided connector
    conn = DbConnector()
    db = conn.db

    try:
        # Load support maps
        movieId_to_tmdb, tmdb_to_imdb = load_links_mapping(data_dir)
        kw_map = load_keywords_map(data_dir)
        cast_map, crew_map = load_credits_maps(data_dir)

        # Build movie docs from metadata + credits + keywords + imdb mapping
        movie_docs = build_movie_docs(data_dir, tmdb_to_imdb, cast_map, crew_map, kw_map)
        import_movies(db, movie_docs, drop_first=True)

        # Build and import ratings with TMDB id mapping
        rating_docs = load_ratings(data_dir, movieId_to_tmdb)
        import_ratings(db, rating_docs, drop_first=True)

        # Create indexes for performance on the 10 tasks
        create_indexes(db)

        print("Import and indexing completed.")
    except BulkWriteError as bwe:
        print("Bulk write error:", bwe.details)
    except Exception as e:
        print("ERROR during import:", e)
    finally:
        conn.close_connection()


if __name__ == "__main__":
    main()