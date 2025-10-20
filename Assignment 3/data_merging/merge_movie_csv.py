import ast
import json
import math
import numpy as np
import pandas as pd
from collections import OrderedDict

# ---------- Helpers ----------
def safe_literal_eval(x, default=None):
    """
    Safely parse JSON-like strings stored in CSV cells (e.g., "[{'id': 1, 'name':'...'}]").
    Returns default (None or []) if parsing fails or input is NaN/empty.
    """
    if pd.isna(x) or x is None or (isinstance(x, str) and x.strip() == ""):
        return default
    if isinstance(x, (list, dict)):
        return x
    try:
        return ast.literal_eval(x)
    except Exception:
        return default

def coerce_int(series):
    """Coerce a pandas Series to integer with NA support."""
    return pd.to_numeric(series, errors="coerce").astype("Int64")

def rating_distribution(series):
    """
    Build a distribution dict for ratings (MovieLens uses 0.5 steps typically from 0.5 to 5.0).
    Keys are strings for JSON-friendliness.
    """
    if series is None or len(series) == 0:
        return {}
    counts = series.value_counts(dropna=True).sort_index()
    return {str(float(k)): int(v) for k, v in counts.items()}

def keep_fields(items, allowed_keys):
    """Trim each dict in a list to only allowed_keys."""
    if not isinstance(items, list):
        return []
    out = []
    for it in items:
        if isinstance(it, dict):
            out.append({k: it.get(k, None) for k in allowed_keys})
    return out

# ---------- Load data ----------
credits = pd.read_csv('../data/credits.csv')
keywords = pd.read_csv('../data/keywords.csv')
links = pd.read_csv('../data/links.csv')
movies_metadata = pd.read_csv('../data/movies_metadata.csv', low_memory=False)
ratings = pd.read_csv('../data/ratings.csv')

# ---------- Fix IDs and types ----------
# movies_metadata.id (TMDB id) sometimes comes as strings or malformed rows; coerce to Int
movies_metadata['id'] = coerce_int(movies_metadata['id'])

# links.tmdbId is float in many dumps; coerce to Int
links['tmdbId'] = coerce_int(links['tmdbId'])

# credits.id and keywords.id are TMDB ids; ensure Int
credits['id'] = coerce_int(credits['id'])
keywords['id'] = coerce_int(keywords['id'])

# ---------- Parse JSON-like string columns ----------
# Movies metadata fields to parse
for col, default in [
    ('genres', []),
    ('production_companies', []),
    ('spoken_languages', []),
    ('belongs_to_collection', None),
]:
    movies_metadata[col] = movies_metadata[col].apply(lambda x: safe_literal_eval(x, default=default))

# Credits table fields
credits['cast'] = credits['cast'].apply(lambda x: safe_literal_eval(x, default=[]))
credits['crew'] = credits['crew'].apply(lambda x: safe_literal_eval(x, default=[]))

# Keywords table
keywords['keywords'] = keywords['keywords'].apply(lambda x: safe_literal_eval(x, default=[]))

# ---------- Merge TMDB tables ----------
# Start from movies (one row per movie)
movies = movies_metadata.copy()

# Add credits (cast/crew)
movies = movies.merge(
    credits[['id', 'cast', 'crew']],
    how='left',
    left_on='id',
    right_on='id'
)

# Add keywords
movies = movies.merge(
    keywords[['id', 'keywords']],
    how='left',
    left_on='id',
    right_on='id'
)

# Add link to MovieLens movieId via tmdbId
movies = movies.merge(
    links[['movieId', 'tmdbId']],
    how='left',
    left_on='id',
    right_on='tmdbId'
)

# ---------- Build ratings aggregates (MovieLens) ----------
# Join ratings with links to get TMDB id per rating
ratings_with_tmdb = ratings.merge(
    links[['movieId', 'tmdbId']],
    how='left',
    on='movieId'
)

# Group by tmdbId (TMDB movie id)
grp = ratings_with_tmdb.dropna(subset=['tmdbId']).groupby('tmdbId')

ratings_agg = grp['rating'].agg(['count', 'mean', 'std', 'min', 'max']).reset_index()
ratings_agg = ratings_agg.rename(columns={
    'count': 'ml_count',
    'mean': 'ml_mean',
    'std': 'ml_std',
    'min': 'ml_min',
    'max': 'ml_max'
})

# Build rating distribution per tmdbId
dist_map = (
    ratings_with_tmdb.dropna(subset=['tmdbId'])
    .groupby('tmdbId')['rating']
    .apply(rating_distribution)
    .to_dict()
)

# Merge aggregates back into movies
movies = movies.merge(
    ratings_agg,
    how='left',
    left_on='id',
    right_on='tmdbId'
)

# ---------- Trim big nested lists to useful fields (optional) ----------
# Keep only relevant keys for compact JSON (adjust if you want everything)
GENRE_KEYS = ['id', 'name']
COMPANY_KEYS = ['id', 'name', 'origin_country']
LANG_KEYS = ['iso_639_1', 'name']
COLLECTION_KEYS = ['id', 'name', 'poster_path', 'backdrop_path']

CAST_KEYS = ['cast_id', 'character', 'credit_id', 'gender', 'id', 'name', 'order', 'profile_path']
CREW_KEYS = ['credit_id', 'department', 'gender', 'id', 'job', 'name', 'profile_path']
KEYWORD_KEYS = ['id', 'name']

movies['genres'] = movies['genres'].apply(lambda lst: keep_fields(lst, GENRE_KEYS))
movies['production_companies'] = movies['production_companies'].apply(lambda lst: keep_fields(lst, COMPANY_KEYS))
movies['spoken_languages'] = movies['spoken_languages'].apply(lambda lst: keep_fields(lst, LANG_KEYS))
movies['belongs_to_collection'] = movies['belongs_to_collection'].apply(
    lambda d: {k: d.get(k, None) for k in COLLECTION_KEYS} if isinstance(d, dict) else None
)

movies['cast'] = movies['cast'].apply(lambda lst: keep_fields(lst, CAST_KEYS))
movies['crew'] = movies['crew'].apply(lambda lst: keep_fields(lst, CREW_KEYS))
movies['keywords'] = movies['keywords'].apply(lambda lst: keep_fields(lst, KEYWORD_KEYS))

# ---------- Build final JSON structure per movie ----------
def build_movie_record(row):
    # MovieLens ratings distribution
    dist = dist_map.get(int(row['id'])) if not pd.isna(row['id']) and int(row['id']) in dist_map else {}

    # Ensure numeric fallbacks (convert pandas NA to None for JSON)
    def none_if_nan(x):
        if pd.isna(x):
            return None
        if isinstance(x, (np.floating, float)) and (math.isnan(x) if isinstance(x, float) else False):
            return None
        return x

    rec = OrderedDict()
    # Top-level (normalized names)
    rec['id'] = none_if_nan(row['id'])
    rec['title'] = none_if_nan(row.get('title'))
    rec['original_title'] = none_if_nan(row.get('original_title'))
    rec['overview'] = none_if_nan(row.get('overview'))
    rec['release_date'] = none_if_nan(row.get('release_date'))
    rec['runtime'] = none_if_nan(row.get('runtime'))
    rec['budget'] = none_if_nan(row.get('budget'))
    rec['revenue'] = none_if_nan(row.get('revenue'))
    rec['vote_average'] = none_if_nan(row.get('vote_average'))   # TMDB community rating
    rec['vote_count'] = none_if_nan(row.get('vote_count'))       # TMDB vote count
    rec['genres'] = row.get('genres', [])
    rec['production_companies'] = row.get('production_companies', [])
    rec['spoken_languages'] = row.get('spoken_languages', [])
    rec['belongs_to_collection'] = row.get('belongs_to_collection', None)

    # Nested: keywords, cast, crew
    rec['keywords'] = row.get('keywords', [])

    rec['cast'] = row.get('cast', [])
    rec['crew'] = row.get('crew', [])

    # Nested: ratings (MovieLens)
    rec['ratings'] = {
        'movielens': {
            'count': none_if_nan(row.get('ml_count')),
            'mean': none_if_nan(row.get('ml_mean')),
            'std': none_if_nan(row.get('ml_std')),
            'min': none_if_nan(row.get('ml_min')),
            'max': none_if_nan(row.get('ml_max')),
            'distribution': dist  # e.g., {"0.5": 12, "1.0": 34, ...}
        }
    }

    # Optional: add mapping IDs for traceability
    rec['_ids'] = {
        'tmdb_id': none_if_nan(row.get('id')),
        'imdb_id': none_if_nan(row.get('imdb_id')),
        'movielens_movieId': none_if_nan(row.get('movieId'))  # from links
    }

    return rec

records = [build_movie_record(row) for _, row in movies.iterrows()]

# ---------- Save JSON ----------
# As a single JSON array:
with open('movies_merged.json', 'w', encoding='utf-8') as f:
    json.dump(records, f, ensure_ascii=False, indent=2)

# If you prefer JSON Lines instead, use this:
# with open('movies_merged.jsonl', 'w', encoding='utf-8') as f:
#     for r in records:
#         f.write(json.dumps(r, ensure_ascii=False) + "\n")

print(f"Built {len(records)} movie records.")
print("Wrote movies_merged.json")
