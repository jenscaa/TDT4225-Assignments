from typing import List, Dict, Any, Tuple
from statistics import median
from pprint import pprint
import time
import json
from pathlib import Path

from DbConnector import DbConnector
from pymongo import ASCENDING, DESCENDING


def _safe_median(values: List[float]) -> float:
    vals = [v for v in values if isinstance(v, (int, float))]
    if not vals:
        return None
    return float(median(vals))


def _json_default(o):
    try:
        import datetime
        if isinstance(o, (datetime.datetime, datetime.date)):
            return o.isoformat()
    except Exception:
        pass
    return str(o)

def _save_json(rel_name: str, data):
    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / rel_name
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=_json_default)
    return str(out_path)

def _time_and_save(label: str, func, filename: str):
    t0 = time.perf_counter()
    res = func()
    dur = time.perf_counter() - t0
    payload = {
        "meta": {"label": label, "duration_sec": round(dur, 3), "count": (len(res) if hasattr(res, "__len__") else None)},
        "data": res,
    }
    path = _save_json(filename, payload)
    print(f"{label} -> {path} ({round(dur, 2)}s, {len(res) if hasattr(res,'__len__') else '?'} items)")


class Repository:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.movies = self.db["movies"]
        self.ratings = self.db["ratings"]

    def close(self):
        self.connection.close_connection()

    # 1) Top 10 directors (>= 5 movies) by median revenue; include movie_count, mean vote_average
    def directors_by_median_revenue_top10(self) -> List[Dict[str, Any]]:
        pipeline = [
            {"$match": {"crew.job": "Director"}},
            {"$project": {
                "revenue": 1,
                "vote_average": 1,
                "directors": {"$filter": {
                    "input": "$crew", "as": "c", "cond": {"$eq": ["$$c.job", "Director"]}
                }}
            }},
            {"$unwind": "$directors"},
            {"$group": {
                "_id": {"id": "$directors.id", "name": "$directors.name"},
                "movie_count": {"$sum": 1},
                "revenues": {"$push": "$revenue"},
                "mean_vote_average": {"$avg": "$vote_average"},
            }},
        ]
        rows = list(self.movies.aggregate(pipeline, allowDiskUse=True))
        out = []
        for r in rows:
            med = _safe_median([v for v in r.get("revenues", []) if v is not None])
            if r["movie_count"] >= 5 and med is not None:
                out.append({
                    "director_id": r["_id"]["id"],
                    "director": r["_id"]["name"],
                    "movie_count": r["movie_count"],
                    "median_revenue": med,
                    "mean_vote_average": r.get("mean_vote_average"),
                })
        out.sort(key=lambda x: (x["median_revenue"], x["movie_count"]), reverse=True)
        return out[:10]

    # 2) Actor pairs co-starred in >= 3 movies; include co-appearances and avg movie vote_average
    def actor_pairs_costars(self) -> List[Dict[str, Any]]:
        pipeline = [
            {"$project": {"cast": {"$ifNull": ["$cast", []]}, "vote_average": 1}},
            {"$match": {"cast.1": {"$exists": True}}},  # at least two cast
            {"$set": {"castArr": "$cast"}},
            {"$unwind": "$castArr"},
            {"$set": {"c1": "$castArr"}},
            {"$unwind": "$cast"},
            {"$match": {"$expr": {"$lt": ["$c1.id", "$cast.id"]}}},
            {"$group": {
                "_id": {
                    "a_id": "$c1.id", "a_name": "$c1.name",
                    "b_id": "$cast.id", "b_name": "$cast.name"
                },
                "co_appearances": {"$sum": 1},
                "avg_vote_average": {"$avg": "$vote_average"},
            }},
            {"$match": {"co_appearances": {"$gte": 3}}},
            {"$sort": {"co_appearances": -1, "avg_vote_average": -1}},
        ]
        rows = list(self.movies.aggregate(pipeline, allowDiskUse=True))
        return [{
            "actor_a_id": r["_id"]["a_id"],
            "actor_a": r["_id"]["a_name"],
            "actor_b_id": r["_id"]["b_id"],
            "actor_b": r["_id"]["b_name"],
            "co_appearances": r["co_appearances"],
            "avg_vote_average": r.get("avg_vote_average"),
        } for r in rows]

    # 3) Top 10 actors (>= 10 movies) with widest genre breadth
    def actors_genre_breadth_top10(self) -> List[Dict[str, Any]]:
        pipeline = [
            {"$unwind": "$cast"},
            {"$unwind": {"path": "$genres", "preserveNullAndEmptyArrays": False}},
            {"$group": {
                "_id": {"id": "$cast.id", "name": "$cast.name"},
                "genres_set": {"$addToSet": "$genres.name"},
                "movies": {"$addToSet": "$_id"},
            }},
            {"$project": {
                "_id": 0,
                "actor_id": "$_id.id",
                "actor": "$_id.name",
                "genre_count": {"$size": "$genres_set"},
                "movie_count": {"$size": "$movies"},
                "example_genres": {"$slice": ["$genres_set", 5]},
            }},
            {"$match": {"movie_count": {"$gte": 10}}},
            {"$sort": {"genre_count": -1, "movie_count": -1, "actor": 1}},
            {"$limit": 10},
        ]
        return list(self.movies.aggregate(pipeline, allowDiskUse=True))

    # 4) Top 10 collections by total revenue (>= 3 movies); include count, total revenue, median vote_average, earliest->latest
    def top_collections_by_total_revenue_top10(self) -> List[Dict[str, Any]]:
        pipeline = [
            {"$match": {"belongs_to_collection.name": {"$ne": None}}},
            {"$group": {
                "_id": "$belongs_to_collection.name",
                "movie_count": {"$sum": 1},
                "total_revenue": {"$sum": {"$ifNull": ["$revenue", 0]}},
                "votes": {"$push": "$vote_average"},
                "earliest": {"$min": "$release_date"},
                "latest": {"$max": "$release_date"},
            }},
            {"$match": {"movie_count": {"$gte": 3}}},
            {"$sort": {"total_revenue": -1}},
            {"$limit": 10},
        ]
        rows = list(self.movies.aggregate(pipeline, allowDiskUse=True))
        out = []
        for r in rows:
            out.append({
                "collection": r["_id"],
                "movie_count": r["movie_count"],
                "total_revenue": r["total_revenue"],
                "median_vote_average": _safe_median([v for v in r.get("votes", []) if v is not None]),
                "earliest": r.get("earliest"),
                "latest": r.get("latest"),
            })
        return out

    # 5) By decade and primary genre: median runtime and movie count; sort by decade then median runtime desc
    def decade_primary_genre_median_runtime(self) -> List[Dict[str, Any]]:
        pipeline = [
            {"$match": {"primary_genre": {"$ne": None}, "decade": {"$ne": None}}},
            {"$group": {
                "_id": {"decade": "$decade", "primary_genre": "$primary_genre"},
                "movie_count": {"$sum": 1},
                "runtimes": {"$push": "$runtime"},
            }},
        ]
        rows = list(self.movies.aggregate(pipeline, allowDiskUse=True))
        out = []
        for r in rows:
            med_rt = _safe_median([v for v in r.get("runtimes", []) if v is not None])
            out.append({
                "decade": r["_id"]["decade"],
                "primary_genre": r["_id"]["primary_genre"],
                "median_runtime": med_rt,
                "movie_count": r["movie_count"],
            })
        out.sort(key=lambda x: (x["decade"], -(x["median_runtime"] if x["median_runtime"] is not None else -1)))
        return out

    # 6) Proportion of female among top-billed 5 cast per movie; aggregate by decade
    def female_top5_proportion_by_decade(self) -> List[Dict[str, Any]]:
        pipeline = [
            {"$project": {
                "decade": 1,
                "top": {"$filter": {
                    "input": {"$ifNull": ["$cast", []]},
                    "as": "c",
                    "cond": {"$and": [
                        {"$lte": ["$$c.order", 4]},
                        {"$in": ["$$c.gender", [1, 2]]}
                    ]}
                }}
            }},
            {"$project": {
                "decade": 1,
                "female_count": {"$size": {"$filter": {
                    "input": "$top", "as": "c", "cond": {"$eq": ["$$c.gender", 1]}
                }}},
                "total": {"$size": "$top"}
            }},
            {"$project": {
                "decade": 1,
                "female_prop": {"$cond": [{"$gt": ["$total", 0]}, {"$divide": ["$female_count", "$total"]}, None]}
            }},
            {"$match": {"female_prop": {"$ne": None}, "decade": {"$ne": None}}},
            {"$group": {
                "_id": "$decade",
                "avg_female_prop": {"$avg": "$female_prop"},
                "movie_count": {"$sum": 1}
            }},
            {"$sort": {"avg_female_prop": -1}},
        ]
        rows = list(self.movies.aggregate(pipeline, allowDiskUse=True))
        return [{"decade": r["_id"], "avg_female_prop": r["avg_female_prop"], "movie_count": r["movie_count"]} for r in rows]

    # 7) Text search for "noir" or "neo-noir" with vote_count >= 50; top 20 by vote_average
    def top_noir_movies(self) -> List[Dict[str, Any]]:
        # Ensure these indexes exist once (run in your index setup):
        # self.db["movies"].create_index([("keywords.name", 1)], name="idx_keywords_name")

        pipeline = [
            # Branch A: text search (must be first)
            {"$match": {"$text": {"$search": 'noir "neo noir" neo-noir'}}},
            {"$match": {"vote_count": {"$gte": 50}}},
            {"$project": {
                "_id": 1, "title": 1, "year": 1, "vote_average": 1, "vote_count": 1,
                "source": {"$literal": "text"}
            }},
            # Merge with Branch B: keywords regex
            {"$unionWith": {
                "coll": "movies",
                "pipeline": [
                    {"$match": {"vote_count": {"$gte": 50}}},
                    {"$match": {"$or": [
                        {"keywords.name": {"$regex": r"(?i)\bneo[- ]?noir\b"}},
                        {"keywords.name": {"$regex": r"(?i)\bnoir\b"}},
                    ]}},
                    {"$project": {
                        "_id": 1, "title": 1, "year": 1, "vote_average": 1, "vote_count": 1,
                        "source": {"$literal": "keywords"}
                    }},
                ]
            }},
            # Deduplicate by movie
            {"$group": {"_id": "$_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$sort": {"vote_average": -1, "vote_count": -1}},
            {"$limit": 20},
            {"$project": {"_id": 0, "title": 1, "year": 1, "vote_average": 1, "vote_count": 1}}
        ]
        return list(self.movies.aggregate(pipeline, allowDiskUse=True))

    # 8) Top 20 director–actor pairs (>= 3 collaborations; movies vote_count >= 100) by mean vote_average; include films count and mean revenue
    def top_director_actor_pairs(self) -> List[Dict[str, Any]]:
        pipeline = [
            {"$match": {"vote_count": {"$gte": 100}}},
            {"$project": {
                "vote_average": 1,
                "revenue": 1,
                "directors": {"$filter": {"input": "$crew", "as": "c", "cond": {"$eq": ["$$c.job", "Director"]}}},
                "cast": {"$ifNull": ["$cast", []]},
            }},
            {"$unwind": "$directors"},
            {"$unwind": "$cast"},
            {"$group": {
                "_id": {
                    "d_id": "$directors.id", "d_name": "$directors.name",
                    "a_id": "$cast.id", "a_name": "$cast.name"
                },
                "films": {"$sum": 1},
                "mean_vote_average": {"$avg": "$vote_average"},
                "mean_revenue": {"$avg": "$revenue"},
            }},
            {"$match": {"films": {"$gte": 3}}},
            {"$sort": {"mean_vote_average": -1, "films": -1}},
            {"$limit": 20},
        ]
        rows = list(self.movies.aggregate(pipeline, allowDiskUse=True))
        return [{
            "director_id": r["_id"]["d_id"],
            "director": r["_id"]["d_name"],
            "actor_id": r["_id"]["a_id"],
            "actor": r["_id"]["a_name"],
            "films": r["films"],
            "mean_vote_average": r.get("mean_vote_average"),
            "mean_revenue": r.get("mean_revenue"),
        } for r in rows]

    # 9) Non-English originals with US involvement (production_countries) – top 10 original languages by count, include an example title
    def top10_original_languages_in_us_involved_non_english(self) -> List[Dict[str, Any]]:
        pipeline = [
            {"$match": {
                "original_language": {"$ne": "en"},
                "$or": [
                    {"production_countries.iso_3166_1": "US"},
                    {"production_countries.name": "United States of America"}
                ]
            }},
            {"$group": {"_id": "$original_language", "count": {"$sum": 1}, "example": {"$first": "$title"}}},
            {"$sort": {"count": -1}},
            {"$limit": 10},
        ]
        rows = list(self.movies.aggregate(pipeline, allowDiskUse=True))
        return [{"original_language": r["_id"], "count": r["count"], "example_title": r["example"]} for r in rows]

    # 10) User stats: ratings count, population variance of ratings, distinct genres rated
    # Returns two lists: top 10 by genre diversity; top 10 by variance (users with >= 20 ratings)
    def user_stats_toplists(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        pipeline = [
            {"$group": {
                "_id": "$userId",
                "ratings_count": {"$sum": 1},
                "mean_rating": {"$avg": "$rating"},
                "avg_sq_rating": {"$avg": {"$multiply": ["$rating", "$rating"]}},
                "movie_ids": {"$addToSet": "$movie_tmdb"},
            }},
            {"$addFields": {
                "var_pop": {"$subtract": ["$avg_sq_rating", {"$multiply": ["$mean_rating", "$mean_rating"]}]}
            }},
            {"$lookup": {
                "from": "movies",
                "let": {"mids": "$movie_ids"},
                "pipeline": [
                    {"$match": {"$expr": {"$in": ["$_id", "$$mids"]}}},
                    {"$project": {"genres": 1}}
                ],
                "as": "movies_joined"
            }},
            {"$project": {
                "ratings_count": 1,
                "var_pop": 1,
                "genres_all": {
                    "$reduce": {
                        "input": {
                            "$map": {
                                "input": "$movies_joined",
                                "as": "m",
                                "in": {"$map": {"input": {"$ifNull": ["$$m.genres", []]}, "as": "g", "in": "$$g.name"}}
                            }
                        },
                        "initialValue": [],
                        "in": {"$setUnion": ["$$value", "$$this"]}
                    }
                }
            }},
            {"$addFields": {"distinct_genres": {"$size": "$genres_all"}}},
        ]
        stats = list(self.ratings.aggregate(pipeline, allowDiskUse=True))

        genre_diverse = [s for s in stats if s["ratings_count"] >= 20]
        genre_diverse.sort(key=lambda x: (x["distinct_genres"], x["ratings_count"]), reverse=True)
        genre_diverse_top10 = [{"userId": r["_id"], "ratings_count": r["ratings_count"],
                                "distinct_genres": r["distinct_genres"], "var_pop": r["var_pop"]}
                               for r in genre_diverse[:10]]

        high_variance = [s for s in stats if s["ratings_count"] >= 20]
        high_variance.sort(key=lambda x: (x["var_pop"] if x["var_pop"] is not None else -1.0), reverse=True)
        high_variance_top10 = [{"userId": r["_id"], "ratings_count": r["ratings_count"],
                                "distinct_genres": r["distinct_genres"], "var_pop": r["var_pop"]}
                               for r in high_variance[:10]]

        return genre_diverse_top10, high_variance_top10


if __name__ == "__main__":
    repo = Repository()
    try:
        # _time_and_save("Q1 Directors by median revenue (top 10)", repo.directors_by_median_revenue_top10, "q1_directors_by_median_revenue.json")
        # _time_and_save("Q2 Actor pairs co-starring (>=3)", repo.actor_pairs_costars, "q2_actor_pairs_costars.json")
        # _time_and_save("Q3 Actors genre breadth (top 10)", repo.actors_genre_breadth_top10, "q3_actors_genre_breadth.json")
        # _time_and_save("Q4 Top collections by total revenue (top 10)", repo.top_collections_by_total_revenue_top10, "q4_top_collections.json")
        # _time_and_save("Q5 Decade x primary genre median runtime", repo.decade_primary_genre_median_runtime, "q5_decade_primary_genre_median_runtime.json")
        # _time_and_save("Q6 Female proportion among top-5 cast by decade", repo.female_top5_proportion_by_decade, "q6_female_top5_proportion_by_decade.json")
        _time_and_save("Q7 Top noir / neo-noir movies", repo.top_noir_movies, "q7_top_noir_movies.json")
        # _time_and_save("Q8 Top director–actor pairs", repo.top_director_actor_pairs, "q8_top_director_actor_pairs.json")
        # _time_and_save("Q9 Top original languages in US-involved non-English", repo.top10_original_languages_in_us_involved_non_english, "q9_top_original_languages_us_involved_non_english.json")

        # Q10 returns two lists -> save both
        t0 = time.perf_counter()
        diverse, variance = repo.user_stats_toplists()
        dur = time.perf_counter() - t0
        p1 = _save_json("q10_user_stats_genre_diverse.json", {
            "meta": {"label": "Q10 Top 10 genre-diverse users", "duration_sec": round(dur, 3), "count": len(diverse)},
            "data": diverse
        })
        p2 = _save_json("q10_user_stats_high_variance.json", {
            "meta": {"label": "Q10 Top 10 highest-variance users", "duration_sec": round(dur, 3), "count": len(variance)},
            "data": variance
        })
        print(f"Q10 -> {p1} and {p2} ({round(dur, 2)}s)")
    finally:
        repo.close()