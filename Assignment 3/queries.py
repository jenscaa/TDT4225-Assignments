import json
from pathlib import Path

from pymongo import MongoClient
import time
import pprint

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "movieDB"

pp = pprint.PrettyPrinter(indent=2, width=100)


def connect_to_db():
    """Connect to MongoDB and return a database handle."""
    print("Connecting to MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    print(f"Connected to database: {DB_NAME}")
    return db


def run_query(query_number, description, func):
    """Run a query function, pretty-print results, and save them to a JSON file."""

    print(f"QUERY {query_number}: {description}")

    start = time.time()
    try:
        # Run the query and collect all results
        results = list(func())
        duration = time.time() - start

        # Print summary info
        print(f"\nCompleted in {duration:.2f} seconds.")
        print(f"Total results: {len(results)}\n")

        # Pretty print first 20 results to console
        for i, r in enumerate(results[:20], start=1):
            print(f"--- Result {i} ---")
            pp.pprint(r)
            print()

        if len(results) > 20:
            print(f"... ({len(results) - 20} more results not shown)")

        # --- Save results to JSON ---
        results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)

        # Construct output path (e.g. results/query_1.json)
        filename = f"query_{query_number}.json"
        output_path = results_dir / filename

        # Save formatted data
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "meta": {
                        "query_number": query_number,
                        "description": description,
                        "duration_sec": round(duration, 3),
                        "count": len(results)
                    },
                    "data": results,
                },
                f,
                ensure_ascii=False,
                indent=2,
                default=str  # handles datetime or ObjectId
            )

        print(f"\nResults saved to: {output_path.resolve()}")

    except Exception as e:
        print(f"Error while running query {query_number}: {e}")


if __name__ == "__main__":
    db = connect_to_db()


    def query_1():
        """Considering only crew with job = Director, find top 10 directors (≥5 movies)
        ranked by median revenue, with movie count and mean vote_average.
        """
        pipeline = [
            # Keep movies with crew info and valid revenue
            {
                "$match": {
                    "crew": {"$exists": True, "$ne": []},
                    "revenue": {"$gt": 0}
                }
            },
            # Expand crew array and keep only directors
            {"$unwind": "$crew"},
            {"$match": {"crew.job": "Director"}},

            # Group by director
            {
                "$group": {
                    "_id": "$crew.name",
                    "movies": {"$sum": 1},
                    "revenues": {"$push": "$revenue"},
                    "avg_vote": {"$avg": "$vote_average"}
                }
            },
            # Keep only directors with >=5 movies
            {"$match": {"movies": {"$gte": 5}}},

            # Compute median revenue
            {
                "$project": {
                    "movies": 1,
                    "avg_vote": 1,
                    "median_revenue": {
                        "$let": {
                            "vars": {"sorted": {"$sortArray": {"input": "$revenues", "sortBy": 1}}},
                            "in": {
                                "$cond": [
                                    {"$eq": [{"$mod": [{"$size": "$$sorted"}, 2]}, 0]},
                                    {
                                        "$avg": [
                                            {"$arrayElemAt": ["$$sorted", {
                                                "$subtract": [{"$divide": [{"$size": "$$sorted"}, 2]}, 1]}]},
                                            {"$arrayElemAt": ["$$sorted", {"$divide": [{"$size": "$$sorted"}, 2]}]}
                                        ]
                                    },
                                    {"$arrayElemAt": ["$$sorted", {"$floor": {"$divide": [{"$size": "$$sorted"}, 2]}}]}
                                ]
                            }
                        }
                    }
                }
            },
            # Sort and limit to top 10
            {"$sort": {"median_revenue": -1}},
            {"$limit": 10}
        ]
        return db.movies.aggregate(pipeline)


    run_query(1, "Top 10 Directors by Median Revenue (≥5 movies)", query_1)


    def query_2():
        """
        Find all actor pairs who have co-starred in ≥ 3 movies,
        along with their number of co-appearances and average vote_average.
        Sorted by co-appearance count (descending).
        """
        pipeline = [
            # Filter only movies with cast and numeric vote_average
            {
                "$match": {
                    "cast": {"$exists": True, "$ne": []},
                    "vote_average": {"$type": "number"}
                }
            },

            # Keep only actor names
            {
                "$project": {
                    "cast_names": {
                        "$map": {"input": "$cast", "as": "c", "in": "$$c.name"}
                    },
                    "vote_average": 1
                }
            },

            # Keep only movies with at least 2 actors
            {"$match": {"cast_names.1": {"$exists": True}}},

            # Generate all possible unique actor pairs
            {
                "$project": {
                    "vote_average": 1,
                    "pairs": {
                        "$reduce": {
                            "input": {"$range": [0, {"$size": "$cast_names"}]},
                            "initialValue": [],
                            "in": {
                                "$concatArrays": [
                                    "$$value",
                                    {
                                        "$map": {
                                            "input": {
                                                "$range": [
                                                    {"$add": ["$$this", 1]},
                                                    {"$size": "$cast_names"}
                                                ]
                                            },
                                            "as": "j",
                                            "in": [
                                                {"$arrayElemAt": ["$cast_names", "$$this"]},
                                                {"$arrayElemAt": ["$cast_names", "$$j"]}
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }
            },

            # Unwind actor pairs
            {"$unwind": "$pairs"},

            # Normalize pairs alphabetically to avoid duplicates
            {
                "$project": {
                    "vote_average": 1,
                    "pair": {
                        "$cond": [
                            {"$lt": [{"$arrayElemAt": ["$pairs", 0]}, {"$arrayElemAt": ["$pairs", 1]}]},
                            "$pairs",
                            [
                                {"$arrayElemAt": ["$pairs", 1]},
                                {"$arrayElemAt": ["$pairs", 0]}
                            ]
                        ]
                    }
                }
            },

            # Group by actor pair
            {
                "$group": {
                    "_id": "$pair",
                    "co_appearances": {"$sum": 1},
                    "avg_vote": {"$avg": "$vote_average"}
                }
            },

            # Keep only pairs with ≥ 3 co-appearances
            {"$match": {"co_appearances": {"$gte": 3}}},

            # Sort descending by number of co-appearances
            {"$sort": {"co_appearances": -1, "_id": 1}},

            # Limit output
            {"$limit": 20}
        ]

        return db.movies.aggregate(pipeline)


    run_query(2, "Example placeholder query", query_2)


    def query_3():
        """
        Find the top 10 actors (≥10 movies) with the widest genre breadth.
        Report actor name, number of distinct genres, and up to 5 example genres.
        """
        pipeline = [
            # Keep only movies that have both cast and genres
            {
                "$match": {
                    "cast": {"$exists": True, "$ne": []},
                    "genres": {"$exists": True, "$ne": []}
                }
            },

            # Create all actor–genre combinations
            {
                "$project": {
                    "cast_names": {"$map": {"input": "$cast", "as": "c", "in": "$$c.name"}},
                    "genre_names": {"$map": {"input": "$genres", "as": "g", "in": "$$g.name"}}
                }
            },
            {"$unwind": "$cast_names"},
            {"$unwind": "$genre_names"},

            # Group by actor and collect distinct genres + movie count
            {
                "$group": {
                    "_id": "$cast_names",
                    "genres": {"$addToSet": "$genre_names"},
                    "movies": {"$addToSet": "$$ROOT"}  # track distinct movies per actor
                }
            },

            # Compute counts
            {
                "$project": {
                    "genre_count": {"$size": "$genres"},
                    "example_genres": {"$slice": ["$genres", 5]},
                    "movie_count": {"$size": "$movies"}
                }
            },

            # Only keep actors with ≥10 credited movies
            {"$match": {"movie_count": {"$gte": 10}}},

            # Sort descending by genre_count
            {"$sort": {"genre_count": -1, "movie_count": -1}},

            # Top 10
            {"$limit": 10}
        ]

        return db.movies.aggregate(pipeline)


    run_query(
        3,
        "Top 10 actors (≥10 movies) with widest genre breadth",
        query_3
    )


    def query_4():
        """
        For film collections (belongs_to_collection.name not null) with ≥3 movies,
        find the 10 with the largest total revenue.
        Report movie count, total revenue, median vote_average,
        and earliest → latest release date.
        """
        pipeline = [
            # Keep only movies that belong to a collection and have valid revenue & vote data
            {
                "$match": {
                    "belongs_to_collection.name": {"$exists": True, "$ne": None, "$ne": ""},
                    "revenue": {"$gt": 0},
                    "vote_average": {"$type": "number"},
                    "release_date": {"$exists": True, "$type": "string", "$ne": ""}
                }
            },

            # Group by collection name
            {
                "$group": {
                    "_id": "$belongs_to_collection.name",
                    "movie_count": {"$sum": 1},
                    "total_revenue": {"$sum": "$revenue"},
                    "votes": {"$push": "$vote_average"},
                    "release_dates": {"$push": "$release_date"}
                }
            },

            # Only include collections with ≥3 movies
            {"$match": {"movie_count": {"$gte": 3}}},

            # Compute median vote_average + min/max release dates
            {
                "$project": {
                    "movie_count": 1,
                    "total_revenue": 1,
                    "median_vote": {
                        "$let": {
                            "vars": {"sorted": {"$sortArray": {"input": "$votes", "sortBy": 1}}},
                            "in": {
                                "$cond": [
                                    {"$eq": [{"$mod": [{"$size": "$$sorted"}, 2]}, 0]},
                                    {
                                        "$avg": [
                                            {"$arrayElemAt": ["$$sorted", {
                                                "$subtract": [{"$divide": [{"$size": "$$sorted"}, 2]}, 1]}]},
                                            {"$arrayElemAt": ["$$sorted", {"$divide": [{"$size": "$$sorted"}, 2]}]}
                                        ]
                                    },
                                    {"$arrayElemAt": ["$$sorted", {"$floor": {"$divide": [{"$size": "$$sorted"}, 2]}}]}
                                ]
                            }
                        }
                    },
                    "earliest_date": {"$min": "$release_dates"},
                    "latest_date": {"$max": "$release_dates"}
                }
            },

            # Sort descending by total revenue
            {"$sort": {"total_revenue": -1}},

            # Top 10
            {"$limit": 10}
        ]

        return db.movies.aggregate(pipeline)


    run_query(
        4,
        "Top 10 film collections (≥3 movies) by total revenue",
        query_4
    )


    def query_5():
        """
        By decade and primary genre (first element in genres),
        compute the median runtime and movie count.
        Sort results by decade, then median runtime (desc).
        """
        pipeline = [
            # Filter movies with valid release_date, runtime, and genres
            {
                "$match": {
                    "release_date": {"$exists": True, "$type": "string", "$ne": ""},
                    "runtime": {"$gt": 0},
                    "genres": {"$exists": True, "$ne": []}
                }
            },

            # Extract decade and primary genre
            {
                "$addFields": {
                    "year": {
                        "$toInt": {"$substr": ["$release_date", 0, 4]}
                    }
                }
            },
            {
                "$addFields": {
                    "decade": {
                        "$multiply": [
                            {"$floor": {"$divide": ["$year", 10]}},
                            10
                        ]
                    },
                    "primary_genre": {"$arrayElemAt": ["$genres.name", 0]}
                }
            },

            # Group by decade + primary genre
            {
                "$group": {
                    "_id": {
                        "decade": "$decade",
                        "genre": "$primary_genre"
                    },
                    "runtimes": {"$push": "$runtime"},
                    "movie_count": {"$sum": 1}
                }
            },

            # Compute median runtime
            {
                "$project": {
                    "movie_count": 1,
                    "median_runtime": {
                        "$let": {
                            "vars": {"sorted": {"$sortArray": {"input": "$runtimes", "sortBy": 1}}},
                            "in": {
                                "$cond": [
                                    {"$eq": [{"$mod": [{"$size": "$$sorted"}, 2]}, 0]},
                                    {
                                        "$avg": [
                                            {"$arrayElemAt": ["$$sorted", {
                                                "$subtract": [{"$divide": [{"$size": "$$sorted"}, 2]}, 1]}]},
                                            {"$arrayElemAt": ["$$sorted", {"$divide": [{"$size": "$$sorted"}, 2]}]}
                                        ]
                                    },
                                    {"$arrayElemAt": ["$$sorted", {"$floor": {"$divide": [{"$size": "$$sorted"}, 2]}}]}
                                ]
                            }
                        }
                    }
                }
            },

            # Sort by decade then median_runtime (desc)
            {"$sort": {"_id.decade": 1, "median_runtime": -1}}
        ]

        return db.movies.aggregate(pipeline)


    run_query(
        5,
        "By decade and primary genre: median runtime and movie count",
        query_5
    )


    # TODO Must fix gender properly
    def query_6():
        """
        For each movie’s top-billed 5 cast, compute the proportion of female cast (ignoring unknowns).
        Aggregate by decade, sorted by average female proportion (desc), including movie counts.
        """
        pipeline = [
            # Keep only movies with release date and cast info
            {
                "$match": {
                    "cast": {"$exists": True, "$ne": []},
                    "release_date": {"$exists": True, "$type": "string", "$ne": ""}
                }
            },

            # Extract year and decade
            {
                "$addFields": {
                    "year": {
                        "$toInt": {"$substr": ["$release_date", 0, 4]}
                    }
                }
            },
            {
                "$addFields": {
                    "decade": {
                        "$multiply": [
                            {"$floor": {"$divide": ["$year", 10]}},
                            10
                        ]
                    }
                }
            },

            # Slice cast to top 5 billed members (by order)
            {"$unwind": "$cast"},
            {"$sort": {"_id": 1, "cast.order": 1}},  # order within each movie
            {
                "$group": {
                    "_id": "$_id",
                    "decade": {"$first": "$decade"},
                    "cast_top5": {"$push": "$cast"}
                }
            },
            {
                "$project": {
                    "decade": 1,
                    "cast_top5": {"$slice": ["$cast_top5", 5]}
                }
            },

            # Compute female proportion (ignore gender = 0 or null)
            {
                "$project": {
                    "decade": 1,
                    "valid_cast": {
                        "$filter": {
                            "input": "$cast_top5",
                            "as": "c",
                            "cond": {"$in": ["$$c.gender", [1, 2]]}
                        }
                    }
                }
            },
            {
                "$addFields": {
                    "female_count": {
                        "$size": {
                            "$filter": {
                                "input": "$valid_cast",
                                "as": "c",
                                "cond": {"$eq": ["$$c.gender", 1]}
                            }
                        }
                    },
                    "valid_count": {"$size": "$valid_cast"}
                }
            },
            {
                "$addFields": {
                    "female_proportion": {
                        "$cond": [
                            {"$gt": ["$valid_count", 0]},
                            {"$divide": ["$female_count", "$valid_count"]},
                            None
                        ]
                    }
                }
            },

            # Group by decade
            {
                "$group": {
                    "_id": "$decade",
                    "avg_female_proportion": {"$avg": "$female_proportion"},
                    "movie_count": {"$sum": 1}
                }
            },

            # Sort by average female proportion (desc)
            {"$sort": {"avg_female_proportion": -1}}
        ]

        return db.movies.aggregate(pipeline)


    run_query(
        6,
        "Average female proportion (top-5 cast) per decade",
        query_6
    )


    def query_7():
        """
        Query 7:
        Using a regex search (instead of a text index) over `overview` and `tagline`,
        find the 20 movies matching “noir” or “neo-noir” (vote_count ≥ 50)
        with the highest vote_average.
        Return title, year, vote_average, and vote_count.
        """
        pipeline = [
            # Match at least 50 votes and 'noir' or 'neo-noir' in overview/tagline
            {
                "$match": {
                    "$and": [
                        {"vote_count": {"$gte": 50}},
                        {
                            "$or": [
                                {"overview": {"$regex": "noir", "$options": "i"}},
                                {"tagline": {"$regex": "noir", "$options": "i"}},
                            ]
                        },
                    ]
                }
            },
            # Project only relevant fields
            {
                "$project": {
                    "title": 1,
                    "release_date": 1,
                    "vote_average": 1,
                    "vote_count": 1,
                }
            },
            # Extract year from release_date
            {
                "$addFields": {
                    "year": {"$substr": ["$release_date", 0, 4]}
                }
            },
            # Sort by vote_average descending
            {"$sort": {"vote_average": -1}},
            # Limit to top 20
            {"$limit": 20},
            # Final clean output
            {
                "$project": {
                    "_id": 0,
                    "title": 1,
                    "year": 1,
                    "vote_average": 1,
                    "vote_count": 1,
                }
            },
        ]

        return db.movies.aggregate(pipeline)


    run_query(
        7,
        "Top 20 noir / neo-noir movies by vote_average (vote_count ≥ 50)",
        query_7
    )


    def query_8():
        """
        Query 8:
        Which 20 director–actor pairs with ≥ 3 collaborations (same movie)
        have the highest mean vote_average, considering only movies with
        vote_count ≥ 100. Include the pair’s films count and mean revenue.
        """
        pipeline = [
            # Match movies with enough votes, both cast & crew, and valid revenue
            {
                "$match": {
                    "vote_count": {"$gte": 100},
                    "crew": {"$exists": True, "$ne": []},
                    "cast": {"$exists": True, "$ne": []},
                    "revenue": {"$gt": 0}
                }
            },
            # Unwind arrays to pair each director with each actor
            {"$unwind": "$crew"},
            {"$unwind": "$cast"},
            # Keep only directors
            {"$match": {"crew.job": "Director"}},
            # Group by (director, actor)
            {
                "$group": {
                    "_id": {
                        "director": "$crew.name",
                        "actor": "$cast.name"
                    },
                    "films": {"$sum": 1},
                    "avg_rating": {"$avg": "$vote_average"},
                    "avg_revenue": {"$avg": "$revenue"}
                }
            },
            # Keep only pairs with ≥ 3 movies together
            {"$match": {"films": {"$gte": 3}}},
            # Sort by highest mean vote_average
            {"$sort": {"avg_rating": -1}},
            # Limit to top 20
            {"$limit": 20},
            # Final clean projection
            {
                "$project": {
                    "_id": 0,
                    "director": "$_id.director",
                    "actor": "$_id.actor",
                    "films": 1,
                    "avg_rating": 1,
                    "avg_revenue": 1
                }
            }
        ]

        return db.movies.aggregate(pipeline)


    run_query(
        8,
        "Top 20 director–actor pairs with ≥ 3 collaborations (by mean vote_average, vote_count ≥ 100)",
        query_8
    )


    def query_9():
        """
        Query 9:
        Among movies where original_language ≠ "en" but at least one production
        company or country is United States, find the top 10 original languages by count.
        For each language, report the count and one example title.
        """
        pipeline = [
            # Match: exclude English movies, include those with US company or country
            {
                "$match": {
                    "original_language": {"$ne": "en"},
                    "$or": [
                        {"production_countries.name": "United States of America"},
                        {"production_companies.name": "United States of America"}
                    ]
                }
            },
            # Group by language, count, and pick one example title
            {
                "$group": {
                    "_id": "$original_language",
                    "count": {"$sum": 1},
                    "example_title": {"$first": "$title"}
                }
            },
            # Sort by descending count
            {"$sort": {"count": -1}},
            # Limit to top 10 languages
            {"$limit": 10},
            # Final projection
            {
                "$project": {
                    "_id": 0,
                    "language": "$_id",
                    "count": 1,
                    "example_title": 1
                }
            }
        ]

        return db.movies.aggregate(pipeline)


    run_query(
        9,
        "Top 10 non-English languages in US-produced movies (count and example title)",
        query_9
    )


    def query_10():
        """
        Query 10:
        For each user, compute:
          • their total ratings count,
          • the population variance of their ratings, and
          • the number of distinct genres rated.
        List:
          1) top 10 most genre-diverse users, and
          2) top 10 highest-variance users.
        (Only users with ≥ 20 ratings are included.)

        NOTE: The join path is:
          ratings.movieId → links.movieId → links.tmdbId → movies.tmdbId
        """

        pipeline = [
            # Join ratings with links to map movieId → tmdbId
            {
                "$lookup": {
                    "from": "links",
                    "localField": "movieId",
                    "foreignField": "movieId",
                    "as": "link"
                }
            },
            {"$unwind": "$link"},

            # Join with movies to get movie details (esp. genres)
            {
                "$lookup": {
                    "from": "movies",
                    "localField": "link.tmdbId",
                    "foreignField": "tmdbId",
                    "as": "movie"
                }
            },
            {"$unwind": "$movie"},

            # Flatten genres
            {"$unwind": "$movie.genres"},

            # Group by user to gather stats
            {
                "$group": {
                    "_id": "$userId",
                    "ratings_count": {"$sum": 1},
                    "avg_rating": {"$avg": "$rating"},
                    "rating_values": {"$push": "$rating"},
                    "genres": {"$addToSet": "$movie.genres.name"}
                }
            },

            # Compute population variance and genre count
            {
                "$addFields": {
                    "genre_count": {"$size": "$genres"},
                    "variance": {
                        "$let": {
                            "vars": {
                                "avg": "$avg_rating",
                                "vals": "$rating_values"
                            },
                            "in": {
                                "$cond": [
                                    {"$gt": [{"$size": "$$vals"}, 1]},
                                    {
                                        "$divide": [
                                            {
                                                "$sum": {
                                                    "$map": {
                                                        "input": "$$vals",
                                                        "as": "r",
                                                        "in": {
                                                            "$pow": [
                                                                {"$subtract": ["$$r", "$$avg"]},
                                                                2
                                                            ]
                                                        }
                                                    }
                                                }
                                            },
                                            {"$size": "$$vals"}
                                        ]
                                    },
                                    0
                                ]
                            }
                        }
                    }
                }
            },

            # Filter users with ≥ 20 ratings
            {"$match": {"ratings_count": {"$gte": 20}}},

            # Project clean output
            {
                "$project": {
                    "_id": 0,
                    "userId": "$_id",
                    "ratings_count": 1,
                    "variance": 1,
                    "genre_count": 1
                }
            }
        ]

        # Run aggregation
        results = list(db.ratings.aggregate(pipeline))

        # Post-process results to show two rankings
        most_diverse = sorted(results, key=lambda x: x["genre_count"], reverse=True)[:10]
        most_variable = sorted(results, key=lambda x: x["variance"], reverse=True)[:10]

        print("\nTop 10 Most Genre-Diverse Users:")
        for i, u in enumerate(most_diverse, 1):
            print(f"{i:2d}. User {u['userId']} — {u['genre_count']} genres rated "
                  f"({u['ratings_count']} ratings)")

        print("\nTop 10 Highest-Variance Users:")
        for i, u in enumerate(most_variable, 1):
            print(f"{i:2d}. User {u['userId']} — Variance {u['variance']:.3f} "
                  f"({u['ratings_count']} ratings)")

        return results


    run_query(
        10,
        "User rating diversity and variance (≥ 20 ratings, linked via links.tmdbId)",
        query_10
    )
