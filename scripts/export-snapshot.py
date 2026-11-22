import argparse
from decimal import Decimal
import gzip
import json
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dashboard"))
from app import SNAPSHOT_FILE, create_app, WebHDFS


ROUTES = (
    "/api/overview", "/api/offline/trend", "/api/offline/hourly",
    "/api/offline/movie-scores", "/api/movie/ranking", "/api/realtime",
)


def export(output):
    app = create_app({"DATA_MODE": "cluster"})
    client = app.test_client()

    def get(route):
        response = client.get(route)
        if response.status_code != 200:
            raise ValueError(f"{route} returned HTTP {response.status_code}")
        return response.get_json()

    responses = {route: get(route) for route in ROUTES}
    if responses["/api/realtime"]["snapshot_source"] != "hdfs":
        raise ValueError("Export requires the final realtime snapshot from HDFS")
    user_count = responses["/api/overview"]["user_count"]
    recommendations = {
        str(user_id): get(f"/api/recommendations?user_id={user_id}")["recommendations"]
        for user_id in range(1, user_count + 1)
    }
    if user_count != 6040 or any(len(rows) != 10 for rows in recommendations.values()):
        raise ValueError("Export requires all 6,040 MovieLens 1M users with ten recommendations each")

    addresses = app.config["DATANODES"]
    if isinstance(addresses, str):
        addresses = json.loads(addresses)
    hdfs = WebHDFS(app.config["WEBHDFS"], app.config["HDFS_USER"], addresses)
    profiles = {name: hdfs.parquet_rows("/movie/result/sql/" + name) for name in
                ("age_genre_pivot", "user_profile_cube", "genre_top10", "daily_moving_avg", "occupation_profile")}
    for row in profiles["daily_moving_avg"]:
        row["rating_date"] = str(row["rating_date"])
    snapshot = {"schema_version": 2, "responses": responses, "recommendations": recommendations,
                "similar_movies": hdfs.parquet_rows("/movie/result/rdd/movie_similarity"),
                "sql_profiles": profiles, "graph": get("/api/graph") }
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(".tmp")
    temporary.write_bytes(gzip.compress(json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"), default=lambda value: float(value) if isinstance(value, Decimal) else str(value))
                                        .encode("utf-8"), mtime=0))
    temporary.replace(output)
    print(f"Saved {user_count:,} users and {sum(map(len, recommendations.values())):,} ALS rows to {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export the completed MovieLens result snapshot")
    parser.add_argument("--output", type=Path, default=SNAPSHOT_FILE)
    export(parser.parse_args().output)
