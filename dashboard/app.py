import argparse
import gzip
import json
import os
import socket
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import ProxyHandler, build_opener

from flask import Flask, jsonify, render_template, request


ROOT = Path(__file__).resolve().parent
SNAPSHOT_FILE = ROOT / "snapshots" / "completed-results.json.gz"
OFFLINE = "/movie/result/offline"
RECOMMENDATIONS = "/movie/result/recommendations"
REALTIME = "/movie/result/realtime/latest.json"
WINDOW_FIELDS = {"window_start", "window_end", "rating_count", "avg_rating",
                 "low_rating_ratio", "active_user_count", "alert_level", "batch_id"}


class WebHDFS:
    def __init__(self, root, user, datanodes):
        self.root = root.rstrip("/")
        self.user = user
        self.datanodes = datanodes
        self.opener = build_opener(ProxyHandler({}))

    def _get(self, url):
        with self.opener.open(url, timeout=5) as response:
            return response.read()

    def _query(self, path, operation, **options):
        query = urlencode({"op": operation, "user.name": self.user, **options})
        return json.loads(self._get(f"{self.root}{quote(path, safe='/')}?{query}"))

    def read(self, path):
        location = urlsplit(self._query(path, "OPEN", noredirect="true")["Location"])
        address = self.datanodes.get(location.hostname, location.hostname)
        if location.scheme != "http" or address not in self.datanodes.values() or not location.port:
            raise ValueError(f"Unexpected WebHDFS redirect: {location.hostname}")
        target = location._replace(netloc=f"{address}:{location.port}")
        return self._get(target.geturl())

    def parts(self, path, suffix):
        entries = self._query(path, "LISTSTATUS")["FileStatuses"]["FileStatus"]
        names = {entry["pathSuffix"] for entry in entries if entry["type"] == "FILE"}
        parts = sorted(name for name in names if name.startswith("part-") and name.endswith(suffix))
        if "_SUCCESS" not in names or not parts:
            raise ValueError(f"No completed snapshot in {path}")
        return [f"{path}/{name}" for name in parts]

    def json_lines(self, path):
        return [json.loads(line) for line in self.read(path).splitlines() if line.strip()]

    def json_snapshot(self, path):
        return [row for part in self.parts(path, ".json") for row in self.json_lines(part)]

    def parquet_rows(self, path, columns=None):
        import pyarrow as pa
        import pyarrow.parquet as parquet

        return [row for part in self.parts(path, ".parquet")
                for row in parquet.ParquetFile(pa.BufferReader(self.read(part))).read(
                    columns=columns, use_threads=False).to_pylist()]


class DashboardData:
    def __init__(self, hdfs, cache_file, bootstrap):
        self.hdfs = hdfs
        self.cache_file = cache_file
        self.bootstrap = bootstrap
        self.static_lock = Lock()
        self.realtime_lock = Lock()
        self._offline = None
        self._hourly = None
        self._movie_scores = None
        self._recommendations = None
        self._movies = None
        self._snapshot = None

    def movies(self):
        if self._movies is None:
            self._movies = {}
            for line in self.hdfs.read("/movie/raw/movies.dat").decode("latin-1").splitlines():
                movie_id, title, genres = line.split("::", 2)
                self._movies[int(movie_id)] = {"title": title, "genres": genres.split("|")}
        return self._movies

    def offline(self):
        with self.static_lock:
            if self._offline is None:
                daily = self.hdfs.parquet_rows(f"{OFFLINE}/daily_trend",
                                               ["rating_date", "rating_count", "avg_rating", "active_users"])
                daily.sort(key=lambda row: row["rating_date"])
                monthly = defaultdict(int)
                monthly_score = defaultdict(float)
                month_weekday = [[0] * 7 for _ in range(12)]
                for row in daily:
                    day, count = row["rating_date"], row["rating_count"]
                    month = day.isoformat()[:7]
                    monthly[month] += count
                    monthly_score[month] += count * row["avg_rating"]
                    month_weekday[day.month - 1][day.weekday()] += count
                histogram = self.hdfs.parquet_rows(f"{OFFLINE}/rating_distribution")
                users = self.hdfs.parquet_rows(f"{OFFLINE}/user_stats", ["user_id"])
                self._offline = {
                    "rating_count": sum(monthly.values()), "movie_count": len(self.movies()),
                    "user_count": len(users),
                    "monthly": [{"month": month, "rating_count": count}
                                for month, count in sorted(monthly.items())],
                    "monthly_averages": [round(monthly_score[month] / monthly[month], 4) for month in sorted(monthly)],
                    "daily": [[row["rating_date"].isoformat(), row["rating_count"]] for row in daily],
                    "daily_active_users": [[row["rating_date"].isoformat(), row["active_users"]] for row in daily],
                    "month_weekday": month_weekday,
                    "distribution": sorted(histogram, key=lambda row: row["stars"]),
                    "genres": self.hdfs.json_snapshot(f"{OFFLINE}/flask_snapshot/genre_stats"),
                    "ranking": self.hdfs.json_snapshot(f"{OFFLINE}/flask_snapshot/movie_stats"),
                }
            return self._offline

    def hourly(self):
        with self.static_lock:
            if self._hourly is None:
                rows = self.hdfs.parquet_rows(f"{OFFLINE}/hourly_stats", ["rating_hour", "rating_count"])
                self._hourly = sorted(rows, key=lambda row: row["rating_hour"])
            return self._hourly

    def movie_scores(self):
        with self.static_lock:
            if self._movie_scores is None:
                rows = self.hdfs.parquet_rows(f"{OFFLINE}/movie_stats", ["title", "rating_count", "avg_rating"])
                self._movie_scores = [row for row in rows if row["rating_count"] >= 50]
            return self._movie_scores

    def recommendations(self, user_id):
        with self.static_lock:
            if self._recommendations is None:
                movies = self.movies()
                indexed = defaultdict(list)
                columns = ["user_id", "movie_id", "predicted_rating", "recommendation_rank"]
                for row in self.hdfs.parquet_rows(RECOMMENDATIONS, columns):
                    movie = movies.get(row["movie_id"], {"title": str(row["movie_id"]), "genres": []})
                    indexed[row["user_id"]].append({**row, **movie})
                self._recommendations = indexed
            return sorted(self._recommendations.get(user_id, ()), key=lambda row: row["recommendation_rank"])

    @staticmethod
    def validate_windows(windows):
        if not windows or any(not isinstance(row, dict) or not WINDOW_FIELDS <= row.keys() for row in windows):
            raise ValueError("Incomplete realtime snapshot")
        if len({row["batch_id"] for row in windows}) != 1:
            raise ValueError("Realtime snapshot mixes micro-batches")
        return sorted(windows, key=lambda row: row["window_start"])

    def _restore_snapshot(self):
        snapshot = json.loads(self.cache_file.read_text(encoding="utf-8"))
        snapshot["windows"] = self.validate_windows(snapshot["windows"])
        return snapshot

    def _save_snapshot(self, snapshot):
        self.cache_file.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.cache_file.with_suffix(f".{os.getpid()}.tmp")
        temporary.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
        temporary.replace(self.cache_file)

    def realtime(self):
        with self.realtime_lock:
            source = "hdfs"
            try:
                windows = self.validate_windows(self.hdfs.json_lines(REALTIME))
                if self._snapshot is None or self._snapshot["windows"] != windows:
                    self._snapshot = {"windows": windows, "fetched_at": datetime.now(timezone.utc).isoformat()}
                    try:
                        self._save_snapshot(self._snapshot)
                    except OSError:
                        pass  # The in-memory snapshot is still available.
            except (OSError, ValueError, KeyError) as error:
                source = "cache"
                if self._snapshot is None:
                    try:
                        self._snapshot = self._restore_snapshot()
                    except (OSError, ValueError, KeyError):
                        raise RuntimeError("No realtime snapshot is available") from error
            snapshot = self._snapshot

        try:
            with socket.create_connection(self.bootstrap, timeout=0.4):
                reachable = True
        except OSError:
            reachable = False

        return {**snapshot, "snapshot_source": source, "kafka_reachable": reachable,
                "stale": source == "cache" or not reachable, "replay_mode": "historical"}


class BundledResults:
    def __init__(self, path):
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            snapshot = json.load(stream)
        if snapshot["schema_version"] not in (1, 2):
            raise ValueError("Unsupported bundled snapshot version")
        self.responses = snapshot["responses"]
        self.recommendation_rows = snapshot["recommendations"]

    def offline(self):
        return {**self.responses["/api/overview"], **self.responses["/api/offline/trend"],
                "ranking": self.responses["/api/movie/ranking"]["movies"]}

    def hourly(self):
        return self.responses["/api/offline/hourly"]["hourly"]

    def movie_scores(self):
        return self.responses["/api/offline/movie-scores"]["movies"]

    def recommendations(self, user_id):
        return self.recommendation_rows.get(str(user_id), [])

    def realtime(self):
        return {**self.responses["/api/realtime"], "snapshot_source": "bundled",
                "kafka_reachable": None, "stale": False}


def create_app(config=None):
    app = Flask(__name__)
    app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0
    app.config.from_mapping(
        WEBHDFS=os.environ.get("MOVIELENS_WEBHDFS"),
        HDFS_USER=os.environ.get("MOVIELENS_HDFS_USER", "hadoop"),
        DATANODES=os.environ.get("MOVIELENS_DATANODES", "{}"),
        KAFKA_BOOTSTRAP=os.environ.get("MOVIELENS_KAFKA_BOOTSTRAP", "namenode:9092"),
        CACHE_FILE=ROOT / ".cache" / "realtime.json",
        DATA_MODE=os.environ.get("MOVIELENS_MODE"),
        SNAPSHOT_FILE=SNAPSHOT_FILE,
    )
    app.config.update(config or {})
    mode = app.config["DATA_MODE"] or ("cluster" if app.config["WEBHDFS"] else "snapshot")
    if mode == "snapshot":
        dashboard = BundledResults(app.config["SNAPSHOT_FILE"])
    elif mode == "cluster":
        try:
            import pyarrow  # Required for HDFS Parquet reads in cluster mode.
        except ModuleNotFoundError as error:
            raise RuntimeError("Install dashboard/requirements-cluster.txt for cluster mode") from error
        hostname, port = app.config["KAFKA_BOOTSTRAP"].rsplit(":", 1)
        addresses = app.config["DATANODES"]
        if isinstance(addresses, str):
            addresses = json.loads(addresses)
        hdfs = WebHDFS(
            app.config["WEBHDFS"] or "http://namenode:9870/webhdfs/v1",
            app.config["HDFS_USER"],
            addresses or {"datanode1": "datanode1", "datanode2": "datanode2"},
        )
        dashboard = DashboardData(hdfs, Path(app.config["CACHE_FILE"]), (hostname, int(port)))
    else:
        raise ValueError("MOVIELENS_MODE must be snapshot or cluster")

    @app.get("/")
    def index():
        return render_template("index.html", data_mode=mode)

    similar_cache = None
    similar_lock = Lock()

    @app.get("/api/movie/similar")
    def similar_movies():
        nonlocal similar_cache
        value = request.args.get("movie_id", "260")
        if not value.isascii() or not value.isdecimal() or len(value) > 10 or int(value) <= 0:
            return jsonify(error="请输入有效的电影编号"), 400
        with similar_lock:
            if similar_cache is None:
                if mode == "cluster":
                    rows = hdfs.parquet_rows("/movie/result/rdd/movie_similarity")
                else:
                    with gzip.open(app.config["SNAPSHOT_FILE"], "rt", encoding="utf-8") as source:
                        rows = json.load(source).get("similar_movies")
                    if rows is None:
                        return jsonify(error="当前快照尚未导出相似电影，请使用集群模式"), 503
                grouped = defaultdict(list)
                for row in rows:
                    grouped[int(row["movie_id"])].append(row)
                similar_cache = grouped
        rows = sorted(similar_cache.get(int(value), []), key=lambda row: row["rank"])
        return jsonify(movie_id=int(value), movies=rows)

    profile_cache = {}
    profile_lock = Lock()

    def profile_rows(name):
        with profile_lock:
            if name not in profile_cache:
                if mode == "cluster":
                    rows = hdfs.parquet_rows("/movie/result/sql/" + name)
                else:
                    with gzip.open(app.config["SNAPSHOT_FILE"], "rt", encoding="utf-8") as source:
                        rows = json.load(source).get("sql_profiles", {}).get(name)
                    if rows is None:
                        raise ValueError("当前快照尚未导出用户画像")
                profile_cache[name] = rows
            return profile_cache[name]

    @app.get("/api/profile/age-genre")
    def age_genre():
        return jsonify(rows=profile_rows("age_genre_pivot"))

    @app.get("/api/profile/gender")
    def gender_profile():
        return jsonify(rows=[r for r in profile_rows("user_profile_cube") if r["grouping_id"] == 2])

    @app.get("/api/genre/top10")
    def genre_top10():
        rows = profile_rows("genre_top10")
        genres = sorted({r["genre"] for r in rows})
        genre = request.args.get("genre", "Sci-Fi")
        if genre not in genres:
            return jsonify(error="未知电影类型"), 400
        return jsonify(genres=genres, genre=genre, movies=sorted(
            [r for r in rows if r["genre"] == genre], key=lambda r: r["rank"]))

    @app.get("/api/profile/daily")
    def profile_daily():
        rows = sorted(profile_rows("daily_moving_avg"), key=lambda r: str(r["rating_date"]))
        return jsonify(rows=[dict(r, rating_date=str(r["rating_date"])) for r in rows])

    graph_cache = None
    graph_lock = Lock()

    @app.get("/api/graph")
    def movie_graph():
        nonlocal graph_cache
        with graph_lock:
            if graph_cache is None:
                if mode == "cluster":
                    root = "/movie/result/graph_v3"
                    ranks = hdfs.parquet_rows(root + "/pagerank")
                    graph_cache = {
                        "nodes": hdfs.json_snapshot(root + "/edges_top/nodes"),
                        "edges": hdfs.json_snapshot(root + "/edges_top/edges"),
                        "metrics": hdfs.json_snapshot(root + "/metrics")[0],
                        "communities": hdfs.json_snapshot(root + "/community_summary"),
                        "ranking": sorted(ranks, key=lambda r: (-r["pagerank"], r["movie_id"]))[:20],
                        "popular": dashboard.offline()["ranking"],
                    }
                else:
                    with gzip.open(app.config["SNAPSHOT_FILE"], "rt", encoding="utf-8") as source:
                        graph_cache = json.load(source).get("graph")
                    if graph_cache is None:
                        raise ValueError("当前快照尚未导出关系图，请使用集群模式")
        return jsonify(graph_cache)

    @app.get("/api/overview")
    def overview():
        data = dashboard.offline()
        return jsonify({key: data[key] for key in ("rating_count", "movie_count", "user_count")})

    @app.get("/api/offline/trend")
    def offline_trend():
        data = dashboard.offline()
        return jsonify({key: data[key] for key in ("monthly", "distribution", "genres", "month_weekday",
                                                   "daily", "daily_active_users", "monthly_averages")})

    @app.get("/api/offline/hourly")
    def hourly_activity():
        return jsonify(hourly=dashboard.hourly())

    @app.get("/api/offline/movie-scores")
    def movie_scores():
        return jsonify(movies=dashboard.movie_scores())

    @app.get("/api/movie/ranking")
    def ranking():
        return jsonify(movies=dashboard.offline()["ranking"])

    @app.get("/api/recommendations")
    def recommendations():
        user_id = request.args.get("user_id", default="1", type=str)
        if len(user_id) > 10 or not user_id.isascii() or not user_id.isdecimal() or int(user_id) <= 0:
            return jsonify(error="请输入有效的用户编号"), 400
        rows = dashboard.recommendations(int(user_id))
        if not rows:
            return jsonify(error="该用户没有推荐结果", user_id=int(user_id)), 404
        return jsonify(user_id=int(user_id), recommendations=rows)

    @app.get("/api/realtime")
    def realtime():
        response = jsonify(dashboard.realtime())
        response.cache_control.no_store = True
        return response

    @app.get("/api/alerts")
    def alerts():
        snapshot = dashboard.realtime()
        relevant = [row for row in reversed(snapshot["windows"]) if row["alert_level"] in {"HIGH", "MEDIUM"}]
        return jsonify(alerts=relevant, snapshot_source=snapshot["snapshot_source"], stale=snapshot["stale"])

    @app.errorhandler(OSError)
    @app.errorhandler(ValueError)
    @app.errorhandler(KeyError)
    @app.errorhandler(RuntimeError)
    def data_unavailable(error):
        app.logger.warning("Dashboard data unavailable: %s", error)
        return jsonify(error="暂时无法读取结果，请稍后重试"), 503

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MovieLens Flask dashboard")
    parser.add_argument("--port", default=5066, type=int)
    parser.add_argument("--mode", choices=("snapshot", "cluster"))
    args = parser.parse_args()
    settings = {"DATA_MODE": args.mode} if args.mode else None
    try:
        app = create_app(settings)
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        parser.exit(1, f"MovieLens startup failed: {error}\n")
    app.run(host="127.0.0.1", port=args.port, debug=False, threaded=True)
