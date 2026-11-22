import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dashboard"))
from app import create_app
client = create_app({"DATA_MODE": "snapshot"}).test_client()
def get(path):
    response = client.get(path)
    assert response.status_code == 200, (path, response.status_code)
    return response.get_json()
similar = get("/api/movie/similar?movie_id=260")["movies"]
assert len(similar) == 10 and all(0 <= r["similarity"] <= 1 for r in similar)
assert get("/api/profile/age-genre")["rows"]
assert get("/api/profile/gender")["rows"]
assert len(get("/api/genre/top10")["movies"]) == 10
assert get("/api/profile/daily")["rows"]
graph = get("/api/graph")
assert 0 < len(graph["nodes"]) <= 100
ids = {r["movie_id"] for r in graph["nodes"]}
assert all(e["source"] in ids and e["target"] in ids for e in graph["edges"])
assert graph["metrics"]["communities"] > 1
assert client.get("/api/movie/similar?movie_id=abc").status_code == 400
print("M1/M2/M4 offline endpoints and graph integrity verified")
