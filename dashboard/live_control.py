"""Proxy replay controls through the local SSH tunnel."""
import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, ProxyHandler, build_opener

from flask import Blueprint, jsonify, request

live_control = Blueprint("live_control", __name__)
opener = build_opener(ProxyHandler({}))
BASE = "http://127.0.0.1:8090"

def forward(path, method="GET"):
    headers = {"Accept": "application/json"}
    if method == "POST":
        headers["X-Replay-Control"] = "1"

    upstream = Request(
        BASE + path,
        data=b"" if method == "POST" else None,
        headers=headers,
        method=method,
    )

    try:
        with opener.open(upstream, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
            code = response.status
    except HTTPError as error:
        try:
            payload = json.loads(error.read().decode("utf-8"))
        except (ValueError, UnicodeError):
            payload = {"error": "控制服务返回异常"}
        code = error.code
    except (URLError, OSError, ValueError, UnicodeError):
        payload = {
            "error": "无法连接评分控制服务，请检查 SSH 隧道和虚拟机控制接口"
        }
        code = 503

    result = jsonify(payload)
    result.status_code = code
    result.headers["Cache-Control"] = "no-store"
    return result

@live_control.get("/api/live-control/status")
def status():
    return forward("/status")

@live_control.post("/api/live-control/<action>")
def control(action):
    if action not in ("resume", "pause", "stop"):
        return jsonify(error="不支持的控制操作"), 404

    # Only the same-origin page with an explicit control header may mutate.
    if request.headers.get("X-Replay-Control") != "1":
        return jsonify(error="缺少控制请求头"), 403

    origin = request.headers.get("Origin")
    if origin and origin != request.host_url.rstrip("/"):
        return jsonify(error="不允许跨来源控制"), 403

    return forward("/" + action, "POST")