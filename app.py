#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DC·HOT 本地服务：托管前端页面并提供资讯 API。
  GET  /            -> index.html
  GET  /api/news    -> news.json（缺失时自动抓取一次）
  POST /api/refresh -> 重新抓取并返回最新数据
服务端每 REFRESH_MINUTES 分钟自动重抓一次，前端轮询即可拿到新数据。
启动：python3 app.py [端口]，默认 8765
"""
import json
import sys
import threading
import time
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

import fetch_news

ROOT = Path(__file__).parent
REFRESH_MINUTES = 10
_lock = threading.Lock()


def auto_refresh():
    """后台定时重抓，让页面"实时"板块持续有新数据。"""
    while True:
        time.sleep(REFRESH_MINUTES * 60)
        try:
            with _lock:
                d = fetch_news.fetch_all()
            print(f"[自动更新] {d['generated_at'][:19]} 共 {d['count']} 条")
        except Exception as e:
            print(f"[自动更新失败] {e}", file=sys.stderr)


def load_or_fetch():
    if fetch_news.OUT_FILE.exists():
        return json.loads(fetch_news.OUT_FILE.read_text(encoding="utf-8"))
    with _lock:
        return fetch_news.fetch_all()


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def _send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.split("?")[0] == "/api/news":
            try:
                self._send_json(load_or_fetch())
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        if self.path == "/":
            self.path = "/index.html"
        super().do_GET()

    def do_POST(self):
        if self.path.split("?")[0] == "/api/refresh":
            try:
                with _lock:
                    data = fetch_news.fetch_all()
                self._send_json(data)
            except Exception as e:
                self._send_json({"error": str(e)}, 500)
            return
        self.send_error(404)

    def log_message(self, fmt, *args):
        sys.stderr.write("%s %s\n" % (self.address_string(), fmt % args))


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    if not fetch_news.OUT_FILE.exists():
        print("首次启动，正在抓取资讯…")
        fetch_news.fetch_all()
    threading.Thread(target=auto_refresh, daemon=True).start()
    server = HTTPServer(("127.0.0.1", port), Handler)
    print(f"DC·HOT 已启动：http://localhost:{port}  （每 {REFRESH_MINUTES} 分钟自动更新，Ctrl+C 停止）")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")


if __name__ == "__main__":
    main()
