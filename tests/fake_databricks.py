"""A fake Databricks SQL Statement Execution API, for testing the .NET API's Databricks mode offline.

Serves POST /api/2.0/sql/statements/ in the real response shape (JSON_ARRAY format:
every cell a string, types in manifest.schema.columns) from the local gold export,
and splits results into two chunks so the client's chunk-following is exercised.

    python tests/fake_databricks.py --export local_data/export --port 8799
    DATABRICKS_HOST=http://127.0.0.1:8799 DATABRICKS_TOKEN=fake DATABRICKS_WAREHOUSE_ID=wh \
      LinePulse__Mode=Databricks dotnet run --project api/LinePulse.Api
"""

import argparse
import json
import re
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

CHUNKS: dict[str, dict] = {}


def type_name(values):
    v = next((x for x in values if x is not None), None)
    if isinstance(v, bool):
        return "BOOLEAN"
    if isinstance(v, int):
        return "LONG"
    if isinstance(v, float):
        return "DOUBLE"
    if isinstance(v, list):
        return "ARRAY"
    if isinstance(v, str) and re.match(r"\d{4}-\d\d-\d\dT", v):
        return "TIMESTAMP"
    return "STRING"


def cell(v, t):
    if v is None:
        return None
    if t == "BOOLEAN":
        return "true" if v else "false"
    if t == "ARRAY":
        return json.dumps(v)
    if t == "TIMESTAMP":
        return v.replace("Z", "") + ".000Z" if "." not in v else v
    return str(v)


class Handler(BaseHTTPRequestHandler):
    export: Path

    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self):
        return self.headers.get("Authorization", "").startswith("Bearer ")

    def do_GET(self):  # chunk links
        if not self._authorized():
            return self._send(401, {"message": "missing token"})
        chunk = CHUNKS.pop(self.path, None)
        return self._send(200, chunk) if chunk else self._send(404, {"message": "no such chunk"})

    def do_POST(self):
        if not self._authorized():
            return self._send(401, {"message": "missing token"})
        req = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        assert req.get("warehouse_id") and req.get("format") == "JSON_ARRAY", req
        sql, params = req["statement"], {p["name"]: p.get("value") for p in req.get("parameters", [])}
        table = re.findall(r"FROM\s+\w+\.\w+\.(gold_\w+)", sql)[-1]
        rows = json.loads((self.export / f"{table}.json").read_text())
        for col in ("line_id", "status", "signal"):
            if params.get(col) is not None:
                rows = [r for r in rows if r.get(col) == params[col]]
        if not rows:
            return self._send(200, {"statement_id": "s0", "status": {"state": "SUCCEEDED"},
                                    "manifest": {"schema": {"columns": []}}, "result": {"data_array": []}})
        cols = list(rows[0].keys())
        types = {c: type_name([r[c] for r in rows]) for c in cols}
        data = [[cell(r[c], types[c]) for c in cols] for r in rows]
        half = max(1, len(data) // 2)
        link = None
        statement_id = uuid.uuid4().hex  # unique per statement, as on Databricks (concurrent requests)
        if len(data) > 1:
            link = f"/api/2.0/sql/statements/{statement_id}/result/chunks/1"
            CHUNKS[link] = {"chunk_index": 1, "data_array": data[half:]}
        self._send(200, {
            "statement_id": statement_id,
            "status": {"state": "SUCCEEDED"},
            "manifest": {"format": "JSON_ARRAY",
                         "schema": {"columns": [{"name": c, "type_name": types[c]} for c in cols]}},
            "result": {"chunk_index": 0, "data_array": data[:half], "next_chunk_internal_link": link},
        })

    def log_message(self, *_):
        pass


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--export", default="local_data/export")
    ap.add_argument("--port", type=int, default=8799)
    a = ap.parse_args()
    Handler.export = Path(a.export)
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()
