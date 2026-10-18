import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

# Tests default to the parquet engine so they run anywhere (no Maven download).
# Set LINEPULSE_ENGINE=delta to run the same tests against Delta Lake.
ENGINE = os.environ.get("LINEPULSE_ENGINE", "parquet")


@pytest.fixture(scope="session")
def spark():
    from run_local import local_spark
    s = local_spark(ENGINE)
    yield s
    s.stop()


@pytest.fixture
def engine():
    return ENGINE


@pytest.fixture
def fake_llm():
    """Serves scripted chat-completion replies in order and records each request."""
    state = {"replies": [], "requests": [], "status": 200}

    class H(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            state["requests"].append({"auth": self.headers.get("Authorization"), "body": body})
            if state["status"] != 200:
                self.send_response(state["status"]); self.end_headers(); self.wfile.write(b"{}"); return
            msg = state["replies"].pop(0)
            out = json.dumps({"choices": [{"message": msg}]}).encode()
            self.send_response(200); self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(out))); self.end_headers(); self.wfile.write(out)

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    from linepulse.handover import ChatClient
    state["client"] = ChatClient(base_url=f"http://127.0.0.1:{srv.server_port}/v1", api_key="test-key",
                                 model="fake-model", min_interval_s=0)
    yield state
    srv.shutdown()
