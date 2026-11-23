# 慢速 Range 服务器（续传/关窗 E2E 用）: GET /f 返回 3MB 确定性数据, 支持 Range, 限速 ~150KB/s
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DATA = hashlib.sha256(b"suntey-e2e").digest() * 46875  # 3000000B
PORT = 18093

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        rng = self.headers.get("Range")
        if rng and rng.startswith("bytes="):
            s, e = rng[6:].split("-")
            start = int(s or 0)
            end = int(e) if e else len(DATA) - 1
            end = min(end, len(DATA) - 1)
            body = DATA[start:end + 1]
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(DATA)}")
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(len(body)))
        else:
            body = DATA
            self.send_response(200)
            self.send_header("Content-Length", str(len(DATA)))
            self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        try:
            for i in range(0, len(body), 15000):  # ~150KB/s pacing
                self.wfile.write(body[i:i + 15000])
                self.wfile.flush()
                import time as _t
                _t.sleep(0.1)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *a):
        pass

if __name__ == "__main__":
    import hashlib as _h
    print("sha256:", _h.sha256(DATA).hexdigest())
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
