#!/bin/bash

cleanup() {
    echo ""
    echo "[run] stopping..."
    kill $MAIN_PID $RECEIVER_PID $PRODUCER_PID 2>/dev/null
    docker compose down
    exit 0
}
trap cleanup SIGINT SIGTERM

echo "[run] starting rabbitmq..."
docker compose up -d
sleep 5

echo "[run] starting receiver on :9090..."
python3 -c "
from http.server import HTTPServer, BaseHTTPRequestHandler
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length)
        print(f'[receiver] POST {self.path} | {body.decode()}')
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'ok')
HTTPServer(('localhost', 9090), H).serve_forever()
" &
RECEIVER_PID=$!
sleep 1

echo "[run] starting main service on :8080..."
go run main.go &
MAIN_PID=$!
sleep 2

echo "[run] starting producer (1000 events/s)..."
go run cmd/producer/main.go &
PRODUCER_PID=$!

wait
