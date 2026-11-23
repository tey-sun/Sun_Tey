#!/usr/bin/env bash
taskkill //F //IM suntey-core.exe >/dev/null 2>&1
sleep 0.5
rm -f /tmp/rec2.db
cd "H:/软件编写/gopeed/suntey/core"
./bin/suntey-core.exe serve --port 0 --store /tmp/rec2.db --dir "C:/Users/TuanTuan/Downloads" >/tmp/l5.json 2>&1 &
sleep 1.2
OUT=$(head -1 /tmp/l5.json)
PORT=$(echo "$OUT" | python -c "import sys,json;print(json.load(sys.stdin)['port'])" 2>/dev/null)
TOKEN=$(echo "$OUT" | python -c "import sys,json;print(json.load(sys.stdin)['token'])" 2>/dev/null)
[ -z "$PORT" ] && { echo "no listen line"; cat /tmp/l5.json; taskkill //F //IM suntey-core.exe >/dev/null 2>&1; exit 1; }
RES=$(curl -s -X POST -H "X-SunTey-Token: $TOKEN" -H 'Content-Type: application/json' -d '{"url":"https://registry.npmmirror.com/-/binary/node/v20.11.0/node-v20.11.0-win-x64.zip"}' http://127.0.0.1:$PORT/api/v1/tasks)
TID=$(echo "$RES" | python -c "import sys,json;print(json.load(sys.stdin)['data']['id'])")
curl -s -X POST -H "X-SunTey-Token: $TOKEN" -H 'Content-Type: application/json' -d "{\"id\":\"$TID\"}" http://127.0.0.1:$PORT/api/v1/tasks/start >/dev/null
sleep 2
taskkill //F //IM suntey-core.exe >/dev/null 2>&1   # hard kill: leaves Running row
echo "killed mid-run. restarting for recovery..."
./bin/suntey-core.exe serve --port 0 --store /tmp/rec2.db --dir "C:/Users/TuanTuan/Downloads" >/tmp/l6.json 2>&1 &
sleep 1.2
OUT=$(head -1 /tmp/l6.json)
PORT=$(echo "$OUT" | python -c "import sys,json;print(json.load(sys.stdin)['port'])" 2>/dev/null)
TOKEN=$(echo "$OUT" | python -c "import sys,json;print(json.load(sys.stdin)['token'])" 2>/dev/null)
echo "after recovery (5=Running 4=Paused — expect 4):"
curl -s -H "X-SunTey-Token: $TOKEN" http://127.0.0.1:$PORT/api/v1/tasks | python -c "
import sys,json
for t in json.load(sys.stdin)['data']:
    print('  id', t['id'], 'status=', t['status'], 'dl=', t['downloaded'], 'size=', t['size'])"
taskkill //F //IM suntey-core.exe >/dev/null 2>&1
echo KILLED-ALL
