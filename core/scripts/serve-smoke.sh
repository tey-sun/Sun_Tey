#!/usr/bin/env bash
set -x
cd "H:/软件编写/gopeed/suntey/core"
rm -f /tmp/serve-test.db /tmp/l2.json
./bin/suntey-core.exe serve --port 0 --store /tmp/serve-test.db --dir "C:/Users/TuanTuan/Downloads" >/tmp/l2.json 2>&1 &
SPID=$!
sleep 1.5
echo "PID=$SPID file-exists=$(test -f /tmp/l2.json && echo yes || echo no)"
head -2 /tmp/l2.json
