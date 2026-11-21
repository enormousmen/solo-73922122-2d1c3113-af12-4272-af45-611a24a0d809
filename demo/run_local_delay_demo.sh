#!/usr/bin/env bash
# Local delay-broadcast demo without Docker.
#
# Starts the relay in delay mode (DELAY_SECONDS, default 8 s), a playout-side
# sink, a clean dual-path source and the operator console. The operator lists
# pending queue ids / RTP seqs, retracts RTP seqs 3,4,5 while they are still
# held, demonstrates the rejection paths, and waits for the queue to drain.
# The sink report then proves those packets never reached the playout machine.
#
# Override timing for a quicker local run:
#   DELAY_SECONDS=1 ./demo/run_local_delay_demo.sh
set -euo pipefail

cd "$(dirname "$0")/.."

API_PORT="${API_PORT:-8000}"
PORT_A="${UDP_PORT_A:-9100}"
PORT_B="${UDP_PORT_B:-9101}"
FWD_PORT="${FORWARD_PORT:-9200}"
DELAY_SECONDS="${DELAY_SECONDS:-8}"
# The operator must act while packets are still inside the hold.
WAIT_PACKED="$(python3 -c "print(min(1.5, max(0.3, ${DELAY_SECONDS} * 0.25)))")"
SINK_DURATION="$(python3 -c "print(${DELAY_SECONDS} + 6.0)")"
LOG_DIR="$(mktemp -d)"
echo "[delay-demo] logs: $LOG_DIR (hold = ${DELAY_SECONDS}s)"

check_port_free() {
  local port="$1"
  if ! python3 - "$port" <<'PY' 2>/dev/null
import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
try:
    s.bind(("127.0.0.1", int(sys.argv[1])))
except OSError:
    sys.exit(1)
finally:
    s.close()
PY
  then
    echo "[delay-demo] ERROR: UDP port $port is already in use. Free it or rerun" \
         "with FORWARD_PORT/UDP_PORT_A/UDP_PORT_B overrides." >&2
    exit 1
  fi
}
check_port_free "$PORT_A"
check_port_free "$PORT_B"
check_port_free "$FWD_PORT"

cleanup() {
  trap - EXIT INT TERM
  kill -TERM 0 2>/dev/null || true
  sleep 0.3
  kill -KILL 0 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "[delay-demo] starting relay with DELAY_ENABLED=true (hold ${DELAY_SECONDS}s)"
API_PORT="$API_PORT" UDP_PORT_A="$PORT_A" UDP_PORT_B="$PORT_B" \
  FORWARD_HOST=127.0.0.1 FORWARD_PORT="$FWD_PORT" \
  DELAY_ENABLED=true DELAY_SECONDS="$DELAY_SECONDS" \
  python3 -u -m app.main >"$LOG_DIR/relay.log" 2>&1 &
RELAY_PID=$!

for _ in $(seq 1 50); do
  if curl -sf "http://127.0.0.1:$API_PORT/health" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$RELAY_PID" 2>/dev/null; then
    echo "[delay-demo] relay died early:"; cat "$LOG_DIR/relay.log"; exit 1
  fi
  sleep 0.1
done

echo "[delay-demo] starting sink (playout machine stand-in)"
python3 -u -m demo.demo_sink --host 127.0.0.1 --port "$FWD_PORT" \
  --duration "$SINK_DURATION" --api-url "http://127.0.0.1:$API_PORT" \
  >"$LOG_DIR/sink.log" 2>&1 &
SINK_PID=$!

sleep 0.3
echo "[delay-demo] starting clean dual-path source (20 pps, 2 s, no loss)"
python3 -u -m demo.demo_source --host 127.0.0.1 \
  --port-a "$PORT_A" --port-b "$PORT_B" \
  --duration 2 --packets-per-sec 20 --start-seq 0 \
  >"$LOG_DIR/source.log" 2>&1

echo "[delay-demo] starting operator console (retracts 3 pending packets)"
set +e
python3 -u -m demo.demo_operator --api-url "http://127.0.0.1:$API_PORT" \
  --retract-count 3 --wait-packed "$WAIT_PACKED" \
  --drain-timeout "$(python3 -c "print(${DELAY_SECONDS} + 4.0)")" \
  2>&1 | tee "$LOG_DIR/operator.log"
OPERATOR_RC=${PIPESTATUS[0]}
set -e

wait "$SINK_PID"

echo
echo "================ sink report ================"
cat "$LOG_DIR/sink.log"
echo "================ relay /status ==============="
curl -s "http://127.0.0.1:$API_PORT/status"; echo
echo "================ relay /pending (should be empty) ==============="
curl -s "http://127.0.0.1:$API_PORT/pending"; echo
echo "================ relay log tail =============="
tail -n 8 "$LOG_DIR/relay.log"
exit "$OPERATOR_RC"
