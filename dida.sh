#!/usr/bin/env bash
#
# dida.sh - dida-trade test bench in tmux (servers, clients, console)
#
# Usage:  ./dida.sh [options] <A|B|C>
#         ./dida.sh stop
#
# Options:
#   -c, --clients N     number of clients to start (default 1)
#   -k, --console 0|1   start the console (default 1)
#   -p, --port PORT     base port of server 0 (default 58000)
#   -n, --no-build      skip the maven build
#   -l, --log           also save the output of every pane to ./logs/
#   -d, --detach        create the session but do not attach to it
#   -h, --help          show this help
#
# Examples:
#   ./dida.sh A                 3 servers, 1 client, 1 console
#   ./dida.sh C -c 2            7 servers, 2 clients, 1 console
#   ./dida.sh B -k 0 -n -l      no console, no build, logs saved
#
set -euo pipefail

SESSION="dida"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOST="localhost"

# Module directories: each pane runs `mvn exec:java` inside one of them (see README)
SERVER_DIR="server"
APP_DIR="app"
CONSOLE_DIR="console"
BUILD_CMD="mvn -q clean install -DskipTests"

usage() { sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }
die()   { echo "error: $*" >&2; exit 1; }

# ---- arguments -------------------------------------------------------------------
SCHED=""
CLIENTS=1
CONSOLE=1
PORT=58000
BUILD=1
LOG=0
ATTACH=1

while [[ $# -gt 0 ]]; do
    case "$1" in
        -c|--clients)  [[ $# -ge 2 ]] || die "$1 needs a value"; CLIENTS="$2"; shift 2 ;;
        -k|--console)  [[ $# -ge 2 ]] || die "$1 needs a value"; CONSOLE="$2"; shift 2 ;;
        -p|--port)     [[ $# -ge 2 ]] || die "$1 needs a value"; PORT="$2"; shift 2 ;;
        -n|--no-build) BUILD=0; shift ;;
        -l|--log)      LOG=1; shift ;;
        -d|--detach)   ATTACH=0; shift ;;
        -h|--help)     usage; exit 0 ;;
        stop)          if tmux kill-session -t "$SESSION" 2>/dev/null; then
                           echo "session '$SESSION' stopped"
                       else
                           echo "no session '$SESSION' running"
                       fi
                       exit 0 ;;
        A|B|C|a|b|c)   SCHED="${1^^}"; shift ;;
        *)             usage; die "unknown argument: $1" ;;
    esac
done

[[ -n "$SCHED" ]] || { usage; die "missing schedule (A, B or C)"; }
[[ "$CLIENTS" =~ ^[0-9]+$ ]] && (( CLIENTS <= 99 )) || die "clients must be a number between 0 and 99"
[[ "$CONSOLE" == "0" || "$CONSOLE" == "1" ]] || die "console must be 0 or 1"
[[ "$PORT" =~ ^[0-9]+$ ]] || die "port must be a number"

# ---- checks ------------------------------------------------------------------------
command -v tmux >/dev/null || die "tmux is not installed (e.g. sudo apt install tmux)"
command -v mvn  >/dev/null || die "mvn not found (if you use setup_env.sh: source <INSTALL_DIR>/env.sh first)"
if (( BUILD )) && [[ ! -f "$ROOT/contract/pom.xml" ]]; then
    die "contract/pom.xml is missing: copy contract/intel-pom.xml (or arm-pom.xml) to contract/pom.xml"
fi

case "$SCHED" in
    C) N=7 ;;   # schedule C uses 7 servers
    *) N=3 ;;   # schedules A and B use 3 servers
esac

# tmux panes may not inherit the PATH/JAVA_HOME of this shell (e.g. if a tmux server was
# already running), so every command re-exports them.
ENV_PREFIX="export PATH='$PATH'"
[[ -n "${JAVA_HOME:-}" ]] && ENV_PREFIX+=" JAVA_HOME='$JAVA_HOME'"
ENV_PREFIX+="; "

# ---- helpers ---------------------------------------------------------------------------
# Command typed into a pane, same as the README: `mvn exec:java` in a module directory.
run_cmd() {   # module-dir program-args
    echo "${ENV_PREFIX}cd '$ROOT/$1' && mvn -q exec:java -Dexec.args=\"$2\""
}

launch() { tmux send-keys -t "$1" "$2" C-m; }   # pane command

log_pane() {   # pane name
    if (( LOG )); then
        mkdir -p "$ROOT/logs"
        : > "$ROOT/logs/$2.log"
        tmux pipe-pane -t "$1" "cat >> '$ROOT/logs/$2.log'"
    fi
    return 0
}

new_pane() {   # window title -> prints the new pane id
    local id
    id=$(tmux split-window -d -t "$1" -P -F '#{pane_id}')
    tmux select-layout -t "$1" tiled >/dev/null
    tmux select-pane -t "$id" -T "$2"
    echo "$id"
}

style_window() {
    tmux set-option -w -t "$1" pane-border-status top
    tmux set-option -w -t "$1" pane-border-format " #{pane_title} "
    tmux set-option -w -t "$1" allow-set-title off 2>/dev/null || true
}

wait_port() {   # wait until a server accepts connections (max ~60 s)
    local port=$1 tries=0
    until (exec 3<>"/dev/tcp/$HOST/$port") 2>/dev/null; do
        sleep 0.5
        tries=$((tries + 1))
        if (( tries > 120 )); then
            echo "  warning: nothing listening on port $port after 60 s" >&2
            return 0
        fi
    done
}

start_server() {   # server-id
    log_pane "${SERVER_PANES[$1]}" "s$1"
    launch "${SERVER_PANES[$1]}" "$(run_cmd "$SERVER_DIR" "$PORT $1 $SCHED")"
}

# ---- build ---------------------------------------------------------------------------------
tmux kill-session -t "$SESSION" 2>/dev/null || true

if (( BUILD )); then
    echo "Building (use -n to skip)..."
    (cd "$ROOT" && $BUILD_CMD)
fi

# ---- window "servers": one tiled pane per server ------------------------------------------------
first=$(tmux new-session -d -s "$SESSION" -n servers -x 240 -y 60 -c "$ROOT" -P -F '#{pane_id}')
tmux set-option -t "$SESSION" mouse on
tmux set-option -t "$SESSION" status-right-length 90
tmux set-option -t "$SESSION" status-right "C-b n/p: switch window | C-b z: zoom pane | C-b d: detach | schedule $SCHED"
tmux select-pane -t "$first" -T "S0"
SERVER_PANES=("$first")
for ((i = 1; i < N; i++)); do
    SERVER_PANES+=("$(new_pane "$SESSION:servers" "S$i")")
done
tmux select-layout -t "$SESSION:servers" tiled >/dev/null
style_window "$SESSION:servers"

# ---- window "clients": console on the left, clients stacked on the right --------------------------
titles=()
if (( CONSOLE )); then titles+=("console"); fi
for ((c = 1; c <= CLIENTS; c++)); do titles+=("client $c"); done

CTRL_PANES=()
if (( ${#titles[@]} > 0 )); then
    pane=$(tmux new-window -d -t "$SESSION" -n clients -c "$ROOT" -P -F '#{pane_id}')
    tmux select-pane -t "$pane" -T "${titles[0]}"
    CTRL_PANES+=("$pane")
    for ((k = 1; k < ${#titles[@]}; k++)); do
        CTRL_PANES+=("$(new_pane "$SESSION:clients" "${titles[$k]}")")
    done
    if (( CONSOLE && CLIENTS > 0 )); then
        tmux select-layout -t "$SESSION:clients" main-vertical >/dev/null
    else
        tmux select-layout -t "$SESSION:clients" tiled >/dev/null
    fi
    style_window "$SESSION:clients"
fi

# ---- start the processes ------------------------------------------------------------------------------
# Servers 1..N-1 first (in parallel), server 0 (leader of ballot 0) last, so nobody misses
# the first messages of a leader that started before them.
echo "Starting servers (schedule $SCHED, $N servers, base port $PORT)..."
for ((i = N - 1; i >= 1; i--)); do start_server "$i"; done
for ((i = N - 1; i >= 1; i--)); do wait_port $((PORT + i)); done
start_server 0
wait_port "$PORT"
echo "Servers are up."

k=0
if (( CONSOLE )); then
    log_pane "${CTRL_PANES[0]}" "console"
    launch "${CTRL_PANES[0]}" "$(run_cmd "$CONSOLE_DIR" "$HOST $PORT $SCHED")"
    k=1
fi
for ((c = 1; c <= CLIENTS; c++)); do
    log_pane "${CTRL_PANES[$k]}" "client$c"
    launch "${CTRL_PANES[$k]}" "$(run_cmd "$APP_DIR" "$c $HOST $PORT $SCHED")"
    k=$((k + 1))
done

# open on the window where you type commands
if (( ${#titles[@]} > 0 )); then
    tmux select-window -t "$SESSION:clients"
else
    tmux select-window -t "$SESSION:servers"
fi

echo "Session '$SESSION' ready. Stop everything with: ./dida.sh stop"
if (( ATTACH )); then
    if [[ -n "${TMUX:-}" ]]; then
        tmux switch-client -t "$SESSION"
    else
        tmux attach-session -t "$SESSION"
    fi
fi
