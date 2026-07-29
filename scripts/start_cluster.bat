@echo off
REM ================================================================
REM  Raft 3-Node Cluster Startup Script (Windows)
REM
REM  Starts 3 kv_raft nodes with Raft consensus on ports 8001-8003
REM  and Redis-compatible RESP servers on ports 6379-6381.
REM ================================================================

setlocal
set BINARY=..\build_sweep\kv_raft.exe

if not exist "%BINARY%" (
    echo ERROR: kv_raft.exe not found. Run: cmake --build build_sweep --target kv_raft
    exit /b 1
)

REM Create data directories
if not exist "cluster\node1" mkdir "cluster\node1"
if not exist "cluster\node2" mkdir "cluster\node2"
if not exist "cluster\node3" mkdir "cluster\node3"

echo ========================================================
echo   Starting 3-Node Raft KV Cluster
echo ========================================================
echo.
echo   Node 1: raft=8001, resp=6379, metrics=9091
echo   Node 2: raft=8002, resp=6380, metrics=9092
echo   Node 3: raft=8003, resp=6381, metrics=9093
echo.
echo   Press Ctrl+C in each window to stop.
echo ========================================================
echo.

REM Start Node 1
start "KV Raft Node 1" cmd /c "%BINARY% --id node1 --raft-port 8001 --resp-port 6379 --metrics-port 9091 --peer node1:127.0.0.1:8001 --peer node2:127.0.0.1:8002 --peer node3:127.0.0.1:8003 --data-dir cluster\node1"

REM Start Node 2
start "KV Raft Node 2" cmd /c "%BINARY% --id node2 --raft-port 8002 --resp-port 6380 --metrics-port 9092 --peer node1:127.0.0.1:8001 --peer node2:127.0.0.1:8002 --peer node3:127.0.0.1:8003 --data-dir cluster\node2"

REM Start Node 3
start "KV Raft Node 3" cmd /c "%BINARY% --id node3 --raft-port 8003 --resp-port 6381 --metrics-port 9093 --peer node1:127.0.0.1:8001 --peer node2:127.0.0.1:8002 --peer node3:127.0.0.1:8003 --data-dir cluster\node3"

echo All 3 nodes started in separate windows.
echo.
echo Test with:
echo   redis-cli -h 127.0.0.1 -p 6379 PING
echo   redis-cli -h 127.0.0.1 -p 6379 SET hello world
echo   redis-cli -h 127.0.0.1 -p 6380 GET hello
echo.
echo Metrics:
echo   http://127.0.0.1:9091/metrics
echo   http://127.0.0.1:9092/metrics
echo   http://127.0.0.1:9093/metrics
echo.

endlocal