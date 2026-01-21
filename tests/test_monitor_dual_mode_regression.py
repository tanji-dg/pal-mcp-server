import asyncio
import os
import signal
import socket
import subprocess
import sys
import time
import httpx
import pytest

# Constants for the test
TEST_PORT = 9877  # Use a different port to avoid conflict with running instance
TEST_SOCKET = "/tmp/pal-monitor-test-dual.sock"

def is_port_open(host, port):
    """Check if a TCP port is open."""
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False

def is_socket_open(socket_path):
    """Check if a Unix socket is listening."""
    if not os.path.exists(socket_path):
        return False
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            sock.connect(socket_path)
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False

@pytest.mark.asyncio
async def test_monitor_dual_mode_startup():
    """
    Regression test for Monitor Coordinator Dual Mode.
    
    Verifies that running 'python monitor/run_coordinator.py --dual'
    starts servers on BOTH the specified TCP port AND the Unix socket.
    """
    # Cleanup previous run artifacts
    if os.path.exists(TEST_SOCKET):
        os.unlink(TEST_SOCKET)

    # Command to run the coordinator in dual mode
    cmd = [
        sys.executable,
        "monitor/run_coordinator.py",
        "--dual",
        "--socket", TEST_SOCKET,
        "--port", str(TEST_PORT),
        "--host", "127.0.0.1",
        "--log-level", "DEBUG"
    ]

    print(f"\nStarting coordinator with command: {' '.join(cmd)}")
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    try:
        # Wait for startup (max 10 seconds)
        start_time = time.time()
        http_ready = False
        unix_ready = False
        
        while time.time() - start_time < 10:
            if not http_ready and is_port_open("127.0.0.1", TEST_PORT):
                print(f"HTTP port {TEST_PORT} is open.")
                http_ready = True
            
            if not unix_ready and is_socket_open(TEST_SOCKET):
                print(f"Unix socket {TEST_SOCKET} is open.")
                unix_ready = True
            
            if http_ready and unix_ready:
                break
            
            # Check if process died
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                print(f"Process died unexpectedly with code {process.returncode}")
                print(f"STDOUT:\n{stdout}")
                print(f"STDERR:\n{stderr}")
                pytest.fail("Coordinator process died during startup")
            
            await asyncio.sleep(0.5)

        # Assertions
        if not http_ready:
            # Check logs if failed
            process.terminate()
            stdout, stderr = process.communicate()
            print(f"STDOUT:\n{stdout}")
            print(f"STDERR:\n{stderr}")
            pytest.fail(f"HTTP port {TEST_PORT} did not open in dual mode")

        if not unix_ready:
            process.terminate()
            stdout, stderr = process.communicate()
            print(f"STDOUT:\n{stdout}")
            print(f"STDERR:\n{stderr}")
            pytest.fail(f"Unix socket {TEST_SOCKET} did not open in dual mode")

        # Verify endpoints respond
        async with httpx.AsyncClient() as client:
            # Check HTTP health
            try:
                resp = await client.get(f"http://127.0.0.1:{TEST_PORT}/health")
                assert resp.status_code == 200, f"HTTP /health returned {resp.status_code}"
                print("HTTP /health check passed")
            except Exception as e:
                pytest.fail(f"HTTP request failed: {e}")

            # Check Unix socket health (via HTTP over Unix socket)
            # httpx doesn't natively support unix socket URLs easily in this context without transport adapter,
            # but we verified the socket exists and connects.
            # We can skip complex unix http check if socket connect check passed.

    finally:
        # Cleanup
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
        
        if os.path.exists(TEST_SOCKET):
            os.unlink(TEST_SOCKET)
