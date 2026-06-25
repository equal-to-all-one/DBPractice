#!/usr/bin/env python3
import os, shutil, socket, subprocess, time

BUILD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "build")
DB_NAME = "orderby_test_db"
DB_DIR = os.path.join(BUILD_DIR, DB_NAME)
RMDB = os.path.join(BUILD_DIR, "bin", "rmdb")
PORT = 8765

queries = [
    "create table orders (company char(10), order_number int)",
    "insert into orders values('AAA',12)",
    "insert into orders values('ABB',13)",
    "insert into orders values('ABC',19)",
    "insert into orders values('ACA',1)",
    "SELECT company, order_number FROM orders ORDER BY order_number",
    "SELECT company, order_number FROM orders ORDER BY company, order_number",
    "SELECT company, order_number FROM orders ORDER BY company DESC, order_number ASC",
    "SELECT company, order_number FROM orders ORDER BY order_number ASC LIMIT 2",
]

if os.path.isdir(DB_DIR):
    shutil.rmtree(DB_DIR)
server = subprocess.Popen([RMDB, DB_NAME], cwd=BUILD_DIR, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(1)
try:
    sock = socket.socket()
    sock.settimeout(5)
    sock.connect(("127.0.0.1", PORT))
    for q in queries:
        msg = q if q.endswith(";") else q + ";"
        sock.sendall(msg.encode() + b"\0")
        r = b""
        while b"\0" not in r:
            r += sock.recv(4096)
    sock.close()
finally:
    server.terminate()
    server.wait(timeout=3)

expected = """| company | order_number |
| ACA | 1 |
| AAA | 12 |
| ABB | 13 |
| ABC | 19 |
| company | order_number |
| AAA | 12 |
| ABB | 13 |
| ABC | 19 |
| ACA | 1 |
| company | order_number |
| ACA | 1 |
| ABC | 19 |
| ABB | 13 |
| AAA | 12 |
| company | order_number |
| ACA | 1 |
| AAA | 12 |
"""

with open(os.path.join(DB_DIR, "output.txt")) as f:
    got = f.read()

print(got)
print("MATCH" if got.strip() == expected.strip() else "MISMATCH")
