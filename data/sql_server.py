#!/usr/bin/env python3
"""
SQL server for the STOMP project.

Receives SQL commands from the Java STOMP server over a TCP socket
(null-terminated strings), runs them on a local SQLite database and
sends back the result. Tracks users, login history and uploaded reports.
"""

import socket
import sys
import threading
import sqlite3


SERVER_NAME = "STOMP_PYTHON_SQL_SERVER"
DB_FILE = "stomp_server.db"

_conn = sqlite3.connect(DB_FILE,check_same_thread=False)
_db_lock = threading.Lock()

def recv_null_terminated(sock: socket.socket) -> str:
    data = b""
    while True:
        chunk = sock.recv(1024)
        if not chunk:
            return ""
        data += chunk
        if b"\0" in data:
            msg, _ = data.split(b"\0", 1)
            return msg.decode("utf-8", errors="replace")

def close():
    with _db_lock:
        _conn.commit() 
        _conn.close()

def init_database():
    with _db_lock:
        _conn.executescript(""" DROP TABLE IF EXISTS users;
            CREATE TABLE users (
            username TEXT PRIMARY KEY,
            password TEXT NOT NULL,
            registration_date TEXT NOT NULL);
            
            DROP TABLE IF EXISTS login_history;
            CREATE TABLE login_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            login_time TEXT NOT NULL,
            logout_time TEXT,
            FOREIGN KEY(username) REFERENCES users(username));

            DROP TABLE IF EXISTS file_tracking;        
            CREATE TABLE file_tracking (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            filename TEXT NOT NULL,
            upload_time TEXT NOT NULL,
            game_channel TEXT,
            FOREIGN KEY(username) REFERENCES users(username));""")
        _conn.commit()


def execute_sql_command(sql_command: str) -> str:
    try: 
        with _db_lock:
            _conn.execute(sql_command)
            _conn.commit()
            return "SUCCESS"
    except Exception as e:
        print(f"ERROR:Couldnot execute command:{e}")
        return f"ERROR:Couldnot execute command:{e}"


def execute_sql_query(sql_query: str) -> str:
    try:
        out = ""
        with _db_lock:
            cur = _conn.cursor()
            cur.execute(sql_query)
            out = cur.fetchall()

        res = "SUCCESS"
        for row in out:
            res = res + "|" + str(row) +"|"
        return res
    except Exception as e:
        print(f"ERROR:Couldnot execute query:{e}")
        return f"ERROR:Couldnot execute query:{e}"
    
#Method that differ between sql command and sql queries, get the message from the client and directs it.
def direct_message(msg : str) ->str:
    temp = msg.strip().upper()
    if(temp.startswith("SELECT")):
        return execute_sql_query(msg)
    else:
        return execute_sql_command(msg)
    
def handle_client(client_socket: socket.socket, addr):
    print(f"[{SERVER_NAME}] Client connected from {addr}")

    try:
        while True:
            message = recv_null_terminated(client_socket)
            if message == "":
                break

            print(f"[{SERVER_NAME}] Received:")
            print(message)

            output = direct_message(message)
            client_socket.sendall((output + "\0").encode("utf-8"))

    except Exception as e:
        print(f"[{SERVER_NAME}] Error handling client {addr}: {e}")
    finally:
        try:
            client_socket.close()
        except Exception:
            pass
        print(f"[{SERVER_NAME}] Client {addr} disconnected")


def start_server(host="127.0.0.1", port=7778):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_socket.bind((host, port))
        server_socket.listen(5)
        print(f"[{SERVER_NAME}] Server started on {host}:{port}")
        print(f"[{SERVER_NAME}] Waiting for connections...")

        while True:
            client_socket, addr = server_socket.accept()
            t = threading.Thread(
                target=handle_client,
                args=(client_socket, addr),
                daemon=True
            )
            t.start()

    except KeyboardInterrupt:
        print(f"\n[{SERVER_NAME}] Shutting down server...")
        close()
    finally:
        try:
            server_socket.close()
        except Exception:
            pass


if __name__ == "__main__":
    port = 7778
    if len(sys.argv) > 1:
        raw_port = sys.argv[1].strip()
        try:
            port = int(raw_port)
        except ValueError:
            print(f"Invalid port '{raw_port}', falling back to default {port}")

    init_database()
    start_server(port=port)
