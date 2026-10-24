# STOMP Client-Server: World Cup Updates

A multi-threaded client-server system for sharing live World Cup game updates over the **STOMP 1.2** protocol. Users subscribe to a game channel, report game events from a JSON file, and get a summary of everything that was reported.

I built it together with two partners in the Systems Programming course at Ben-Gurion University.

| Part | Language | What it does |
|---|---|---|
| `server/` | Java | STOMP server: Thread-Per-Client or Reactor mode |
| `client/` | C++ | Command-line client with two threads (keyboard and socket) |
| `data/` | Python | SQL server that stores users, logins and reports in SQLite |

```
C++ client  ──TCP/STOMP──>  Java STOMP server  ──TCP──>  Python SQL server (SQLite)
```

---

## Server (Java)

- **Two server modes**, chosen at startup:
  - **Thread-Per-Client (`tpc`)**: each connection gets its own thread and a blocking handler
  - **Reactor (`reactor`)**: one selector thread with non-blocking I/O, and the messages are processed by a thread pool (`ActorThreadPool`), which keeps the messages of each client in order
- **STOMP protocol** (`StompMsgProtocol`): handles `CONNECT`, `SUBSCRIBE`, `UNSUBSCRIBE`, `SEND` and `DISCONNECT`, and answers with `CONNECTED`, `MESSAGE`, `RECEIPT` or `ERROR`
- **Encoder/decoder** (`StompMsgEncDec`): splits the byte stream into frames on the null character
- **Connections manager**: maps each connection ID to its handler and sends messages to clients
- **Subscription manager**: keeps the subscriptions by channel and by connection in `ConcurrentHashMap`s, so several threads can use it safely
- **Database layer**: sends SQL commands to the Python SQL server over a socket, and prints a report of users, login history and uploaded files when the server shuts down

## Client (C++)

- **Two threads**: one reads commands from the keyboard, the other listens to the server. Shared data (receipts, game updates) is protected with `std::mutex`, and a `condition_variable` wakes the listening thread after login
- Builds STOMP frames for each command and handles the server's responses (receipts, messages, errors)
- Parses game events from JSON (`nlohmann/json`), keeps the stats of every game per user and writes a summary file

Commands:

| Command | STOMP frame | Description |
|---|---|---|
| `login {host:port} {user} {password}` | `CONNECT` | Connect to the server |
| `join {game}` | `SUBSCRIBE` | Subscribe to a game channel |
| `exit {game}` | `UNSUBSCRIBE` | Unsubscribe from a game channel |
| `report {file.json}` | `SEND` | Send all the events in the file to the game channel |
| `summary {game} {user} {file}` | - | Write a summary of the events a user reported |
| `logout` | `DISCONNECT` | Disconnect and wait for the receipt |

## SQL server (Python)

A small multi-threaded TCP server that runs the SQL commands it receives on a SQLite database with three tables: `users`, `login_history` and `file_tracking`. Access to the database is protected with a lock.

---

## Build and run

**1. SQL server**
```bash
python3 data/sql_server.py
```

**2. STOMP server** (Java 8+, Maven)
```bash
cd server
mvn compile
mvn exec:java -Dexec.mainClass="bgu.spl.net.impl.stomp.StompServer" -Dexec.args="7777 tpc"
# or "7777 reactor"
```

**3. Client** (C++11, Boost)
```bash
cd client
make
./bin/StompWCIClient
```

Example session:
```
login 127.0.0.1:7777 alice 1234
join Germany_Japan
report data/events1.json
summary Germany_Japan alice summary.txt
logout
```

The game channel name is built from the two team names in the JSON file (`{team a}_{team b}`).
