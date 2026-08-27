# CWebsocketClient

A basic C++ websocket client built on [Boost.Beast](https://www.boost.org/doc/libs/release/libs/beast/), supporting plain (`ws://`) and TLS (`wss://`) connections, synchronous and asynchronous connects, text and binary payloads, and event callbacks.

## Features

- **Plain and TLS connections** — pass an `sslcontext::ISslContextPtr` in the settings to establish a `wss://` connection (with SNI and certificate verification via the system CA paths).
- **Sync and async connect** — `Connect(...)` blocks until the handshake completes (bounded by `handshakeTimeoutS`); `AsyncConnect(...)` returns immediately and fires the connect callback on success.
- **Text and binary payloads** — `SendMessage` sends text frames, `SendContent` sends binary frames. The `shared_ptr` overload of `SendContent` avoids copying the payload. Sends are queued and written in order.
- **Callbacks** — register connect, disconnect, text-message, and binary-content callbacks. Callbacks are invoked from IO threads; exceptions thrown by callbacks are caught and logged.
- **Keep-alive / idle timeout** — with `enablePings` set (the default), a ping is sent after the connection has been idle for `idleTimeoutS / 2`, and the connection is dropped (with the disconnect callback fired) after `idleTimeoutS` of silence.
- **Threaded IO** — the client runs its own `io_context` on `(numThreads + 1) * 2` threads.

## Layout

```
src/CWebsocketClient.h                 Public client API
src/ISslContext.h                      SSL context abstraction (+ default TLS client impl)
src/*.cpp                              Implementation (Boost.Beast, pimpl)
examples/echo_client.cpp               Small demo program
tests/websocketclient_test.cpp         Loopback echo-server tests (Boost.Test)
tests/websocketclient_stress_test.cpp  Concurrency + lifecycle stress tests (Boost.Test)
tests/test_helpers.h                   Shared test servers (echo + silent)
```

## Building

Requires CMake ≥ 3.16, a C++17 compiler, Boost ≥ 1.71 (headers), and OpenSSL.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure   # run the tests
```

Tests are written with Boost.Test (the header-only "included" variant, so no
extra Boost libraries are needed) and register with CTest; run them via
`ctest` or directly (e.g. `./websocketclient_stress_test --log_level=test_suite`,
`--run_test=ConcurrentSends` for a single case).

The stress suite (`websocketclient_stress_test`) covers concurrent sends from
many threads, Send/Close races, client destruction while sends are queued,
while a connect is in progress, and while callbacks are executing, plus
close-during-connect, bounded synchronous-connect timeouts, reconnect cycles,
and throwing callbacks. It runs clean under ThreadSanitizer and
AddressSanitizer/LeakSanitizer/UBSan, e.g.:

```sh
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DCMAKE_CXX_FLAGS="-fsanitize=thread -g" \
      -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread"
cmake --build build-tsan -j && ctest --test-dir build-tsan --output-on-failure
```

## Usage

```cpp
#include "CWebsocketClient.h"

using namespace websocketclient;

CWebsocketClient client("my_client");

client.RegisterConnectCallback([] { std::cout << "connected\n"; });
client.RegisterDisconnectCallback([] { std::cout << "disconnected\n"; });
client.RegisterMessageCallback([](const std::string& msg) { std::cout << msg << "\n"; });
client.RegisterContentCallback([](const std::vector<uint8_t>& data) { /* binary */ });

if (client.Connect("127.0.0.1", 8080, "/server/path/to/websocket"))
{
    client.SendMessage("hello");
    client.SendContent({0xDE, 0xAD, 0xBE, 0xEF});
}
client.Close();
```

TLS:

```cpp
CWebsocketClient::CClientSettings settings;
settings.sslContext = sslcontext::MakeSslContext(/*verifyPeer=*/true);
CWebsocketClient client("secure_client", settings);
client.Connect("example.com", 443, "/ws");
```

## Notes

- `Connect`/`AsyncConnect` may be called again after a disconnect; each call establishes a fresh session (an existing session is closed first).
- `Close()` is idempotent and flushes queued writes before initiating the websocket closing handshake.
- Do not call the blocking `Connect(...)` from inside a registered callback (callbacks run on IO threads and the blocking connect would deadlock); use `AsyncConnect(...)` there instead.
