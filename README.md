# CWebsocketClient / CWebsocketServer

A basic C++ websocket client and server built on [Boost.Beast](https://www.boost.org/doc/libs/release/libs/beast/), supporting plain (`ws://`) and TLS (`wss://`) connections, text and binary payloads, and event callbacks.

## Client features

- **Plain and TLS connections** — pass an `sslcontext::ISslContextPtr` in the settings to establish a `wss://` connection (with SNI and certificate verification via the system CA paths).
- **Sync and async connect** — `Connect(...)` blocks until the handshake completes (bounded by `handshakeTimeoutS`); `AsyncConnect(...)` returns immediately and fires the connect callback on success.
- **Text and binary payloads** — `SendMessage` sends text frames, `SendContent` sends binary frames. The `shared_ptr` overload of `SendContent` avoids copying the payload. Sends are queued and written in order.
- **Callbacks** — register connect, disconnect, text-message, and binary-content callbacks. Callbacks are invoked from IO threads; exceptions thrown by callbacks are caught and logged.
- **Keep-alive / idle timeout** — with `enablePings` set (the default), a ping is sent after the connection has been idle for `idleTimeoutS / 2`, and the connection is dropped (with the disconnect callback fired) after `idleTimeoutS` of silence.
- **Threaded IO** — the client runs its own `io_context` on `(numThreads + 1) * 2` threads.

## Server features

- **Websocket + HTTP on one port** — websocket upgrade requests become tracked clients (each with a unique `clientId`); plain HTTP requests (GET/POST/...) on registered URIs are delivered as `HttpMsg` through `CHttpWrapper`, which auto-replies "400 Bad Request" if the handler never responds. Unmatched HTTP paths get a standard 404 JSON error.
- **Plain and TLS** — with an `sslContext` set, `sslRequired` either enforces TLS or auto-detects TLS vs plain per connection (the added callback reports `isSsl`).
- **URI routing** — `AddUri`/`RemoveUri` (or the RAII `CUriWrapper`) register per-URI callbacks (messages, content, HTTP requests, connection-count changes) with path-segment prefix matching; `allowedUris` optionally filters which targets may upgrade.
- **Sends** — per-client, broadcast to all, or broadcast per URI; text and binary; delayed-eval overloads build the payload only if a matching client is connected. Broadcast payloads are shared, not copied per client. A per-session backlog cap (`maxSessionBacklog`, default `StandardMaxOutstandingWrites`) drops payloads to slow consumers instead of buffering unboundedly.
- **Serialized callbacks** — all callbacks are delivered on one dedicated workqueue thread and never run concurrently, except the closed/connection-change callbacks during `Stop()`/`RemoveClient()`, which run in the calling thread (per the header contract).

## Shared IO pool

By default every client and server owns its IO threads. When a process hosts many
instances, give them one `CIoPool` instead:

```cpp
#include "CIoPool.h"

auto pool = std::make_shared<CIoPool>(4); // e.g. sized to the core count

CWebsocketClient::CClientSettings clientSettings;
clientSettings.ioPool = pool;
CWebsocketServer::CServerSettings serverSettings;
serverSettings.ioPool = pool;
// every instance created with these settings shares the 4 IO threads
```

With `ioPool` set, `numThreads` is ignored and the instance holds a reference to the
pool for its lifetime, so the pool outlives it under normal destruction order.
Callbacks are still delivered on each instance's own single-threaded workqueue
(never on pool threads), so one slow callback cannot stall shared IO — but blocking
inside a callback still delays that instance's later callbacks, and the blocking
`Connect(...)` must not be called from any callback of any instance on the pool.

## Layout

```
src/CIoPool.h                             Shared IO thread pool
src/CWebsocketClient.h                    Public client API
src/CWebsocketServer.h                    Public server API
src/ISslContext.h                         SSL context abstraction (+ default TLS client impl)
src/*.cpp                                 Implementation (Boost.Beast, pimpl)
examples/echo_client.cpp                  Small client demo
examples/echo_server.cpp                  Small server demo (websocket echo + http endpoint)
tests/websocketclient_test.cpp            Client loopback tests (Boost.Test)
tests/websocketclient_stress_test.cpp     Client concurrency + lifecycle stress tests
tests/websocketserver_test.cpp            Server functional tests (uses CWebsocketClient as peer)
tests/websocketserver_stress_test.cpp     Server concurrency + lifecycle stress tests
tests/websocketpool_test.cpp              Shared io-pool tests (client + server on one pool)
tests/websockettls_test.cpp               TLS wire tests (in-process self-signed certificate)
tests/test_helpers.h                      Shared test servers (echo + silent)
tests/tls_helpers.h                       Self-signed certificate generation + test ssl contexts
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

Server:

```cpp
#include "CWebsocketServer.h"

auto server = std::make_shared<CWebsocketServer>("my_server");

CWebsocketServer::CClientCallbacks callbacks;
callbacks.mOnClientAddedCb = [](uint32_t id, const std::string& uriAppend, bool isSsl) { /* ... */ };
callbacks.mOnClientMessageReceivedCb = [&](uint32_t id, const std::string& msg) {
    server->SendMessage(id, msg); // echo
};

CWebsocketServer::CUriCallbacks apiCallbacks;
apiCallbacks.mOnRequestReceivedCb = [](const CWebsocketServer::CHttpWrapperPtr& wrapper) {
    auto msg = wrapper->HttpMsg();     // method, uri, queryParameters, body...
    msg->body.assign(...);             // becomes the response body
    wrapper->Reply();                  // unreplied wrappers auto-answer 400
};
server->AddUri("/api", apiCallbacks);

server->Start(8080, callbacks);        // Start(0, ...) picks an ephemeral port, see Port()
server->SendMessage("broadcast to everyone");
server->SendMessage("/api", "just clients under /api");
server->Stop();
```

## Notes

- `Connect`/`AsyncConnect` may be called again after a disconnect; each call establishes a fresh session (an existing session is closed first).
- `Close()` is idempotent and flushes queued writes before initiating the websocket closing handshake.
- Do not call the blocking `Connect(...)` from inside a registered callback (callbacks run on IO threads and the blocking connect would deadlock); use `AsyncConnect(...)` there instead.
- Do not destroy a client or server from inside one of its own callbacks; destruction joins the callback/IO threads.
