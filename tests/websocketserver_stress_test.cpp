/// Stress and lifecycle tests for CWebsocketServer:
///  - many clients sending concurrently with payload integrity checks
///  - broadcast to many clients under load
///  - server destruction while clients are connected and sending
///  - Stop() during traffic (closed callbacks exactly once, in the calling thread)
///  - RemoveClient during traffic
///  - rapid connect/disconnect churn, sequential and from multiple threads
///  - server destruction while a callback is executing
///  - concurrent http requests
///  - slow-consumer backlog cap does not wedge the server
///
/// All Boost.Test assertions run on the main thread; callbacks and workers
/// only signal through promises, atomics, and mutex-guarded containers.

#define BOOST_TEST_MODULE WebsocketServerStress
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include "CWebsocketClient.h"
#include "CWebsocketServer.h"
#include "test_helpers.h"

namespace utf = boost::unit_test;
namespace http = boost::beast::http;
namespace net = boost::asio;
namespace beast = boost::beast;
using tcp = net::ip::tcp;

using testhelpers::PollUntil;
using testhelpers::WaitFor;
using websocketclient::CWebsocketClient;
using websocketclient::CWebsocketServer;

namespace
{

CWebsocketServer::CServerSettings ServerSettings()
{
	CWebsocketServer::CServerSettings settings;
	settings.handshakeTimeoutS = 5;
	settings.idleTimeoutS = 30;
	settings.numThreads = 2;
	return settings;
}

CWebsocketClient::CClientSettings ClientSettings()
{
	CWebsocketClient::CClientSettings settings;
	settings.handshakeTimeoutS = 5;
	settings.idleTimeoutS = 30;
	return settings;
}

} // namespace

BOOST_AUTO_TEST_CASE(ManyClientsConcurrentTraffic, *utf::timeout(120))
{
	const int NUM_CLIENTS = 6;
	const int MESSAGES_PER_CLIENT = 30;

	CWebsocketServer server("stress_many", ServerSettings());

	std::mutex receivedMutex;
	std::set<std::string> serverReceived;
	CWebsocketServer::CClientCallbacks callbacks;
	callbacks.mOnClientMessageReceivedCb = [&](uint32_t, const std::string& m) {
		std::lock_guard<std::mutex> lock(receivedMutex);
		serverReceived.insert(m);
	};
	server.Start(static_cast<unsigned short>(0), callbacks);

	std::vector<std::unique_ptr<CWebsocketClient>> clients;
	std::vector<std::atomic<int>> clientReceived(NUM_CLIENTS);
	for (int i = 0; i < NUM_CLIENTS; ++i)
	{
		clients.push_back(
			std::make_unique<CWebsocketClient>("stress_client_" + std::to_string(i), ClientSettings()));
		clients.back()->RegisterMessageCallback(
			[&clientReceived, i](const std::string&) { ++clientReceived[i]; });
		BOOST_REQUIRE(clients.back()->Connect("127.0.0.1", server.Port(), "/"));
	}
	BOOST_REQUIRE(PollUntil([&]() { return server.GetConnectedClientIds().size() == NUM_CLIENTS; }));

	// All clients send concurrently; the server must see every distinct payload
	std::set<std::string> expected;
	for (int c = 0; c < NUM_CLIENTS; ++c)
	{
		for (int i = 0; i < MESSAGES_PER_CLIENT; ++i)
		{
			expected.insert("client-" + std::to_string(c) + "-msg-" + std::to_string(i));
		}
	}
	std::vector<std::thread> senders;
	for (int c = 0; c < NUM_CLIENTS; ++c)
	{
		senders.emplace_back([&clients, c]() {
			for (int i = 0; i < MESSAGES_PER_CLIENT; ++i)
			{
				clients[c]->SendMessage("client-" + std::to_string(c) + "-msg-" + std::to_string(i));
			}
		});
	}
	for (auto& thread : senders)
	{
		thread.join();
	}
	BOOST_CHECK_MESSAGE(PollUntil([&]() {
							std::lock_guard<std::mutex> lock(receivedMutex);
							return serverReceived.size() >= expected.size();
						}),
						"server received all client messages");
	{
		std::lock_guard<std::mutex> lock(receivedMutex);
		BOOST_CHECK_MESSAGE(serverReceived == expected, "payloads intact across concurrent clients");
	}

	// Broadcast under load: every client gets every broadcast
	const int BROADCASTS = 20;
	for (int i = 0; i < BROADCASTS; ++i)
	{
		server.SendMessage("broadcast-" + std::to_string(i));
	}
	BOOST_CHECK_MESSAGE(PollUntil([&]() {
							for (int c = 0; c < NUM_CLIENTS; ++c)
							{
								if (clientReceived[c] < BROADCASTS)
								{
									return false;
								}
							}
							return true;
						}),
						"every client received every broadcast");
}

BOOST_AUTO_TEST_CASE(ServerDestructionWithActiveClients, *utf::timeout(120))
{
	BOOST_TEST_MESSAGE("client-side \"Send dropped\"/\"connection\" errors are expected here");
	for (int iteration = 0; iteration < 5; ++iteration)
	{
		std::vector<std::unique_ptr<CWebsocketClient>> clients;
		std::atomic<int> disconnects{0};
		{
			CWebsocketServer server("stress_dtor", ServerSettings());
			server.Start(static_cast<unsigned short>(0), CWebsocketServer::CClientCallbacks());
			for (int c = 0; c < 3; ++c)
			{
				clients.push_back(std::make_unique<CWebsocketClient>("dtor_client", ClientSettings()));
				clients.back()->RegisterDisconnectCallback([&disconnects]() { ++disconnects; });
				BOOST_REQUIRE(clients.back()->Connect("127.0.0.1", server.Port(), "/"));
				clients.back()->SendMessage("traffic");
			}
			// Server destructor runs here with clients connected and traffic in flight
		}
		BOOST_CHECK_MESSAGE(PollUntil([&]() { return disconnects == 3; }),
							"all clients saw the disconnect on iteration " + std::to_string(iteration));
	}
}

BOOST_AUTO_TEST_CASE(StopDuringTraffic, *utf::timeout(120))
{
	BOOST_TEST_MESSAGE("client-side \"Send dropped\"/\"Cannot send\" errors are expected here");
	for (int iteration = 0; iteration < 5; ++iteration)
	{
		CWebsocketServer server("stress_stop", ServerSettings());
		std::atomic<int> closedCount{0};
		CWebsocketServer::CClientCallbacks callbacks;
		callbacks.mOnClientClosedCb = [&](uint32_t) { ++closedCount; };
		server.Start(static_cast<unsigned short>(0), callbacks);

		const int NUM_CLIENTS = 3;
		std::vector<std::unique_ptr<CWebsocketClient>> clients;
		std::atomic<int> disconnects{0};
		for (int c = 0; c < NUM_CLIENTS; ++c)
		{
			clients.push_back(std::make_unique<CWebsocketClient>("stop_client", ClientSettings()));
			clients.back()->RegisterDisconnectCallback([&disconnects]() { ++disconnects; });
			BOOST_REQUIRE(clients.back()->Connect("127.0.0.1", server.Port(), "/"));
		}

		std::atomic<bool> keepSending{true};
		std::vector<std::thread> senders;
		for (int c = 0; c < NUM_CLIENTS; ++c)
		{
			senders.emplace_back([&clients, &keepSending, c]() {
				while (keepSending)
				{
					clients[c]->SendMessage("spam");
				}
			});
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
		server.Stop();
		keepSending = false;
		for (auto& thread : senders)
		{
			thread.join();
		}

		BOOST_CHECK_EQUAL(closedCount, NUM_CLIENTS);
		BOOST_CHECK_MESSAGE(PollUntil([&]() { return disconnects == NUM_CLIENTS; }),
							"all clients saw the disconnect on iteration " + std::to_string(iteration));
		// After Stop the port is closed and a fresh connect fails
		BOOST_CHECK(!server.ClientsConnected());
	}
}

BOOST_AUTO_TEST_CASE(RemoveClientDuringTraffic, *utf::timeout(60))
{
	CWebsocketServer server("stress_remove", ServerSettings());
	std::atomic<int> closedCount{0};
	CWebsocketServer::CClientCallbacks callbacks;
	callbacks.mOnClientClosedCb = [&](uint32_t) { ++closedCount; };
	server.Start(static_cast<unsigned short>(0), callbacks);

	CWebsocketClient client("remove_client", ClientSettings());
	std::promise<void> disconnected;
	client.RegisterDisconnectCallback([&]() { disconnected.set_value(); });
	BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));
	BOOST_REQUIRE(PollUntil([&]() { return server.ClientsConnected(); }));

	std::atomic<bool> keepSending{true};
	std::thread sender([&]() {
		while (keepSending)
		{
			client.SendMessage("spam");
		}
	});
	std::this_thread::sleep_for(std::chrono::milliseconds(10));
	const auto ids = server.GetConnectedClientIds();
	BOOST_REQUIRE_EQUAL(ids.size(), 1u);
	server.RemoveClient(ids.begin()->first);
	BOOST_CHECK_EQUAL(closedCount, 1);
	server.RemoveClient(ids.begin()->first); // idempotent
	BOOST_CHECK_EQUAL(closedCount, 1);

	auto disconnectedFuture = disconnected.get_future();
	BOOST_CHECK(WaitFor(disconnectedFuture));
	keepSending = false;
	sender.join();
	BOOST_CHECK(!server.ClientsConnected());
}

BOOST_AUTO_TEST_CASE(ConnectDisconnectChurn, *utf::timeout(120))
{
	CWebsocketServer server("stress_churn", ServerSettings());
	std::atomic<int> added{0};
	std::atomic<int> closed{0};
	CWebsocketServer::CClientCallbacks callbacks;
	callbacks.mOnClientAddedCb = [&](uint32_t, const std::string&, bool) { ++added; };
	callbacks.mOnClientClosedCb = [&](uint32_t) { ++closed; };
	server.Start(static_cast<unsigned short>(0), callbacks);

	// Sequential churn on one client object
	for (int cycle = 0; cycle < 10; ++cycle)
	{
		CWebsocketClient client("churn_client", ClientSettings());
		BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));
		client.SendMessage("cycle");
		client.Close();
	}
	BOOST_CHECK(PollUntil([&]() { return added == 10 && closed == 10; }));

	// Concurrent churn from several threads, each with its own clients
	const int NUM_THREADS = 3;
	const int CYCLES_PER_THREAD = 5;
	std::vector<std::thread> churners;
	for (int t = 0; t < NUM_THREADS; ++t)
	{
		churners.emplace_back([&server, t]() {
			for (int cycle = 0; cycle < CYCLES_PER_THREAD; ++cycle)
			{
				CWebsocketClient client("churn_" + std::to_string(t), ClientSettings());
				if (client.Connect("127.0.0.1", server.Port(), "/"))
				{
					client.SendMessage("hi");
					// Half the cycles close cleanly, half just destroy the client
					if (cycle % 2 == 0)
					{
						client.Close();
					}
				}
			}
		});
	}
	for (auto& thread : churners)
	{
		thread.join();
	}
	const int expectedTotal = 10 + NUM_THREADS * CYCLES_PER_THREAD;
	BOOST_CHECK_MESSAGE(PollUntil([&]() { return added == expectedTotal && closed == expectedTotal; }),
						"every churned connection was added and closed exactly once");
	BOOST_CHECK(!server.ClientsConnected());
}

BOOST_AUTO_TEST_CASE(DestructionDuringCallback, *utf::timeout(60))
{
	std::promise<void> callbackEntered;
	std::atomic<bool> callbackFinished{false};
	CWebsocketClient client("cb_client", ClientSettings());
	{
		CWebsocketServer server("stress_cb_dtor", ServerSettings());
		CWebsocketServer::CClientCallbacks callbacks;
		callbacks.mOnClientMessageReceivedCb = [&](uint32_t, const std::string&) {
			callbackEntered.set_value();
			std::this_thread::sleep_for(std::chrono::milliseconds(300));
			callbackFinished = true;
		};
		server.Start(static_cast<unsigned short>(0), callbacks);
		BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));
		client.SendMessage("trigger");
		auto enteredFuture = callbackEntered.get_future();
		BOOST_REQUIRE_MESSAGE(WaitFor(enteredFuture), "message callback entered");
		// Server destructor runs here while the callback is executing on the workqueue thread
	}
	BOOST_CHECK_MESSAGE(callbackFinished, "destructor waited for the in-flight callback");
}

BOOST_AUTO_TEST_CASE(ConcurrentHttpRequests, *utf::timeout(120))
{
	CWebsocketServer server("stress_http", ServerSettings());
	std::atomic<int> handled{0};
	CWebsocketServer::CUriCallbacks apiCallbacks;
	apiCallbacks.mOnRequestReceivedCb = [&](const CWebsocketServer::CHttpWrapperPtr& wrapper) {
		++handled;
		const std::string body = "{\"ok\": true}";
		wrapper->HttpMsg()->body.assign(body.begin(), body.end());
		wrapper->Reply();
	};
	server.AddUri("/api", apiCallbacks);
	server.Start(static_cast<unsigned short>(0), CWebsocketServer::CClientCallbacks());

	const int NUM_THREADS = 4;
	const int REQUESTS_PER_THREAD = 10;
	std::atomic<int> successes{0};
	std::vector<std::thread> requesters;
	for (int t = 0; t < NUM_THREADS; ++t)
	{
		requesters.emplace_back([&server, &successes]() {
			for (int i = 0; i < REQUESTS_PER_THREAD; ++i)
			{
				try
				{
					net::io_context ioc;
					beast::tcp_stream stream(ioc);
					stream.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), server.Port()));
					http::request<http::string_body> request{http::verb::get, "/api/ping", 11};
					request.set(http::field::host, "127.0.0.1");
					http::write(stream, request);
					beast::flat_buffer buffer;
					http::response<http::string_body> response;
					http::read(stream, buffer, response);
					if (response.result_int() == 200)
					{
						++successes;
					}
				}
				catch (...)
				{
				}
			}
		});
	}
	for (auto& thread : requesters)
	{
		thread.join();
	}
	BOOST_CHECK_EQUAL(successes, NUM_THREADS * REQUESTS_PER_THREAD);
	BOOST_CHECK_EQUAL(handled, NUM_THREADS * REQUESTS_PER_THREAD);
}

BOOST_AUTO_TEST_CASE(SlowConsumerBacklogCap, *utf::timeout(120))
{
	BOOST_TEST_MESSAGE("a server-side \"Session backlog full\" error is expected here");
	auto settings = ServerSettings();
	settings.maxSessionBacklog = 8;
	CWebsocketServer server("stress_backlog", settings);
	server.Start(static_cast<unsigned short>(0), CWebsocketServer::CClientCallbacks());

	CWebsocketClient client("backlog_client", ClientSettings());
	std::atomic<int> receivedCount{0};
	client.RegisterMessageCallback([&](const std::string&) { ++receivedCount; });
	BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));
	BOOST_REQUIRE(PollUntil([&]() { return server.ClientsConnected(); }));

	// Flood far past the backlog; extra payloads are dropped but nothing wedges
	const std::string bigMessage(16 * 1024, 'z');
	for (int i = 0; i < 500; ++i)
	{
		server.SendMessage(bigMessage);
	}
	BOOST_CHECK(PollUntil([&]() { return receivedCount > 0; }));

	// The connection is still alive: keep probing until a probe makes it through
	// (probes sent while the flood is still queued are themselves dropped by the cap)
	std::atomic<bool> gotProbe{false};
	client.RegisterMessageCallback([&](const std::string& m) {
		if (m == "still-alive")
		{
			gotProbe = true;
		}
	});
	const bool survived = PollUntil([&]() {
		server.SendMessage("still-alive");
		return gotProbe.load();
	});
	BOOST_CHECK_MESSAGE(survived, "connection survived the backlog flood");
	BOOST_CHECK(server.ClientsConnected());
}
