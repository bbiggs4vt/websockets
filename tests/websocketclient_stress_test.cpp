/// Stress and lifecycle tests for CWebsocketClient:
///  - many threads sending text + binary concurrently over one connection
///  - concurrent Send vs Close from multiple threads
///  - client destruction (shutdown) while sends are still queued/in flight
///  - client destruction while an async connect is in progress
///  - client destruction while a user callback is still executing
///  - Close() during an in-progress connect
///  - synchronous Connect timeout is bounded against a silent server
///  - repeated connect/close cycles on one client
///  - throwing callbacks don't kill the connection
///
/// All Boost.Test assertions run on the main thread; worker threads and
/// callbacks only signal through promises and atomics.

#define BOOST_TEST_MODULE WebsocketClientStress
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <set>
#include <sstream>
#include <thread>

#include "CWebsocketClient.h"
#include "test_helpers.h"

namespace utf = boost::unit_test;

using testhelpers::CEchoServer;
using testhelpers::CSilentServer;
using testhelpers::PollUntil;
using testhelpers::WaitFor;
using websocketclient::CWebsocketClient;

namespace
{

CWebsocketClient::CClientSettings FastSettings()
{
	CWebsocketClient::CClientSettings settings;
	settings.handshakeTimeoutS = 5;
	settings.idleTimeoutS = 30;
	return settings;
}

} // namespace

BOOST_AUTO_TEST_CASE(ConcurrentSends, *utf::timeout(120))
{
	const int NUM_THREADS = 8;
	const int MESSAGES_PER_THREAD = 25; // per kind (text + binary)

	CEchoServer server;
	CWebsocketClient client("stress_concurrent", FastSettings());

	std::mutex receivedMutex;
	std::set<std::string> receivedTexts;
	std::set<std::vector<uint8_t>> receivedBinaries;

	client.RegisterMessageCallback([&](const std::string& m) {
		std::lock_guard<std::mutex> lock(receivedMutex);
		receivedTexts.insert(m);
	});
	client.RegisterContentCallback([&](const std::vector<uint8_t>& c) {
		std::lock_guard<std::mutex> lock(receivedMutex);
		receivedBinaries.insert(c);
	});

	BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));

	std::set<std::string> expectedTexts;
	std::set<std::vector<uint8_t>> expectedBinaries;
	for (int t = 0; t < NUM_THREADS; ++t)
	{
		for (int i = 0; i < MESSAGES_PER_THREAD; ++i)
		{
			std::ostringstream text;
			text << "thread-" << t << "-msg-" << i << "-" << std::string(100 + i, 'x');
			expectedTexts.insert(text.str());
			expectedBinaries.insert(
				{static_cast<uint8_t>(t), static_cast<uint8_t>(i), 0xAB, static_cast<uint8_t>(t * i % 251)});
		}
	}

	std::vector<std::thread> senders;
	for (int t = 0; t < NUM_THREADS; ++t)
	{
		senders.emplace_back([&client, t]() {
			for (int i = 0; i < MESSAGES_PER_THREAD; ++i)
			{
				std::ostringstream text;
				text << "thread-" << t << "-msg-" << i << "-" << std::string(100 + i, 'x');
				client.SendMessage(text.str());
				client.SendContent(std::vector<uint8_t>{static_cast<uint8_t>(t), static_cast<uint8_t>(i), 0xAB,
														static_cast<uint8_t>(t * i % 251)});
			}
		});
	}
	for (auto& thread : senders)
	{
		thread.join();
	}

	const size_t expectedTotal = expectedTexts.size() + expectedBinaries.size();
	const bool allReceived = PollUntil([&]() {
		std::lock_guard<std::mutex> lock(receivedMutex);
		return receivedTexts.size() + receivedBinaries.size() >= expectedTotal;
	});
	BOOST_CHECK_MESSAGE(allReceived, "all concurrent messages echoed back");
	{
		std::lock_guard<std::mutex> lock(receivedMutex);
		BOOST_CHECK_MESSAGE(receivedTexts == expectedTexts, "text payloads intact (no interleaving corruption)");
		BOOST_CHECK_MESSAGE(receivedBinaries == expectedBinaries, "binary payloads intact");
	}
	client.Close();
}

BOOST_AUTO_TEST_CASE(ConcurrentSendAndClose, *utf::timeout(60))
{
	BOOST_TEST_MESSAGE("\"Send dropped\"/\"Cannot send\" errors on stderr are expected here");

	CEchoServer server;

	std::atomic<int> disconnectCount{0};
	std::promise<void> disconnected;
	{
		CWebsocketClient client("stress_send_vs_close", FastSettings());
		client.RegisterDisconnectCallback([&]() {
			if (++disconnectCount == 1)
			{
				disconnected.set_value();
			}
		});
		BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));

		std::vector<std::thread> workers;
		for (int t = 0; t < 4; ++t)
		{
			workers.emplace_back([&client, t]() {
				for (int i = 0; i < 20; ++i)
				{
					client.SendMessage("racing-" + std::to_string(t) + "-" + std::to_string(i));
					client.IsConnected();
				}
			});
		}
		// Two threads racing to close while the senders are still going
		workers.emplace_back([&client]() {
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			client.Close();
		});
		workers.emplace_back([&client]() {
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			client.Close();
		});
		for (auto& thread : workers)
		{
			thread.join();
		}

		auto disconnectedFuture = disconnected.get_future();
		BOOST_CHECK_MESSAGE(WaitFor(disconnectedFuture), "disconnect fired after racing close");
		// Allow any straggling (erroneous) second callback to arrive before checking
		std::this_thread::sleep_for(std::chrono::milliseconds(200));
		BOOST_CHECK_EQUAL(disconnectCount, 1);
		BOOST_CHECK(!client.IsConnected());
	}
}

BOOST_AUTO_TEST_CASE(ShutdownDuringQueuedSends, *utf::timeout(120))
{
	const std::string bigMessage(8 * 1024, 'p');
	const auto bigContent = std::make_shared<std::vector<uint8_t>>(8 * 1024, 0x5A);

	for (int iteration = 0; iteration < 10; ++iteration)
	{
		CEchoServer server;
		{
			CWebsocketClient client("stress_shutdown_send", FastSettings());
			BOOST_REQUIRE_MESSAGE(client.Connect("127.0.0.1", server.Port(), "/"),
								  "connect for iteration " << iteration);
			std::vector<std::thread> senders;
			for (int t = 0; t < 4; ++t)
			{
				senders.emplace_back([&client, &bigMessage, &bigContent]() {
					for (int i = 0; i < 25; ++i)
					{
						client.SendMessage(bigMessage);
						client.SendContent(bigContent);
					}
				});
			}
			for (auto& thread : senders)
			{
				thread.join();
			}
			if (iteration % 2 == 1)
			{
				std::this_thread::sleep_for(std::chrono::milliseconds(iteration));
			}
			// Destructor runs here with a deep write queue and reads in flight
		}
	}
	BOOST_CHECK_MESSAGE(true, "survived 10 destructions with queued sends");
}

BOOST_AUTO_TEST_CASE(ShutdownDuringConnect, *utf::timeout(120))
{
	CSilentServer silent;

	for (int iteration = 0; iteration < 10; ++iteration)
	{
		CWebsocketClient client("stress_shutdown_connect", FastSettings());
		client.AsyncConnect("127.0.0.1", silent.Port(), "/");
		// Vary how far the connect gets: not at all / resolving / parked in the
		// websocket handshake against the silent server
		if (iteration % 3 == 1)
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		else if (iteration % 3 == 2)
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(20));
		}
		// Destructor runs here mid-connect
	}
	BOOST_CHECK_MESSAGE(true, "survived 10 destructions mid-connect against silent server");

	// Same against a real server so some iterations die between TCP connect,
	// websocket handshake completion, and connect callback dispatch
	CEchoServer echo;
	for (int iteration = 0; iteration < 10; ++iteration)
	{
		CWebsocketClient client("stress_shutdown_connect2", FastSettings());
		client.AsyncConnect("127.0.0.1", echo.Port(), "/");
		if (iteration % 2 == 1)
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(iteration));
		}
	}
	BOOST_CHECK_MESSAGE(true, "survived 10 destructions mid-connect against live server");
}

BOOST_AUTO_TEST_CASE(ShutdownDuringMessageCallback, *utf::timeout(60))
{
	CEchoServer server;

	std::promise<void> callbackEntered;
	std::atomic<bool> callbackFinished{false};
	{
		CWebsocketClient client("stress_shutdown_callback", FastSettings());
		client.RegisterMessageCallback([&](const std::string&) {
			callbackEntered.set_value();
			std::this_thread::sleep_for(std::chrono::milliseconds(300));
			callbackFinished = true;
		});
		BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));
		client.SendMessage("trigger");
		auto enteredFuture = callbackEntered.get_future();
		BOOST_REQUIRE_MESSAGE(WaitFor(enteredFuture), "message callback entered");
		// Destructor runs here while the callback is still sleeping on an IO thread
	}
	BOOST_CHECK_MESSAGE(callbackFinished, "destructor waited for in-flight message callback");
}

BOOST_AUTO_TEST_CASE(ShutdownDuringDisconnectCallback, *utf::timeout(60))
{
	CEchoServer server;

	std::promise<void> callbackEntered;
	std::atomic<bool> callbackFinished{false};
	{
		CWebsocketClient client("stress_shutdown_disconnect_cb", FastSettings());
		client.RegisterDisconnectCallback([&]() {
			callbackEntered.set_value();
			std::this_thread::sleep_for(std::chrono::milliseconds(300));
			callbackFinished = true;
		});
		BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));
		client.Close();
		auto enteredFuture = callbackEntered.get_future();
		BOOST_REQUIRE_MESSAGE(WaitFor(enteredFuture), "disconnect callback entered");
		// Destructor runs here while the disconnect callback is still executing
	}
	BOOST_CHECK_MESSAGE(callbackFinished, "destructor waited for in-flight disconnect callback");
}

BOOST_AUTO_TEST_CASE(CloseDuringConnect, *utf::timeout(60))
{
	CSilentServer silent;

	std::atomic<bool> connectFired{false};
	CWebsocketClient client("stress_close_during_connect", FastSettings());
	client.RegisterConnectCallback([&]() { connectFired = true; });
	client.AsyncConnect("127.0.0.1", silent.Port(), "/");
	std::this_thread::sleep_for(std::chrono::milliseconds(100)); // parked in websocket handshake
	client.Close();
	std::this_thread::sleep_for(std::chrono::milliseconds(300));
	BOOST_CHECK_MESSAGE(!connectFired, "connect callback never fired after Close during connect");
	BOOST_CHECK(!client.IsConnected());
}

BOOST_AUTO_TEST_CASE(SyncConnectTimeoutBounded, *utf::timeout(60))
{
	CSilentServer silent;

	CWebsocketClient::CClientSettings settings;
	settings.handshakeTimeoutS = 1;
	CWebsocketClient client("stress_sync_timeout", settings);

	const auto start = std::chrono::steady_clock::now();
	const bool connected = client.Connect("127.0.0.1", silent.Port(), "/");
	const auto elapsed = std::chrono::steady_clock::now() - start;

	BOOST_CHECK_MESSAGE(!connected, "Connect against silent server fails");
	BOOST_CHECK_MESSAGE(elapsed < std::chrono::seconds(5), "Connect returned within the timeout bound");
}

BOOST_AUTO_TEST_CASE(ReconnectCycles, *utf::timeout(120))
{
	CEchoServer server;
	CWebsocketClient client("stress_reconnect", FastSettings());

	for (int cycle = 0; cycle < 10; ++cycle)
	{
		std::promise<std::string> echoed;
		std::promise<void> disconnected;
		client.RegisterMessageCallback([&echoed](const std::string& m) { echoed.set_value(m); });
		client.RegisterDisconnectCallback([&disconnected]() { disconnected.set_value(); });

		BOOST_REQUIRE_MESSAGE(client.Connect("127.0.0.1", server.Port(), "/"),
							  "reconnect cycle " << cycle << " connect");
		const std::string message = "cycle-" + std::to_string(cycle);
		client.SendMessage(message);
		auto echoedFuture = echoed.get_future();
		BOOST_REQUIRE_MESSAGE(WaitFor(echoedFuture), "reconnect cycle " << cycle << " echo");
		BOOST_CHECK_EQUAL(echoedFuture.get(), message);
		client.Close();
		auto disconnectedFuture = disconnected.get_future();
		BOOST_REQUIRE_MESSAGE(WaitFor(disconnectedFuture), "reconnect cycle " << cycle << " disconnect");
	}
}

BOOST_AUTO_TEST_CASE(ThrowingCallback, *utf::timeout(60))
{
	BOOST_TEST_MESSAGE("\"Unhandled exception\" errors on stderr are expected here");
	CEchoServer server;
	CWebsocketClient client("stress_throwing_cb", FastSettings());

	std::atomic<int> callbackCount{0};
	client.RegisterMessageCallback([&](const std::string&) {
		++callbackCount;
		throw std::runtime_error("callback exploded");
	});

	BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));
	client.SendMessage("boom-1");
	client.SendMessage("boom-2");
	BOOST_CHECK_MESSAGE(PollUntil([&]() { return callbackCount == 2; }),
						"both messages delivered despite exceptions");
	BOOST_CHECK_MESSAGE(client.IsConnected(), "still connected after callback exceptions");
	client.Close();
}

BOOST_AUTO_TEST_CASE(SendWithoutConnection, *utf::timeout(60))
{
	BOOST_TEST_MESSAGE("\"Cannot send\" errors on stderr are expected here");
	CWebsocketClient client("stress_no_connection", FastSettings());
	client.SendMessage("into the void");
	client.SendContent(std::vector<uint8_t>{1, 2, 3});
	client.SendContent(std::shared_ptr<std::vector<uint8_t>>()); // null payload
	BOOST_CHECK(!client.IsConnected());
}
