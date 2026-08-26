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

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <mutex>
#include <set>
#include <sstream>
#include <thread>

#include "test_helpers.h"
#include "websocket/CWebsocketClient.h"

using testhelpers::CEchoServer;
using testhelpers::Check;
using testhelpers::CSilentServer;
using testhelpers::gFailures;
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

/// Polls until `done` returns true or the deadline passes
bool PollUntil(const std::function<bool()>& done, std::chrono::seconds timeout = std::chrono::seconds(20))
{
	const auto deadline = std::chrono::steady_clock::now() + timeout;
	while (std::chrono::steady_clock::now() < deadline)
	{
		if (done())
		{
			return true;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	return done();
}

void TestConcurrentSends()
{
	std::cout << "\n=== concurrent sends from many threads ===\n";
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

	Check(client.Connect("127.0.0.1", server.Port(), "/"), "connect for concurrent send test");

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
	Check(allReceived, "all concurrent messages echoed back");
	{
		std::lock_guard<std::mutex> lock(receivedMutex);
		Check(receivedTexts == expectedTexts, "text payloads intact (no interleaving corruption)");
		Check(receivedBinaries == expectedBinaries, "binary payloads intact");
	}
	client.Close();
}

void TestConcurrentSendAndClose()
{
	std::cout << "\n=== concurrent Send vs Close from multiple threads ===\n";
	std::cout << "(\"Send dropped\"/\"Cannot send\" errors below are expected)\n";

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
		Check(client.Connect("127.0.0.1", server.Port(), "/"), "connect for send-vs-close test");

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
		Check(WaitFor(disconnectedFuture), "disconnect fired after racing close");
		// Allow any straggling (erroneous) second callback to arrive before checking
		std::this_thread::sleep_for(std::chrono::milliseconds(200));
		Check(disconnectCount == 1, "disconnect callback fired exactly once");
		Check(!client.IsConnected(), "not connected after racing close");
	}
}

void TestShutdownDuringQueuedSends()
{
	std::cout << "\n=== destruction while sends are queued/in flight ===\n";
	const std::string bigMessage(8 * 1024, 'p');
	const auto bigContent = std::make_shared<std::vector<uint8_t>>(8 * 1024, 0x5A);

	for (int iteration = 0; iteration < 10; ++iteration)
	{
		CEchoServer server;
		{
			CWebsocketClient client("stress_shutdown_send", FastSettings());
			if (!client.Connect("127.0.0.1", server.Port(), "/"))
			{
				Check(false, "connect for shutdown-during-send iteration " + std::to_string(iteration));
				continue;
			}
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
	Check(true, "survived 10 destructions with queued sends");
}

void TestShutdownDuringConnect()
{
	std::cout << "\n=== destruction while a connect is in progress ===\n";
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
	Check(true, "survived 10 destructions mid-connect");

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
	Check(true, "survived 10 destructions mid-connect against live server");
}

void TestShutdownDuringMessageCallback()
{
	std::cout << "\n=== destruction while a message callback is executing ===\n";
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
		Check(client.Connect("127.0.0.1", server.Port(), "/"), "connect for callback-shutdown test");
		client.SendMessage("trigger");
		auto enteredFuture = callbackEntered.get_future();
		Check(WaitFor(enteredFuture), "message callback entered");
		// Destructor runs here while the callback is still sleeping on an IO thread
	}
	Check(callbackFinished, "destructor waited for in-flight message callback");
}

void TestShutdownDuringDisconnectCallback()
{
	std::cout << "\n=== destruction while the disconnect callback is executing ===\n";
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
		Check(client.Connect("127.0.0.1", server.Port(), "/"), "connect for disconnect-callback-shutdown test");
		client.Close();
		auto enteredFuture = callbackEntered.get_future();
		Check(WaitFor(enteredFuture), "disconnect callback entered");
		// Destructor runs here while the disconnect callback is still executing
	}
	Check(callbackFinished, "destructor waited for in-flight disconnect callback");
}

void TestCloseDuringConnect()
{
	std::cout << "\n=== Close() during an in-progress connect ===\n";
	CSilentServer silent;

	std::atomic<bool> connectFired{false};
	CWebsocketClient client("stress_close_during_connect", FastSettings());
	client.RegisterConnectCallback([&]() { connectFired = true; });
	client.AsyncConnect("127.0.0.1", silent.Port(), "/");
	std::this_thread::sleep_for(std::chrono::milliseconds(100)); // parked in websocket handshake
	client.Close();
	std::this_thread::sleep_for(std::chrono::milliseconds(300));
	Check(!connectFired, "connect callback never fired after Close during connect");
	Check(!client.IsConnected(), "not connected after Close during connect");
}

void TestSyncConnectTimeoutBounded()
{
	std::cout << "\n=== synchronous Connect timeout is bounded ===\n";
	CSilentServer silent;

	CWebsocketClient::CClientSettings settings;
	settings.handshakeTimeoutS = 1;
	CWebsocketClient client("stress_sync_timeout", settings);

	const auto start = std::chrono::steady_clock::now();
	const bool connected = client.Connect("127.0.0.1", silent.Port(), "/");
	const auto elapsed = std::chrono::steady_clock::now() - start;

	Check(!connected, "Connect against silent server fails");
	Check(elapsed < std::chrono::seconds(5), "Connect returned within the timeout bound");
}

void TestReconnectCycles()
{
	std::cout << "\n=== repeated connect/close cycles on one client ===\n";
	CEchoServer server;
	CWebsocketClient client("stress_reconnect", FastSettings());

	bool allCyclesPassed = true;
	for (int cycle = 0; cycle < 10; ++cycle)
	{
		std::promise<std::string> echoed;
		std::promise<void> disconnected;
		client.RegisterMessageCallback([&echoed](const std::string& m) { echoed.set_value(m); });
		client.RegisterDisconnectCallback([&disconnected]() { disconnected.set_value(); });

		if (!client.Connect("127.0.0.1", server.Port(), "/"))
		{
			Check(false, "reconnect cycle " + std::to_string(cycle) + " connect");
			allCyclesPassed = false;
			break;
		}
		const std::string message = "cycle-" + std::to_string(cycle);
		client.SendMessage(message);
		auto echoedFuture = echoed.get_future();
		if (!WaitFor(echoedFuture) || echoedFuture.get() != message)
		{
			Check(false, "reconnect cycle " + std::to_string(cycle) + " echo");
			allCyclesPassed = false;
			break;
		}
		client.Close();
		auto disconnectedFuture = disconnected.get_future();
		if (!WaitFor(disconnectedFuture))
		{
			Check(false, "reconnect cycle " + std::to_string(cycle) + " disconnect");
			allCyclesPassed = false;
			break;
		}
	}
	Check(allCyclesPassed, "10 connect/close cycles completed");
}

void TestThrowingCallback()
{
	std::cout << "\n=== throwing callbacks are contained ===\n";
	std::cout << "(\"Unhandled exception\" errors below are expected)\n";
	CEchoServer server;
	CWebsocketClient client("stress_throwing_cb", FastSettings());

	std::atomic<int> callbackCount{0};
	client.RegisterMessageCallback([&](const std::string&) {
		++callbackCount;
		throw std::runtime_error("callback exploded");
	});

	Check(client.Connect("127.0.0.1", server.Port(), "/"), "connect for throwing-callback test");
	client.SendMessage("boom-1");
	client.SendMessage("boom-2");
	Check(PollUntil([&]() { return callbackCount == 2; }), "both messages delivered despite exceptions");
	Check(client.IsConnected(), "still connected after callback exceptions");
	client.Close();
}

void TestSendWithoutConnection()
{
	std::cout << "\n=== sends without a connection are safe ===\n";
	std::cout << "(\"Cannot send\" errors below are expected)\n";
	CWebsocketClient client("stress_no_connection", FastSettings());
	client.SendMessage("into the void");
	client.SendContent(std::vector<uint8_t>{1, 2, 3});
	client.SendContent(std::shared_ptr<std::vector<uint8_t>>()); // null payload
	Check(!client.IsConnected(), "sends without connection are no-ops");
}

} // namespace

int main()
{
	TestConcurrentSends();
	TestConcurrentSendAndClose();
	TestShutdownDuringQueuedSends();
	TestShutdownDuringConnect();
	TestShutdownDuringMessageCallback();
	TestShutdownDuringDisconnectCallback();
	TestCloseDuringConnect();
	TestSyncConnectTimeoutBounded();
	TestReconnectCycles();
	TestThrowingCallback();
	TestSendWithoutConnection();

	if (gFailures == 0)
	{
		std::cout << "\nAll stress tests passed\n";
		return EXIT_SUCCESS;
	}
	std::cerr << "\n" << gFailures << " stress test(s) failed\n";
	return EXIT_FAILURE;
}
