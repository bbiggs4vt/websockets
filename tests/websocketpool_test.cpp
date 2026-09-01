/// Tests for sharing one CIoPool across CWebsocketClient and CWebsocketServer
/// instances:
///  - default-configured instances share the process-wide CIoPool::Default()
///    (verified via the process thread count)
///  - client + server end-to-end on one pool, and pool reuse after both die
///  - several servers and clients on one pool with cross traffic
///  - repeated instance destruction on a shared pool (with traffic in flight)
///  - client destruction mid-connect on a shared pool
///  - no client callbacks fire after the destructor returns, even though the
///    shared pool keeps running
///
/// All Boost.Test assertions run on the main thread; callbacks only signal
/// through promises, atomics, and mutex-guarded containers.

#define BOOST_TEST_MODULE WebsocketIoPool
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <fstream>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

#include "CIoPool.h"
#include "CWebsocketClient.h"
#include "CWebsocketServer.h"
#include "test_helpers.h"

namespace utf = boost::unit_test;

using testhelpers::CSilentServer;
using testhelpers::PollUntil;
using testhelpers::WaitFor;
using websocketclient::CIoPool;
using websocketclient::CIoPoolPtr;
using websocketclient::CWebsocketClient;
using websocketclient::CWebsocketServer;

namespace
{

#ifdef __linux__
size_t CountProcessThreads()
{
	std::ifstream status("/proc/self/status");
	std::string line;
	while (std::getline(status, line))
	{
		if (line.rfind("Threads:", 0) == 0)
		{
			return std::stoul(line.substr(8));
		}
	}
	return 0;
}
#endif

CWebsocketServer::CServerSettings ServerSettings(const CIoPoolPtr& pool)
{
	CWebsocketServer::CServerSettings settings;
	settings.handshakeTimeoutS = 5;
	settings.idleTimeoutS = 30;
	settings.ioPool = pool;
	return settings;
}

CWebsocketClient::CClientSettings ClientSettings(const CIoPoolPtr& pool)
{
	CWebsocketClient::CClientSettings settings;
	settings.handshakeTimeoutS = 5;
	settings.idleTimeoutS = 30;
	settings.ioPool = pool;
	return settings;
}

/// One connect/echo/close round trip between a fresh client and server on the pool
bool RoundTrip(const CIoPoolPtr& pool)
{
	CWebsocketServer server("pool_roundtrip_server", ServerSettings(pool));
	std::promise<std::string> serverGot;
	CWebsocketServer::CClientCallbacks callbacks;
	callbacks.mOnClientMessageReceivedCb = [&](uint32_t, const std::string& m) { serverGot.set_value(m); };
	server.Start(static_cast<unsigned short>(0), callbacks);

	CWebsocketClient client("pool_roundtrip_client", ClientSettings(pool));
	if (!client.Connect("127.0.0.1", server.Port(), "/"))
	{
		return false;
	}
	client.SendMessage("ping");
	auto gotFuture = serverGot.get_future();
	if (!WaitFor(gotFuture) || gotFuture.get() != "ping")
	{
		return false;
	}
	client.Close();
	return true;
}

} // namespace

BOOST_AUTO_TEST_CASE(DefaultPoolIsShared, *utf::timeout(120))
{
	BOOST_CHECK(CIoPool::Default() == CIoPool::Default());

	// Default-configured server + client work end to end on the default pool
	CWebsocketServer server("default_pool_server");
	std::promise<std::string> serverGot;
	CWebsocketServer::CClientCallbacks callbacks;
	callbacks.mOnClientMessageReceivedCb = [&](uint32_t, const std::string& m) { serverGot.set_value(m); };
	server.Start(static_cast<unsigned short>(0), callbacks);

	CWebsocketClient client("default_pool_client");
	BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));
	client.SendMessage("via-default-pool");
	auto gotFuture = serverGot.get_future();
	BOOST_REQUIRE(WaitFor(gotFuture));
	BOOST_CHECK_EQUAL(gotFuture.get(), "via-default-pool");

#ifdef __linux__
	// More default-configured clients add only their workqueue thread each -
	// no per-instance IO threads
	const size_t baseThreads = CountProcessThreads();
	std::vector<std::unique_ptr<CWebsocketClient>> extras;
	for (int i = 0; i < 3; ++i)
	{
		extras.push_back(std::make_unique<CWebsocketClient>("default_extra_" + std::to_string(i)));
	}
	const size_t withExtras = CountProcessThreads();
	BOOST_CHECK_EQUAL(withExtras - baseThreads, 3u);
#endif
	client.Close();
}

BOOST_AUTO_TEST_CASE(SharedPoolEndToEnd, *utf::timeout(120))
{
	auto pool = std::make_shared<CIoPool>(2);
	{
		CWebsocketServer server("pool_server", ServerSettings(pool));
		std::promise<std::string> serverGot;
		std::promise<uint32_t> added;
		CWebsocketServer::CClientCallbacks callbacks;
		callbacks.mOnClientAddedCb = [&](uint32_t id, const std::string&, bool) { added.set_value(id); };
		callbacks.mOnClientMessageReceivedCb = [&](uint32_t, const std::string& m) { serverGot.set_value(m); };
		server.Start(static_cast<unsigned short>(0), callbacks);

		CWebsocketClient client("pool_client", ClientSettings(pool));
		std::promise<std::string> clientGot;
		std::promise<void> disconnected;
		client.RegisterMessageCallback([&](const std::string& m) { clientGot.set_value(m); });
		client.RegisterDisconnectCallback([&]() { disconnected.set_value(); });

		BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));
		auto addedFuture = added.get_future();
		BOOST_REQUIRE(WaitFor(addedFuture));
		const uint32_t clientId = addedFuture.get();

		client.SendMessage("up");
		auto serverGotFuture = serverGot.get_future();
		BOOST_REQUIRE_MESSAGE(WaitFor(serverGotFuture), "server received on shared pool");
		BOOST_CHECK_EQUAL(serverGotFuture.get(), "up");

		server.SendMessage(clientId, "down");
		auto clientGotFuture = clientGot.get_future();
		BOOST_REQUIRE_MESSAGE(WaitFor(clientGotFuture), "client received on shared pool");
		BOOST_CHECK_EQUAL(clientGotFuture.get(), "down");

		client.Close();
		auto disconnectedFuture = disconnected.get_future();
		BOOST_CHECK_MESSAGE(WaitFor(disconnectedFuture), "clean close on shared pool");
	}
	// Both instances are gone; the pool must still be fully usable
	BOOST_CHECK_MESSAGE(RoundTrip(pool), "pool remains usable after instances died");
}

BOOST_AUTO_TEST_CASE(ManyInstancesOnOnePool, *utf::timeout(120))
{
	const int NUM_SERVERS = 2;
	const int CLIENTS_PER_SERVER = 2;
	const int MESSAGES_PER_CLIENT = 20;

	auto pool = std::make_shared<CIoPool>(3);

	std::vector<std::unique_ptr<CWebsocketServer>> servers;
	std::vector<std::atomic<int>> serverReceived(NUM_SERVERS);
	for (int s = 0; s < NUM_SERVERS; ++s)
	{
		servers.push_back(
			std::make_unique<CWebsocketServer>("multi_server_" + std::to_string(s), ServerSettings(pool)));
		CWebsocketServer::CClientCallbacks callbacks;
		callbacks.mOnClientMessageReceivedCb = [&serverReceived, s](uint32_t, const std::string&) {
			++serverReceived[s];
		};
		servers.back()->Start(static_cast<unsigned short>(0), callbacks);
	}

	std::vector<std::unique_ptr<CWebsocketClient>> clients;
	std::vector<std::atomic<int>> clientReceived(NUM_SERVERS * CLIENTS_PER_SERVER);
	for (int s = 0; s < NUM_SERVERS; ++s)
	{
		for (int c = 0; c < CLIENTS_PER_SERVER; ++c)
		{
			const int index = s * CLIENTS_PER_SERVER + c;
			clients.push_back(
				std::make_unique<CWebsocketClient>("multi_client_" + std::to_string(index),
												   ClientSettings(pool)));
			clients.back()->RegisterMessageCallback(
				[&clientReceived, index](const std::string&) { ++clientReceived[index]; });
			BOOST_REQUIRE(clients.back()->Connect("127.0.0.1", servers[s]->Port(), "/"));
		}
	}

	// Every client sends to its server concurrently; every server broadcasts back
	std::vector<std::thread> senders;
	for (auto& client : clients)
	{
		senders.emplace_back([&client]() {
			for (int i = 0; i < MESSAGES_PER_CLIENT; ++i)
			{
				client->SendMessage("m" + std::to_string(i));
			}
		});
	}
	for (auto& thread : senders)
	{
		thread.join();
	}
	BOOST_CHECK_MESSAGE(PollUntil([&]() {
							for (int s = 0; s < NUM_SERVERS; ++s)
							{
								if (serverReceived[s] < CLIENTS_PER_SERVER * MESSAGES_PER_CLIENT)
								{
									return false;
								}
							}
							return true;
						}),
						"each server received all of its clients' messages");

	const int BROADCASTS = 10;
	for (auto& server : servers)
	{
		for (int i = 0; i < BROADCASTS; ++i)
		{
			server->SendMessage("b" + std::to_string(i));
		}
	}
	BOOST_CHECK_MESSAGE(PollUntil([&]() {
							for (auto& count : clientReceived)
							{
								if (count < BROADCASTS)
								{
									return false;
								}
							}
							return true;
						}),
						"every client received its server's broadcasts");
}

BOOST_AUTO_TEST_CASE(DestructionStressOnSharedPool, *utf::timeout(120))
{
	BOOST_TEST_MESSAGE("client/server \"Send dropped\"/connection errors are expected here");
	auto pool = std::make_shared<CIoPool>(2);

	for (int iteration = 0; iteration < 8; ++iteration)
	{
		CWebsocketServer server("pool_dtor_server", ServerSettings(pool));
		server.Start(static_cast<unsigned short>(0), CWebsocketServer::CClientCallbacks());
		CWebsocketClient client("pool_dtor_client", ClientSettings(pool));
		BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));
		// Queue traffic in both directions, then destroy both with it in flight
		for (int i = 0; i < 25; ++i)
		{
			client.SendMessage(std::string(4 * 1024, 'c'));
			server.SendMessage(std::string(4 * 1024, 's'));
		}
		if (iteration % 2 == 1)
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(iteration));
		}
		// Both destructors run here on every iteration
	}
	BOOST_CHECK_MESSAGE(RoundTrip(pool), "pool survived 8 destruction cycles with traffic in flight");
}

BOOST_AUTO_TEST_CASE(ShutdownMidConnectOnSharedPool, *utf::timeout(120))
{
	auto pool = std::make_shared<CIoPool>(2);
	CSilentServer silent;

	for (int iteration = 0; iteration < 8; ++iteration)
	{
		CWebsocketClient client("pool_midconnect_client", ClientSettings(pool));
		client.AsyncConnect("127.0.0.1", silent.Port(), "/");
		if (iteration % 3 == 1)
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		else if (iteration % 3 == 2)
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(20));
		}
		// Destructor runs here mid-connect; the shared pool must keep running
	}
	BOOST_CHECK_MESSAGE(RoundTrip(pool), "pool survived 8 mid-connect destructions");
}

BOOST_AUTO_TEST_CASE(NoCallbacksAfterDestruction, *utf::timeout(120))
{
	auto pool = std::make_shared<CIoPool>(2);
	CWebsocketServer server("pool_cb_server", ServerSettings(pool));
	server.Start(static_cast<unsigned short>(0), CWebsocketServer::CClientCallbacks());

	// A background thread broadcasts continuously so messages keep arriving
	// right up to (and past) the client's destruction
	std::atomic<bool> keepBroadcasting{true};
	std::thread broadcaster([&]() {
		while (keepBroadcasting)
		{
			server.SendMessage("spam");
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
	});

	std::atomic<int> received{0};
	{
		CWebsocketClient client("pool_cb_client", ClientSettings(pool));
		client.RegisterMessageCallback([&](const std::string&) { ++received; });
		BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));
		BOOST_REQUIRE(PollUntil([&]() { return received > 0; }));
		// Destructor runs here while broadcasts are still streaming in
	}
	const int countAtDestruction = received;
	std::this_thread::sleep_for(std::chrono::milliseconds(300));
	BOOST_CHECK_MESSAGE(received == countAtDestruction,
						"no client callbacks fired after the destructor returned");

	keepBroadcasting = false;
	broadcaster.join();
}
