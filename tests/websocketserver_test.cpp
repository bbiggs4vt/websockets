/// Functional tests for CWebsocketServer, using CWebsocketClient as the websocket
/// peer and a minimal Beast client for plain http requests. Covers start/stop,
/// client callbacks, per-client/broadcast/uri sends, uri routing and connection
/// counts, allowed-uri filtering, http request handling (including 404 and the
/// auto bad-request reply), delayed payload evaluation, and callback threading.
///
/// All Boost.Test assertions run on the main thread; callbacks only signal
/// through promises, atomics, and mutex-guarded containers.

#define BOOST_TEST_MODULE WebsocketServer
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
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
using websocketclient::CWebsocketServerPtr;

namespace
{

CWebsocketServer::CServerSettings ServerSettings()
{
	CWebsocketServer::CServerSettings settings;
	settings.handshakeTimeoutS = 5;
	settings.idleTimeoutS = 30;
	return settings;
}

CWebsocketClient::CClientSettings ClientSettings()
{
	CWebsocketClient::CClientSettings settings;
	settings.handshakeTimeoutS = 5;
	settings.idleTimeoutS = 30;
	return settings;
}

struct SHttpResult
{
	unsigned status = 0;
	std::string body;
	std::string contentType;
};

/// Minimal synchronous http client for exercising the server's http path
SHttpResult DoHttpRequest(unsigned short port, http::verb method, const std::string& target,
						  const std::string& body = "", const std::string& contentType = "")
{
	net::io_context ioc;
	beast::tcp_stream stream(ioc);
	stream.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), port));

	http::request<http::string_body> request{method, target, 11};
	request.set(http::field::host, "127.0.0.1");
	if (!contentType.empty())
	{
		request.set(http::field::content_type, contentType);
	}
	if (!body.empty())
	{
		request.body() = body;
	}
	request.prepare_payload();
	http::write(stream, request);

	beast::flat_buffer buffer;
	http::response<http::string_body> response;
	http::read(stream, buffer, response);

	SHttpResult result;
	result.status = response.result_int();
	result.body = response.body();
	result.contentType = std::string(response[http::field::content_type]);
	return result;
}

} // namespace

BOOST_AUTO_TEST_CASE(StartStopRestart, *utf::timeout(60))
{
	CWebsocketServer server("srv_startstop", ServerSettings());
	BOOST_CHECK_EQUAL(server.Port(), 0);
	server.Start(static_cast<unsigned short>(0), CWebsocketServer::CClientCallbacks());
	const unsigned short firstPort = server.Port();
	BOOST_CHECK(firstPort != 0);

	// Second Start without Stop is rejected
	BOOST_CHECK_THROW(server.Start(static_cast<unsigned short>(0), CWebsocketServer::CClientCallbacks()),
					  std::exception);

	server.Stop();
	server.Stop(); // may be called multiple times
	BOOST_CHECK_EQUAL(server.Port(), 0);

	// Restart works and can bind a fresh port
	server.Start(static_cast<unsigned short>(0), CWebsocketServer::CClientCallbacks());
	BOOST_CHECK(server.Port() != 0);
	server.Stop();
}

BOOST_AUTO_TEST_CASE(ClientLifecycleAndSends, *utf::timeout(60))
{
	CWebsocketServer server("srv_lifecycle", ServerSettings());

	std::promise<uint32_t> added;
	std::promise<bool> addedSsl;
	std::promise<std::pair<uint32_t, std::string>> gotMessage;
	std::promise<std::pair<uint32_t, std::vector<uint8_t>>> gotContent;
	std::promise<uint32_t> closed;

	CWebsocketServer::CClientCallbacks callbacks;
	callbacks.mOnClientAddedCb = [&](uint32_t id, const std::string&, bool isSsl) {
		added.set_value(id);
		addedSsl.set_value(isSsl);
	};
	callbacks.mOnClientMessageReceivedCb = [&](uint32_t id, const std::string& m) {
		gotMessage.set_value({id, m});
	};
	callbacks.mOnClientContentReceivedCb = [&](uint32_t id, const std::vector<uint8_t>& c) {
		gotContent.set_value({id, c});
	};
	callbacks.mOnClientClosedCb = [&](uint32_t id) { closed.set_value(id); };

	server.Start(static_cast<unsigned short>(0), callbacks);
	BOOST_CHECK(!server.ClientsConnected());

	CWebsocketClient client("lifecycle_client", ClientSettings());
	std::promise<std::string> clientGotText;
	std::promise<std::vector<uint8_t>> clientGotBinary;
	client.RegisterMessageCallback([&](const std::string& m) { clientGotText.set_value(m); });
	client.RegisterContentCallback([&](const std::vector<uint8_t>& c) { clientGotBinary.set_value(c); });

	BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));

	auto addedFuture = added.get_future();
	BOOST_REQUIRE_MESSAGE(WaitFor(addedFuture), "added callback fired");
	const uint32_t clientId = addedFuture.get();
	BOOST_CHECK(clientId != 0);
	auto addedSslFuture = addedSsl.get_future();
	BOOST_REQUIRE(WaitFor(addedSslFuture));
	BOOST_CHECK(!addedSslFuture.get());

	BOOST_CHECK(server.ClientsConnected());
	const auto ids = server.GetConnectedClientIds();
	BOOST_REQUIRE_EQUAL(ids.size(), 1u);
	BOOST_CHECK_EQUAL(ids.begin()->first, clientId);
	BOOST_CHECK(!ids.begin()->second); // not ssl

	// client -> server text and binary
	client.SendMessage("hello server");
	auto messageFuture = gotMessage.get_future();
	BOOST_REQUIRE_MESSAGE(WaitFor(messageFuture), "server received text");
	const auto messageReceived = messageFuture.get();
	BOOST_CHECK_EQUAL(messageReceived.first, clientId);
	BOOST_CHECK_EQUAL(messageReceived.second, "hello server");

	const std::vector<uint8_t> binary{9, 8, 7};
	client.SendContent(binary);
	auto contentFuture = gotContent.get_future();
	BOOST_REQUIRE_MESSAGE(WaitFor(contentFuture), "server received binary");
	BOOST_CHECK_EQUAL(contentFuture.get().first, clientId);

	// server -> client, addressed and broadcast
	server.SendMessage(clientId, "hello client");
	auto textFuture = clientGotText.get_future();
	BOOST_REQUIRE_MESSAGE(WaitFor(textFuture), "client received addressed text");
	BOOST_CHECK_EQUAL(textFuture.get(), "hello client");

	server.SendContent(std::vector<uint8_t>{1, 2, 3});
	auto binaryFuture = clientGotBinary.get_future();
	BOOST_REQUIRE_MESSAGE(WaitFor(binaryFuture), "client received broadcast binary");
	const auto broadcastReceived = binaryFuture.get();
	BOOST_CHECK_EQUAL(broadcastReceived.size(), 3u);

	// client-initiated close reaches the closed callback
	client.Close();
	auto closedFuture = closed.get_future();
	BOOST_REQUIRE_MESSAGE(WaitFor(closedFuture), "closed callback fired");
	BOOST_CHECK_EQUAL(closedFuture.get(), clientId);
	BOOST_CHECK(PollUntil([&]() { return !server.ClientsConnected(); }));
}

BOOST_AUTO_TEST_CASE(BroadcastAndAddressedSends, *utf::timeout(60))
{
	CWebsocketServer server("srv_broadcast", ServerSettings());
	server.Start(static_cast<unsigned short>(0), CWebsocketServer::CClientCallbacks());

	const int NUM_CLIENTS = 3;
	std::vector<std::unique_ptr<CWebsocketClient>> clients;
	std::mutex receivedMutex;
	std::vector<std::vector<std::string>> received(NUM_CLIENTS);

	for (int i = 0; i < NUM_CLIENTS; ++i)
	{
		clients.push_back(std::make_unique<CWebsocketClient>("bc_client_" + std::to_string(i), ClientSettings()));
		clients.back()->RegisterMessageCallback([&received, &receivedMutex, i](const std::string& m) {
			std::lock_guard<std::mutex> lock(receivedMutex);
			received[i].push_back(m);
		});
		BOOST_REQUIRE(clients.back()->Connect("127.0.0.1", server.Port(), "/"));
	}
	BOOST_REQUIRE(PollUntil([&]() { return server.GetConnectedClientIds().size() == NUM_CLIENTS; }));

	server.SendMessage("to-everyone");
	BOOST_CHECK_MESSAGE(PollUntil([&]() {
							std::lock_guard<std::mutex> lock(receivedMutex);
							for (const auto& perClient : received)
							{
								if (perClient.empty())
								{
									return false;
								}
							}
							return true;
						}),
						"broadcast reached all clients");

	// Addressed send reaches exactly one client
	const auto ids = server.GetConnectedClientIds();
	server.SendMessage(ids.begin()->first, "just-for-you");
	BOOST_CHECK_MESSAGE(PollUntil([&]() {
							std::lock_guard<std::mutex> lock(receivedMutex);
							size_t total = 0;
							for (const auto& perClient : received)
							{
								for (const auto& message : perClient)
								{
									if (message == "just-for-you")
									{
										++total;
									}
								}
							}
							return total == 1;
						}),
						"addressed send reached exactly one client");
	std::this_thread::sleep_for(std::chrono::milliseconds(200));
	{
		std::lock_guard<std::mutex> lock(receivedMutex);
		size_t total = 0;
		for (const auto& perClient : received)
		{
			for (const auto& message : perClient)
			{
				if (message == "just-for-you")
				{
					++total;
				}
			}
		}
		BOOST_CHECK_EQUAL(total, 1u);
	}
}

BOOST_AUTO_TEST_CASE(UriRoutingAndConnectionCounts, *utf::timeout(60))
{
	CWebsocketServer server("srv_uri", ServerSettings());

	std::promise<std::string> addedAppend;
	std::atomic<bool> firstAdded{false};
	CWebsocketServer::CClientCallbacks callbacks;
	callbacks.mOnClientAddedCb = [&](uint32_t, const std::string& uriAppend, bool) {
		if (!firstAdded.exchange(true))
		{
			addedAppend.set_value(uriAppend);
		}
	};

	std::mutex stateMutex;
	std::vector<size_t> connectionCounts;
	std::vector<std::string> uriMessages;
	CWebsocketServer::CUriCallbacks uriCallbacks;
	uriCallbacks.mOnConnectionChange = [&](size_t n) {
		std::lock_guard<std::mutex> lock(stateMutex);
		connectionCounts.push_back(n);
	};
	uriCallbacks.mOnClientMessageReceivedCb = [&](uint32_t, const std::string& m) {
		std::lock_guard<std::mutex> lock(stateMutex);
		uriMessages.push_back(m);
	};

	server.AddUri("/alpha", uriCallbacks);
	server.Start(static_cast<unsigned short>(0), callbacks);

	CWebsocketClient client("uri_client", ClientSettings());
	BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/alpha/extra?x=1"));

	auto appendFuture = addedAppend.get_future();
	BOOST_REQUIRE(WaitFor(appendFuture));
	BOOST_CHECK_EQUAL(appendFuture.get(), "/extra?x=1");

	BOOST_CHECK(PollUntil([&]() { return server.ClientsConnected("/alpha"); }));
	BOOST_CHECK(!server.ClientsConnected("/beta"));

	client.SendMessage("uri-routed");
	BOOST_CHECK_MESSAGE(PollUntil([&]() {
							std::lock_guard<std::mutex> lock(stateMutex);
							return uriMessages.size() == 1 && uriMessages[0] == "uri-routed";
						}),
						"uri message callback fired");

	// Uri-scoped broadcast reaches the client
	std::promise<std::string> clientGot;
	client.RegisterMessageCallback([&](const std::string& m) { clientGot.set_value(m); });
	server.SendMessage("/alpha", "for-alpha");
	auto clientGotFuture = clientGot.get_future();
	BOOST_REQUIRE(WaitFor(clientGotFuture));
	BOOST_CHECK_EQUAL(clientGotFuture.get(), "for-alpha");

	client.Close();
	BOOST_CHECK_MESSAGE(PollUntil([&]() {
							std::lock_guard<std::mutex> lock(stateMutex);
							return connectionCounts.size() == 2;
						}),
						"connection change fired for connect and disconnect");
	{
		std::lock_guard<std::mutex> lock(stateMutex);
		BOOST_CHECK_EQUAL(connectionCounts[0], 1u);
		BOOST_CHECK_EQUAL(connectionCounts[1], 0u);
	}

	// A client on an unrelated uri does not trigger the /alpha callbacks
	CWebsocketClient otherClient("uri_client_other", ClientSettings());
	BOOST_REQUIRE(otherClient.Connect("127.0.0.1", server.Port(), "/other"));
	otherClient.SendMessage("not-for-alpha");
	std::this_thread::sleep_for(std::chrono::milliseconds(200));
	{
		std::lock_guard<std::mutex> lock(stateMutex);
		BOOST_CHECK_EQUAL(uriMessages.size(), 1u);
		BOOST_CHECK(!server.ClientsConnected("/alpha"));
	}
	// "/alphabet" must not match the "/alpha" prefix at a non-segment boundary
	CWebsocketClient boundaryClient("uri_client_boundary", ClientSettings());
	BOOST_REQUIRE(boundaryClient.Connect("127.0.0.1", server.Port(), "/alphabet"));
	BOOST_CHECK(PollUntil([&]() { return server.GetConnectedClientIds().size() == 2; }));
	BOOST_CHECK(!server.ClientsConnected("/alpha"));
}

BOOST_AUTO_TEST_CASE(AllowedUrisFilter, *utf::timeout(60))
{
	auto settings = ServerSettings();
	settings.allowedUris = std::set<std::string>{"/ok"};
	CWebsocketServer server("srv_allowed", settings);
	server.Start(static_cast<unsigned short>(0), CWebsocketServer::CClientCallbacks());

	CWebsocketClient goodClient("allowed_client", ClientSettings());
	BOOST_CHECK(goodClient.Connect("127.0.0.1", server.Port(), "/ok/sub"));

	BOOST_TEST_MESSAGE("a \"websocket handshake failed\" client error is expected next");
	CWebsocketClient badClient("denied_client", ClientSettings());
	BOOST_CHECK(!badClient.Connect("127.0.0.1", server.Port(), "/bad"));
	BOOST_CHECK(PollUntil([&]() { return server.GetConnectedClientIds().size() == 1; }));
}

BOOST_AUTO_TEST_CASE(HttpRequests, *utf::timeout(60))
{
	CWebsocketServer server("srv_http", ServerSettings());

	// Snapshot of request fields taken before the handler mutates the (shared) message to reply
	struct SSeenRequest
	{
		std::string method;
		std::string uri;
		std::string body;
		std::string contextType;
		CWebsocketServer::HttpMsg::HttpQueryParameters queryParameters;
		uint32_t clientId = 0;
	};
	std::mutex requestMutex;
	std::vector<SSeenRequest> requests;
	CWebsocketServer::CUriCallbacks apiCallbacks;
	apiCallbacks.mOnRequestReceivedCb = [&](const CWebsocketServer::CHttpWrapperPtr& wrapper) {
		auto msg = wrapper->HttpMsg();
		{
			std::lock_guard<std::mutex> lock(requestMutex);
			requests.push_back(SSeenRequest{msg->method, msg->uri,
											std::string(msg->body.begin(), msg->body.end()), msg->contextType,
											msg->queryParameters, msg->clientId});
		}
		if (msg->method == "GET")
		{
			const std::string body = "{\"items\": []}";
			msg->body.assign(body.begin(), body.end());
			msg->responseCode = 200;
			wrapper->Reply();
		}
		else if (msg->method == "POST")
		{
			msg->responseCode = 201;
			msg->reason = "Created";
			msg->body.clear();
			wrapper->Reply();
		}
		// Anything else: drop the wrapper without replying -> automatic 400
	};
	server.AddUri("/api", apiCallbacks);
	server.Start(static_cast<unsigned short>(0), CWebsocketServer::CClientCallbacks());

	// GET with query parameters
	const auto getResult = DoHttpRequest(server.Port(), http::verb::get, "/api/items?a=1&b=two%20words");
	BOOST_CHECK_EQUAL(getResult.status, 200u);
	BOOST_CHECK_EQUAL(getResult.body, "{\"items\": []}");
	BOOST_CHECK_EQUAL(getResult.contentType, "application/json");
	{
		std::lock_guard<std::mutex> lock(requestMutex);
		BOOST_REQUIRE_EQUAL(requests.size(), 1u);
		BOOST_CHECK_EQUAL(requests[0].method, "GET");
		BOOST_CHECK_EQUAL(requests[0].uri, "/api/items");
		BOOST_CHECK_EQUAL(requests[0].queryParameters.at("a"), "1");
		BOOST_CHECK_EQUAL(requests[0].queryParameters.at("b"), "two words");
		BOOST_CHECK(requests[0].clientId != 0);
	}

	// POST with a body
	const auto postResult =
		DoHttpRequest(server.Port(), http::verb::post, "/api/items", "{\"name\":\"x\"}", "application/json");
	BOOST_CHECK_EQUAL(postResult.status, 201u);
	{
		std::lock_guard<std::mutex> lock(requestMutex);
		BOOST_REQUIRE_EQUAL(requests.size(), 2u);
		BOOST_CHECK_EQUAL(requests[1].body, "{\"name\":\"x\"}");
		BOOST_CHECK_EQUAL(requests[1].contextType, "application/json");
	}

	// Unhandled path -> 404 with the standard error json
	const auto missingResult = DoHttpRequest(server.Port(), http::verb::get, "/nothing/here");
	BOOST_CHECK_EQUAL(missingResult.status, 404u);
	BOOST_CHECK(missingResult.body.find("shortMessage") != std::string::npos);

	// Handler that never replies -> automatic 400 from the wrapper destructor
	const auto unansweredResult = DoHttpRequest(server.Port(), http::verb::delete_, "/api/items");
	BOOST_CHECK_EQUAL(unansweredResult.status, 400u);
	BOOST_CHECK(unansweredResult.body.find("Bad Request") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(RemoveClientAndStopCallbackThreads, *utf::timeout(60))
{
	CWebsocketServer server("srv_threads", ServerSettings());

	std::mutex closedMutex;
	std::vector<std::pair<uint32_t, std::thread::id>> closedEvents;
	CWebsocketServer::CClientCallbacks callbacks;
	callbacks.mOnClientClosedCb = [&](uint32_t id) {
		std::lock_guard<std::mutex> lock(closedMutex);
		closedEvents.push_back({id, std::this_thread::get_id()});
	};
	server.Start(static_cast<unsigned short>(0), callbacks);

	CWebsocketClient clientA("rm_client_a", ClientSettings());
	CWebsocketClient clientB("rm_client_b", ClientSettings());
	std::promise<void> aDisconnected;
	clientA.RegisterDisconnectCallback([&]() { aDisconnected.set_value(); });
	BOOST_REQUIRE(clientA.Connect("127.0.0.1", server.Port(), "/"));
	BOOST_REQUIRE(clientB.Connect("127.0.0.1", server.Port(), "/"));
	BOOST_REQUIRE(PollUntil([&]() { return server.GetConnectedClientIds().size() == 2; }));

	// RemoveClient: closed callback runs in this (the calling) thread
	const auto ids = server.GetConnectedClientIds();
	const uint32_t removedId = ids.begin()->first;
	server.RemoveClient(removedId);
	{
		std::lock_guard<std::mutex> lock(closedMutex);
		BOOST_REQUIRE_EQUAL(closedEvents.size(), 1u);
		BOOST_CHECK_EQUAL(closedEvents[0].first, removedId);
		BOOST_CHECK(closedEvents[0].second == std::this_thread::get_id());
	}
	auto aDisconnectedFuture = aDisconnected.get_future();
	// The removed client observes the disconnect (whichever client was first)
	BOOST_CHECK(PollUntil([&]() { return server.GetConnectedClientIds().size() == 1; }));

	// Stop: remaining closed callback also runs in the calling thread
	server.Stop();
	{
		std::lock_guard<std::mutex> lock(closedMutex);
		BOOST_REQUIRE_EQUAL(closedEvents.size(), 2u);
		BOOST_CHECK(closedEvents[1].second == std::this_thread::get_id());
	}
	(void)aDisconnectedFuture;
}

BOOST_AUTO_TEST_CASE(CallbackSerialization, *utf::timeout(60))
{
	CWebsocketServer server("srv_serial", ServerSettings());

	std::mutex threadMutex;
	std::set<std::thread::id> callbackThreads;
	std::atomic<int> messageCount{0};
	CWebsocketServer::CClientCallbacks callbacks;
	callbacks.mOnClientAddedCb = [&](uint32_t, const std::string&, bool) {
		std::lock_guard<std::mutex> lock(threadMutex);
		callbackThreads.insert(std::this_thread::get_id());
	};
	callbacks.mOnClientMessageReceivedCb = [&](uint32_t, const std::string&) {
		{
			std::lock_guard<std::mutex> lock(threadMutex);
			callbackThreads.insert(std::this_thread::get_id());
		}
		++messageCount;
	};
	server.Start(static_cast<unsigned short>(0), callbacks);

	CWebsocketClient client("serial_client", ClientSettings());
	BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));
	for (int i = 0; i < 20; ++i)
	{
		client.SendMessage("m" + std::to_string(i));
	}
	BOOST_REQUIRE(PollUntil([&]() { return messageCount == 20; }));

	// Async callbacks all arrive on the single workqueue thread, never the test thread
	std::lock_guard<std::mutex> lock(threadMutex);
	BOOST_CHECK_EQUAL(callbackThreads.size(), 1u);
	BOOST_CHECK(*callbackThreads.begin() != std::this_thread::get_id());
}

BOOST_AUTO_TEST_CASE(DelayedEvaluation, *utf::timeout(60))
{
	CWebsocketServer server("srv_delayed", ServerSettings());
	server.Start(static_cast<unsigned short>(0), CWebsocketServer::CClientCallbacks());

	// With no clients connected the payload builders must not run
	std::atomic<int> evalCount{0};
	server.SendMessage([&](std::string& out) {
		++evalCount;
		out = "unused";
	});
	server.SendContent([&](std::vector<uint8_t>& out) {
		++evalCount;
		out = {1};
	});
	BOOST_CHECK_EQUAL(evalCount, 0);

	CWebsocketClient client("delayed_client", ClientSettings());
	std::promise<std::string> gotText;
	std::promise<std::vector<uint8_t>> gotBinary;
	client.RegisterMessageCallback([&](const std::string& m) { gotText.set_value(m); });
	client.RegisterContentCallback([&](const std::vector<uint8_t>& c) { gotBinary.set_value(c); });
	BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));
	BOOST_REQUIRE(PollUntil([&]() { return server.ClientsConnected(); }));

	server.SendMessage([&](std::string& out) {
		++evalCount;
		out = "lazy-text";
	});
	server.SendContent([&](std::vector<uint8_t>& out) {
		++evalCount;
		out = {42};
	});
	auto textFuture = gotText.get_future();
	auto binaryFuture = gotBinary.get_future();
	BOOST_REQUIRE(WaitFor(textFuture));
	BOOST_REQUIRE(WaitFor(binaryFuture));
	BOOST_CHECK_EQUAL(textFuture.get(), "lazy-text");
	BOOST_CHECK_EQUAL(binaryFuture.get().at(0), 42);
	BOOST_CHECK_EQUAL(evalCount, 2);
}

BOOST_AUTO_TEST_CASE(UriWrapperLifecycle, *utf::timeout(60))
{
	auto server = std::make_shared<CWebsocketServer>("srv_wrapper", ServerSettings());
	server->Start(static_cast<unsigned short>(0), CWebsocketServer::CClientCallbacks());

	std::atomic<int> uriMessageCount{0};
	CWebsocketServer::CUriCallbacks uriCallbacks;
	uriCallbacks.mOnClientMessageReceivedCb = [&](uint32_t, const std::string&) { ++uriMessageCount; };

	CWebsocketClient client("wrapper_client", ClientSettings());
	std::promise<std::string> clientGot;
	client.RegisterMessageCallback([&](const std::string& m) { clientGot.set_value(m); });

	{
		CWebsocketServer::CUriWrapper wrapper(server, "/wrapped", uriCallbacks);
		BOOST_REQUIRE(client.Connect("127.0.0.1", server->Port(), "/wrapped"));
		BOOST_REQUIRE(PollUntil([&]() { return wrapper.ClientsConnected(); }));

		wrapper.SendMessage("via-wrapper");
		auto clientGotFuture = clientGot.get_future();
		BOOST_REQUIRE(WaitFor(clientGotFuture));
		BOOST_CHECK_EQUAL(clientGotFuture.get(), "via-wrapper");

		client.SendMessage("to-wrapper");
		BOOST_REQUIRE(PollUntil([&]() { return uriMessageCount == 1; }));
	} // wrapper destruction unregisters the uri

	client.SendMessage("after-unregister");
	std::this_thread::sleep_for(std::chrono::milliseconds(200));
	BOOST_CHECK_EQUAL(uriMessageCount, 1);
}
