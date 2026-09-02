/// Loopback tests for CWebsocketClient: spins up a minimal Beast echo server on
/// an ephemeral port, then exercises connect, text/binary send + receive,
/// callbacks, Close(), and failed-connect handling.

#define BOOST_TEST_MODULE WebsocketClient
#include <boost/test/included/unit_test.hpp>

#include <chrono>
#include <future>

#include "CWebsocketClient.h"
#include "test_helpers.h"

namespace utf = boost::unit_test;

using testhelpers::CEchoServer;
using testhelpers::WaitFor;
using websocketclient::CWebsocketClient;

namespace
{

CWebsocketClient::CClientSettings TestSettings()
{
	CWebsocketClient::CClientSettings settings;
	settings.handshakeTimeoutS = 5;
	settings.idleTimeoutS = 10;
	return settings;
}

} // namespace

// Connect, echo text + binary, clean close
BOOST_AUTO_TEST_CASE(HappyPath, *utf::timeout(60))
{
	CEchoServer server;
	CWebsocketClient client("test_client", TestSettings());

	std::promise<void> connected;
	std::promise<std::string> echoedText;
	std::promise<std::vector<uint8_t>> echoedContent;
	std::promise<void> disconnected;

	client.RegisterConnectCallback([&connected]() { connected.set_value(); });
	client.RegisterMessageCallback([&echoedText](const std::string& m) { echoedText.set_value(m); });
	client.RegisterContentCallback(
		[&echoedContent](const std::vector<uint8_t>& c) { echoedContent.set_value(c); });
	client.RegisterDisconnectCallback([&disconnected]() { disconnected.set_value(); });

	BOOST_CHECK(!client.IsConnected());
	BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));
	BOOST_CHECK(client.IsConnected());

	auto connectedFuture = connected.get_future();
	BOOST_CHECK_MESSAGE(WaitFor(connectedFuture), "connect callback fired");

	client.SendMessage("hello websocket");
	auto textFuture = echoedText.get_future();
	BOOST_REQUIRE_MESSAGE(WaitFor(textFuture), "text payload echoed");
	BOOST_CHECK_EQUAL(textFuture.get(), "hello websocket");

	const std::vector<uint8_t> payload{0x00, 0x01, 0xFE, 0xFF, 0x42};
	client.SendContent(std::make_shared<std::vector<uint8_t>>(payload));
	auto contentFuture = echoedContent.get_future();
	BOOST_REQUIRE_MESSAGE(WaitFor(contentFuture), "binary payload echoed");
	const std::vector<uint8_t> received = contentFuture.get();
	BOOST_CHECK_EQUAL_COLLECTIONS(received.begin(), received.end(), payload.begin(), payload.end());

	client.Close();
	client.Close(); // may be called multiple times
	auto disconnectedFuture = disconnected.get_future();
	BOOST_CHECK_MESSAGE(WaitFor(disconnectedFuture), "disconnect callback fired");
	BOOST_CHECK(!client.IsConnected());
}

// Custom handshake headers appear on the upgrade request; clearing removes them
BOOST_AUTO_TEST_CASE(HandshakeHeaders, *utf::timeout(60))
{
	CEchoServer server;
	CWebsocketClient client("hdr_client", TestSettings());

	std::promise<std::string> echoed;
	std::promise<void> disconnected;
	client.RegisterMessageCallback([&echoed](const std::string& m) { echoed.set_value(m); });
	client.RegisterDisconnectCallback([&disconnected]() { disconnected.set_value(); });

	client.SetHandshakeHeader("Authorization", "Bearer token-123");
	client.SetHandshakeHeader("X-Custom-Header", "first");
	client.SetHandshakeHeader("X-Custom-Header", "custom-value"); // replaces the first value
	BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));

	auto headers = server.LastHandshakeHeaders();
	BOOST_REQUIRE(headers.count("Authorization"));
	BOOST_CHECK_EQUAL(headers.at("Authorization"), "Bearer token-123");
	BOOST_REQUIRE(headers.count("X-Custom-Header"));
	BOOST_CHECK_EQUAL(headers.at("X-Custom-Header"), "custom-value");
	// The standard upgrade fields are still intact alongside the custom ones
	BOOST_CHECK(headers.count("Sec-WebSocket-Key"));
	BOOST_CHECK(headers.count("Host"));

	// The connection works normally with custom headers
	client.SendMessage("with-headers");
	auto echoedFuture = echoed.get_future();
	BOOST_REQUIRE(WaitFor(echoedFuture));
	BOOST_CHECK_EQUAL(echoedFuture.get(), "with-headers");

	client.Close();
	auto disconnectedFuture = disconnected.get_future();
	BOOST_REQUIRE(WaitFor(disconnectedFuture));

	// After clearing, a reconnect carries none of the custom headers
	client.ClearHandshakeHeaders();
	BOOST_REQUIRE(client.Connect("127.0.0.1", server.Port(), "/"));
	headers = server.LastHandshakeHeaders();
	BOOST_CHECK(!headers.count("Authorization"));
	BOOST_CHECK(!headers.count("X-Custom-Header"));
	client.Close();
}

// Nothing listening on the target port
BOOST_AUTO_TEST_CASE(FailedConnect, *utf::timeout(60))
{
	CWebsocketClient::CClientSettings settings;
	settings.handshakeTimeoutS = 3;
	CWebsocketClient client("test_client_fail", settings);
	BOOST_CHECK(!client.Connect("127.0.0.1", 1, "/"));
	BOOST_CHECK(!client.IsConnected());
}

// AsyncConnect + the copying SendContent(const vector&) overload
BOOST_AUTO_TEST_CASE(AsyncConnectAndCopyOverload, *utf::timeout(60))
{
	CEchoServer server;
	CWebsocketClient client("test_client_async", TestSettings());

	std::promise<void> connected;
	std::promise<std::vector<uint8_t>> echoedContent;
	std::promise<void> disconnected;
	client.RegisterConnectCallback([&connected]() { connected.set_value(); });
	client.RegisterContentCallback(
		[&echoedContent](const std::vector<uint8_t>& c) { echoedContent.set_value(c); });
	client.RegisterDisconnectCallback([&disconnected]() { disconnected.set_value(); });

	client.AsyncConnect("127.0.0.1", server.Port(), "/");
	auto connectedFuture = connected.get_future();
	BOOST_REQUIRE_MESSAGE(WaitFor(connectedFuture), "AsyncConnect fired connect callback");

	const std::vector<uint8_t> payload{1, 2, 3};
	client.SendContent(payload);
	auto contentFuture = echoedContent.get_future();
	BOOST_REQUIRE_MESSAGE(WaitFor(contentFuture), "binary payload echoed (copy overload)");
	const std::vector<uint8_t> received = contentFuture.get();
	BOOST_CHECK_EQUAL_COLLECTIONS(received.begin(), received.end(), payload.begin(), payload.end());

	client.Close();
	auto disconnectedFuture = disconnected.get_future();
	BOOST_CHECK_MESSAGE(WaitFor(disconnectedFuture), "clean close before destruction");
}
