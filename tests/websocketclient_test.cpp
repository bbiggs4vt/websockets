/// Loopback test for CWebsocketClient: spins up a minimal Beast echo server on
/// an ephemeral port, then exercises connect, text/binary send + receive,
/// callbacks, Close(), and failed-connect handling.

#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <thread>

#include "test_helpers.h"
#include "websocket/CWebsocketClient.h"

int main()
{
	using testhelpers::CEchoServer;
	using testhelpers::Check;
	using testhelpers::gFailures;
	using testhelpers::WaitFor;
	using websocketclient::CWebsocketClient;

	// --- happy path: connect, echo text + binary, close ---
	{
		CEchoServer server;

		CWebsocketClient::CClientSettings settings;
		settings.handshakeTimeoutS = 5;
		settings.idleTimeoutS = 10;
		CWebsocketClient client("test_client", settings);

		std::promise<void> connected;
		std::promise<std::string> echoedText;
		std::promise<std::vector<uint8_t>> echoedContent;
		std::promise<void> disconnected;

		client.RegisterConnectCallback([&connected]() { connected.set_value(); });
		client.RegisterMessageCallback([&echoedText](const std::string& m) { echoedText.set_value(m); });
		client.RegisterContentCallback(
			[&echoedContent](const std::vector<uint8_t>& c) { echoedContent.set_value(c); });
		client.RegisterDisconnectCallback([&disconnected]() { disconnected.set_value(); });

		Check(!client.IsConnected(), "not connected before Connect");
		Check(client.Connect("127.0.0.1", server.Port(), "/"), "Connect succeeds");
		Check(client.IsConnected(), "IsConnected after Connect");

		auto connectedFuture = connected.get_future();
		Check(WaitFor(connectedFuture), "connect callback fired");

		client.SendMessage("hello websocket");
		auto textFuture = echoedText.get_future();
		Check(WaitFor(textFuture) && textFuture.get() == "hello websocket", "text payload echoed");

		const std::vector<uint8_t> payload{0x00, 0x01, 0xFE, 0xFF, 0x42};
		client.SendContent(std::make_shared<std::vector<uint8_t>>(payload));
		auto contentFuture = echoedContent.get_future();
		Check(WaitFor(contentFuture) && contentFuture.get() == payload, "binary payload echoed");

		client.Close();
		client.Close(); // may be called multiple times
		auto disconnectedFuture = disconnected.get_future();
		Check(WaitFor(disconnectedFuture), "disconnect callback fired");
		Check(!client.IsConnected(), "not connected after Close");
	}

	// --- failed connect: nothing listening ---
	{
		CWebsocketClient::CClientSettings settings;
		settings.handshakeTimeoutS = 3;
		CWebsocketClient client("test_client_fail", settings);
		Check(!client.Connect("127.0.0.1", 1, "/"), "Connect to closed port fails");
		Check(!client.IsConnected(), "not connected after failed Connect");
	}

	// --- async connect + SendContent(const vector&) overload ---
	{
		CEchoServer server;

		CWebsocketClient::CClientSettings settings;
		settings.handshakeTimeoutS = 5;
		CWebsocketClient client("test_client_async", settings);

		std::promise<void> connected;
		std::promise<std::vector<uint8_t>> echoedContent;
		std::promise<void> disconnected;
		client.RegisterConnectCallback([&connected]() { connected.set_value(); });
		client.RegisterContentCallback(
			[&echoedContent](const std::vector<uint8_t>& c) { echoedContent.set_value(c); });
		client.RegisterDisconnectCallback([&disconnected]() { disconnected.set_value(); });

		client.AsyncConnect("127.0.0.1", server.Port(), "/");
		auto connectedFuture = connected.get_future();
		Check(WaitFor(connectedFuture), "AsyncConnect fired connect callback");

		const std::vector<uint8_t> payload{1, 2, 3};
		client.SendContent(payload);
		auto contentFuture = echoedContent.get_future();
		Check(WaitFor(contentFuture) && contentFuture.get() == payload, "binary payload echoed (copy overload)");

		client.Close();
		auto disconnectedFuture = disconnected.get_future();
		Check(WaitFor(disconnectedFuture), "clean close before destruction");
	}

	if (gFailures == 0)
	{
		std::cout << "\nAll tests passed\n";
		return EXIT_SUCCESS;
	}
	std::cerr << "\n" << gFailures << " test(s) failed\n";
	return EXIT_FAILURE;
}
