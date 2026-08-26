/// Example: connect to a websocket server, send a text + binary payload,
/// print whatever the server sends back, then disconnect.
///
/// Usage: echo_client [host] [port] [resource]
///   e.g. echo_client 127.0.0.1 8080 /
/// Test against any websocket echo server.

#include <chrono>
#include <iostream>
#include <thread>

#include "websocket/CWebsocketClient.h"

int main(int argc, char* argv[])
{
	const std::string host = argc > 1 ? argv[1] : "127.0.0.1";
	const size_t port = argc > 2 ? static_cast<size_t>(std::stoul(argv[2])) : 8080;
	const std::string resource = argc > 3 ? argv[3] : "/";

	websocketclient::CWebsocketClient client("echo_client");

	client.RegisterConnectCallback([]() { std::cout << "connected\n"; });
	client.RegisterDisconnectCallback([]() { std::cout << "disconnected\n"; });
	client.RegisterMessageCallback([](const std::string& message) {
		std::cout << "received text: " << message << "\n";
	});
	client.RegisterContentCallback([](const std::vector<uint8_t>& content) {
		std::cout << "received binary: " << content.size() << " bytes\n";
	});

	if (!client.Connect(host, port, resource))
	{
		std::cerr << "failed to connect to " << host << ":" << port << resource << "\n";
		return 1;
	}

	client.SendMessage("hello over websocket");
	client.SendContent(std::vector<uint8_t>{0xDE, 0xAD, 0xBE, 0xEF});

	// Give the server a moment to respond before disconnecting
	std::this_thread::sleep_for(std::chrono::seconds(2));

	client.Close();
	std::this_thread::sleep_for(std::chrono::milliseconds(250));
	return 0;
}
