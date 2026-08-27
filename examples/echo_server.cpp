/// Example: run a websocket echo server that also answers a simple http GET.
///
/// Usage: echo_server [port]
///   e.g. echo_server 8080
/// Pair with the echo_client example, any websocket client, or curl:
///   curl http://127.0.0.1:8080/api/status

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>

#include "CWebsocketServer.h"

namespace
{
std::atomic<bool> gStop{false};
}

int main(int argc, char* argv[])
{
	const unsigned short port = argc > 1 ? static_cast<unsigned short>(std::stoul(argv[1])) : 8080;

	auto server = std::make_shared<websocketclient::CWebsocketServer>("echo_server");

	websocketclient::CWebsocketServer::CClientCallbacks callbacks;
	callbacks.mOnClientAddedCb = [](uint32_t id, const std::string& uriAppend, bool isSsl) {
		std::cout << "client " << id << " connected (uri append '" << uriAppend << "', ssl " << isSsl << ")\n";
	};
	callbacks.mOnClientClosedCb = [](uint32_t id) { std::cout << "client " << id << " disconnected\n"; };
	callbacks.mOnClientMessageReceivedCb = [&server](uint32_t id, const std::string& message) {
		std::cout << "client " << id << " says: " << message << "\n";
		server->SendMessage(id, message); // echo back
	};
	callbacks.mOnClientContentReceivedCb = [&server](uint32_t id, const std::vector<uint8_t>& content) {
		std::cout << "client " << id << " sent " << content.size() << " bytes\n";
		server->SendContent(id, content); // echo back
	};

	websocketclient::CWebsocketServer::CUriCallbacks apiCallbacks;
	apiCallbacks.mOnRequestReceivedCb =
		[](const websocketclient::CWebsocketServer::CHttpWrapperPtr& wrapper) {
			auto msg = wrapper->HttpMsg();
			std::cout << msg->method << " " << msg->uri << "\n";
			const std::string body = "{\"status\": \"ok\"}";
			msg->body.assign(body.begin(), body.end());
			wrapper->Reply();
		};
	server->AddUri("/api", apiCallbacks);

	try
	{
		server->Start(port, callbacks);
	}
	catch (const std::exception& e)
	{
		std::cerr << "failed to start on port " << port << ": " << e.what() << "\n";
		return 1;
	}
	std::cout << "listening on port " << server->Port() << " (ctrl-c to stop)\n";

	std::signal(SIGINT, [](int) { gStop = true; });
	while (!gStop)
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}
	server->Stop();
	return 0;
}
