/// TLS wire tests for CWebsocketClient / CWebsocketServer using an in-process
/// self-signed certificate:
///  - wss:// echo round trip (sslRequired), isSsl reported on both ends
///  - ssl auto-detection accepting TLS and plain clients on one port
///  - sslRequired rejecting plain clients
///  - a plain server rejecting a TLS client
///  - certificate verification: verify_peer succeeds against the trusted test
///    cert and fails against an untrusted (system CA) store
///  - https request handling on the ssl http path
///  - client destruction mid-TLS-handshake
///
/// All Boost.Test assertions run on the main thread; callbacks only signal
/// through promises and atomics.

#define BOOST_TEST_MODULE WebsocketTls
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>

#include "CWebsocketClient.h"
#include "CWebsocketServer.h"
#include "test_helpers.h"
#include "tls_helpers.h"

namespace utf = boost::unit_test;
namespace http = boost::beast::http;
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
namespace beast = boost::beast;
using tcp = net::ip::tcp;

using testhelpers::CSilentServer;
using testhelpers::CTestServerSslContext;
using testhelpers::CTrustingClientSslContext;
using testhelpers::PollUntil;
using testhelpers::WaitFor;
using websocketclient::CWebsocketClient;
using websocketclient::CWebsocketServer;

namespace
{

CWebsocketServer::CServerSettings TlsServerSettings(bool sslRequired)
{
	CWebsocketServer::CServerSettings settings;
	settings.handshakeTimeoutS = 5;
	settings.idleTimeoutS = 30;
	settings.sslContext = std::make_shared<CTestServerSslContext>();
	settings.sslRequired = sslRequired;
	return settings;
}

CWebsocketClient::CClientSettings TlsClientSettings(const sslcontext::ISslContextPtr& sslContext)
{
	CWebsocketClient::CClientSettings settings;
	settings.handshakeTimeoutS = 5;
	settings.idleTimeoutS = 30;
	settings.sslContext = sslContext;
	return settings;
}

CWebsocketClient::CClientSettings PlainClientSettings()
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
};

/// Minimal synchronous https client (trusts nothing: verify_none) for the ssl http path
SHttpResult DoHttpsRequest(unsigned short port, const std::string& target)
{
	net::io_context ioc;
	ssl::context sslCtx(ssl::context::tls_client);
	sslCtx.set_verify_mode(ssl::verify_none);
	beast::ssl_stream<beast::tcp_stream> stream(ioc, sslCtx);
	beast::get_lowest_layer(stream).connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), port));
	stream.handshake(ssl::stream_base::client);

	http::request<http::string_body> request{http::verb::get, target, 11};
	request.set(http::field::host, "127.0.0.1");
	http::write(stream, request);

	beast::flat_buffer buffer;
	http::response<http::string_body> response;
	http::read(stream, buffer, response);

	beast::error_code ec;
	stream.shutdown(ec); // self-signed peers often skip the close_notify; ignore

	SHttpResult result;
	result.status = response.result_int();
	result.body = response.body();
	return result;
}

} // namespace

BOOST_AUTO_TEST_CASE(TlsEchoRoundTrip, *utf::timeout(60))
{
	CWebsocketServer server("tls_server", TlsServerSettings(true));

	std::promise<std::pair<uint32_t, bool>> added;
	std::promise<std::string> serverGotText;
	std::promise<std::vector<uint8_t>> serverGotBinary;
	std::promise<uint32_t> closed;
	CWebsocketServer::CClientCallbacks callbacks;
	callbacks.mOnClientAddedCb = [&](uint32_t id, const std::string&, bool isSsl) {
		added.set_value({id, isSsl});
	};
	callbacks.mOnClientMessageReceivedCb = [&](uint32_t, const std::string& m) {
		serverGotText.set_value(m);
	};
	callbacks.mOnClientContentReceivedCb = [&](uint32_t, const std::vector<uint8_t>& c) {
		serverGotBinary.set_value(c);
	};
	callbacks.mOnClientClosedCb = [&](uint32_t id) { closed.set_value(id); };
	server.Start(static_cast<unsigned short>(0), callbacks);

	// Self-signed server, so the client skips verification here (the verify tests below
	// cover the verifying paths)
	CWebsocketClient client("tls_client", TlsClientSettings(sslcontext::MakeSslContext(false)));
	std::promise<std::string> clientGotText;
	std::promise<std::vector<uint8_t>> clientGotBinary;
	client.RegisterMessageCallback([&](const std::string& m) { clientGotText.set_value(m); });
	client.RegisterContentCallback([&](const std::vector<uint8_t>& c) { clientGotBinary.set_value(c); });

	BOOST_REQUIRE_MESSAGE(client.Connect("127.0.0.1", server.Port(), "/"), "wss connect succeeded");
	BOOST_CHECK(client.IsConnected());

	auto addedFuture = added.get_future();
	BOOST_REQUIRE(WaitFor(addedFuture));
	const auto addedInfo = addedFuture.get();
	BOOST_CHECK_MESSAGE(addedInfo.second, "server reports the client as ssl");
	const auto ids = server.GetConnectedClientIds();
	BOOST_REQUIRE_EQUAL(ids.size(), 1u);
	BOOST_CHECK_MESSAGE(ids.begin()->second, "GetConnectedClientIds reports ssl");

	// Echo both payload kinds in both directions over TLS
	client.SendMessage("secret text");
	auto serverTextFuture = serverGotText.get_future();
	BOOST_REQUIRE(WaitFor(serverTextFuture));
	BOOST_CHECK_EQUAL(serverTextFuture.get(), "secret text");

	const std::vector<uint8_t> payload{0xDE, 0xAD, 0xBE, 0xEF};
	client.SendContent(payload);
	auto serverBinaryFuture = serverGotBinary.get_future();
	BOOST_REQUIRE(WaitFor(serverBinaryFuture));
	const auto serverBinary = serverBinaryFuture.get();
	BOOST_CHECK_EQUAL_COLLECTIONS(serverBinary.begin(), serverBinary.end(), payload.begin(), payload.end());

	server.SendMessage(addedInfo.first, "secret reply");
	auto clientTextFuture = clientGotText.get_future();
	BOOST_REQUIRE(WaitFor(clientTextFuture));
	BOOST_CHECK_EQUAL(clientTextFuture.get(), "secret reply");

	server.SendContent(addedInfo.first, payload);
	auto clientBinaryFuture = clientGotBinary.get_future();
	BOOST_REQUIRE(WaitFor(clientBinaryFuture));

	client.Close();
	auto closedFuture = closed.get_future();
	BOOST_CHECK_MESSAGE(WaitFor(closedFuture), "clean close over TLS");
}

BOOST_AUTO_TEST_CASE(SslDetectionAcceptsBoth, *utf::timeout(60))
{
	CWebsocketServer server("tls_detect_server", TlsServerSettings(false));

	std::mutex addedMutex;
	std::map<uint32_t, bool> addedClients;
	CWebsocketServer::CClientCallbacks callbacks;
	callbacks.mOnClientAddedCb = [&](uint32_t id, const std::string&, bool isSsl) {
		std::lock_guard<std::mutex> lock(addedMutex);
		addedClients[id] = isSsl;
	};
	server.Start(static_cast<unsigned short>(0), callbacks);

	CWebsocketClient tlsClient("detect_tls_client", TlsClientSettings(sslcontext::MakeSslContext(false)));
	CWebsocketClient plainClient("detect_plain_client", PlainClientSettings());
	std::promise<std::string> tlsGot;
	std::promise<std::string> plainGot;
	tlsClient.RegisterMessageCallback([&](const std::string& m) { tlsGot.set_value(m); });
	plainClient.RegisterMessageCallback([&](const std::string& m) { plainGot.set_value(m); });

	BOOST_REQUIRE_MESSAGE(tlsClient.Connect("127.0.0.1", server.Port(), "/"), "TLS client accepted");
	BOOST_REQUIRE_MESSAGE(plainClient.Connect("127.0.0.1", server.Port(), "/"), "plain client accepted");
	BOOST_REQUIRE(PollUntil([&]() {
		std::lock_guard<std::mutex> lock(addedMutex);
		return addedClients.size() == 2;
	}));
	{
		std::lock_guard<std::mutex> lock(addedMutex);
		int sslCount = 0;
		for (const auto& entry : addedClients)
		{
			sslCount += entry.second ? 1 : 0;
		}
		BOOST_CHECK_MESSAGE(sslCount == 1, "exactly one client detected as ssl");
	}

	// Both receive a broadcast despite different transports
	server.SendMessage("mixed transports");
	auto tlsFuture = tlsGot.get_future();
	auto plainFuture = plainGot.get_future();
	BOOST_CHECK(WaitFor(tlsFuture));
	BOOST_CHECK(WaitFor(plainFuture));

	// The plain http path works on the detection port too
	// (no handler registered -> standard 404 json proves the request was parsed)
	net::io_context ioc;
	beast::tcp_stream stream(ioc);
	stream.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), server.Port()));
	http::request<http::string_body> request{http::verb::get, "/nothing", 11};
	request.set(http::field::host, "127.0.0.1");
	http::write(stream, request);
	beast::flat_buffer buffer;
	http::response<http::string_body> response;
	http::read(stream, buffer, response);
	BOOST_CHECK_EQUAL(response.result_int(), 404u);
}

BOOST_AUTO_TEST_CASE(SslRequiredRejectsPlainClient, *utf::timeout(60))
{
	BOOST_TEST_MESSAGE("client-side handshake errors are expected here");
	CWebsocketServer server("tls_strict_server", TlsServerSettings(true));
	server.Start(static_cast<unsigned short>(0), CWebsocketServer::CClientCallbacks());

	CWebsocketClient plainClient("strict_plain_client", PlainClientSettings());
	BOOST_CHECK_MESSAGE(!plainClient.Connect("127.0.0.1", server.Port(), "/"),
						"plain client rejected by ssl-required server");
	BOOST_CHECK(!server.ClientsConnected());

	// A TLS client still gets in
	CWebsocketClient tlsClient("strict_tls_client", TlsClientSettings(sslcontext::MakeSslContext(false)));
	BOOST_CHECK(tlsClient.Connect("127.0.0.1", server.Port(), "/"));
}

BOOST_AUTO_TEST_CASE(PlainServerRejectsTlsClient, *utf::timeout(60))
{
	BOOST_TEST_MESSAGE("client-side ssl handshake errors are expected here");
	CWebsocketServer server("plain_server", []() {
		CWebsocketServer::CServerSettings settings;
		settings.handshakeTimeoutS = 5;
		return settings;
	}());
	server.Start(static_cast<unsigned short>(0), CWebsocketServer::CClientCallbacks());

	CWebsocketClient tlsClient("lost_tls_client", TlsClientSettings(sslcontext::MakeSslContext(false)));
	BOOST_CHECK_MESSAGE(!tlsClient.Connect("127.0.0.1", server.Port(), "/"),
						"TLS client rejected by plain server");
	BOOST_CHECK(!server.ClientsConnected());
}

BOOST_AUTO_TEST_CASE(CertificateVerification, *utf::timeout(60))
{
	BOOST_TEST_MESSAGE("a client-side certificate verify error is expected here");
	CWebsocketServer server("tls_verify_server", TlsServerSettings(true));
	server.Start(static_cast<unsigned short>(0), CWebsocketServer::CClientCallbacks());

	// verify_peer against a store that trusts the test certificate: succeeds
	CWebsocketClient trustingClient("verify_trusting_client",
									TlsClientSettings(std::make_shared<CTrustingClientSslContext>()));
	BOOST_CHECK_MESSAGE(trustingClient.Connect("127.0.0.1", server.Port(), "/"),
						"verify_peer succeeds against the trusted test certificate");
	trustingClient.Close();

	// verify_peer against the system CA store: the self-signed cert fails verification
	CWebsocketClient untrustingClient("verify_untrusting_client",
									  TlsClientSettings(sslcontext::MakeSslContext(true)));
	BOOST_CHECK_MESSAGE(!untrustingClient.Connect("127.0.0.1", server.Port(), "/"),
						"verify_peer fails for an untrusted self-signed certificate");
}

BOOST_AUTO_TEST_CASE(HttpsRequest, *utf::timeout(60))
{
	CWebsocketServer server("tls_http_server", TlsServerSettings(true));
	CWebsocketServer::CUriCallbacks apiCallbacks;
	apiCallbacks.mOnRequestReceivedCb = [](const CWebsocketServer::CHttpWrapperPtr& wrapper) {
		const std::string body = "{\"secure\": true}";
		wrapper->HttpMsg()->body.assign(body.begin(), body.end());
		wrapper->Reply();
	};
	server.AddUri("/api", apiCallbacks);
	server.Start(static_cast<unsigned short>(0), CWebsocketServer::CClientCallbacks());

	const auto result = DoHttpsRequest(server.Port(), "/api/status");
	BOOST_CHECK_EQUAL(result.status, 200u);
	BOOST_CHECK_EQUAL(result.body, "{\"secure\": true}");

	const auto missing = DoHttpsRequest(server.Port(), "/nothing");
	BOOST_CHECK_EQUAL(missing.status, 404u);
}

BOOST_AUTO_TEST_CASE(DestructionMidTlsHandshake, *utf::timeout(60))
{
	// The silent server accepts TCP but never answers, so the client parks in
	// the ssl handshake; destroying it there must not hang or leak
	CSilentServer silent;
	for (int iteration = 0; iteration < 5; ++iteration)
	{
		CWebsocketClient client("mid_tls_client", TlsClientSettings(sslcontext::MakeSslContext(false)));
		client.AsyncConnect("127.0.0.1", silent.Port(), "/");
		if (iteration % 2 == 1)
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(5 * iteration));
		}
		// Destructor runs here mid-TLS-handshake
	}
	BOOST_CHECK_MESSAGE(true, "survived 5 destructions mid-TLS-handshake");
}
