#include "CWebsocketServer.h"

#include "CWorkQueue.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/detect_ssl.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>

namespace websocketclient
{

namespace
{

namespace beast = boost::beast;
namespace websocket = boost::beast::websocket;
namespace http = boost::beast::http;
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
using tcp = boost::asio::ip::tcp;

using CPlainStream = beast::tcp_stream;
using CSslStream = ssl::stream<beast::tcp_stream>;
using CHttpRequest = http::request<http::vector_body<uint8_t>>;
using CHttpResponse = http::response<http::vector_body<uint8_t>>;

const size_t HTTP_BODY_LIMIT = 8 * 1024 * 1024;

std::string DescribeError(const std::string& stage, const beast::error_code& ec)
{
	std::ostringstream description;
	description << stage << " failed: " << ec.message() << " (" << ec << ")";
	return description.str();
}

/// Payload shared across sessions so broadcasts never copy per client
struct SPayload
{
	bool isText = false;
	std::shared_ptr<const std::string> text;
	std::shared_ptr<const std::vector<uint8_t>> content;

	net::const_buffer Buffer() const
	{
		return isText ? net::buffer(*text) : net::buffer(*content);
	}
};

SPayload MakeTextPayload(std::string message)
{
	SPayload payload;
	payload.isText = true;
	payload.text = std::make_shared<const std::string>(std::move(message));
	return payload;
}

SPayload MakeContentPayload(std::vector<uint8_t> content)
{
	SPayload payload;
	payload.isText = false;
	payload.content = std::make_shared<const std::vector<uint8_t>>(std::move(content));
	return payload;
}

std::string UrlDecode(const std::string& in)
{
	std::string out;
	out.reserve(in.size());
	for (size_t i = 0; i < in.size(); ++i)
	{
		if (in[i] == '%' && i + 2 < in.size() && std::isxdigit(in[i + 1]) && std::isxdigit(in[i + 2]))
		{
			out += static_cast<char>(std::stoi(in.substr(i + 1, 2), nullptr, 16));
			i += 2;
		}
		else if (in[i] == '+')
		{
			out += ' ';
		}
		else
		{
			out += in[i];
		}
	}
	return out;
}

/// Splits a request target into path and decoded query parameters
void ParseTarget(const std::string& target, std::string& path,
				 CWebsocketServer::HttpMsg::HttpQueryParameters& parameters)
{
	const size_t queryPos = target.find('?');
	path = target.substr(0, queryPos);
	if (queryPos == std::string::npos)
	{
		return;
	}
	std::string query = target.substr(queryPos + 1);
	size_t start = 0;
	while (start <= query.size())
	{
		size_t end = query.find('&', start);
		if (end == std::string::npos)
		{
			end = query.size();
		}
		const std::string pair = query.substr(start, end - start);
		if (!pair.empty())
		{
			const size_t eq = pair.find('=');
			if (eq == std::string::npos)
			{
				parameters[UrlDecode(pair)] = "";
			}
			else
			{
				parameters[UrlDecode(pair.substr(0, eq))] = UrlDecode(pair.substr(eq + 1));
			}
		}
		start = end + 1;
	}
}

/// true if path lies within uri using path-segment boundaries ("/a" matches "/a" and "/a/b", not "/ab")
bool UriMatches(const std::string& uri, const std::string& path)
{
	if (uri.empty() || path.compare(0, uri.size(), uri) != 0)
	{
		return false;
	}
	return path.size() == uri.size() || path[uri.size()] == '/' || uri.back() == '/';
}

/// A single connected websocket client from the server's perspective
class IWsSession
{
  public:
	virtual ~IWsSession() = default;
	virtual void Send(SPayload payload) = 0;
	virtual void Close() = 0;
};

/// Sink for connection events, implemented by CWebsocketServer::CImpl.
/// Sessions hold this weakly so a dying server never has events fired into it
class IServerEvents
{
  public:
	virtual ~IServerEvents() = default;
	/// Registers an accepted websocket client. Returns the assigned client id, or 0 if the
	/// server is shutting down (the session should close)
	virtual uint32_t RegisterWsClient(const std::shared_ptr<IWsSession>& session, const std::string& target,
									  bool isSsl) = 0;
	virtual void OnWsClosed(uint32_t clientId) = 0;
	virtual void OnWsMessage(uint32_t clientId, std::string message) = 0;
	virtual void OnWsContent(uint32_t clientId, std::vector<uint8_t> content) = 0;
	/// @return whether a websocket upgrade for this path passes the allowedUris filter
	virtual bool UpgradeAllowed(const std::string& path) = 0;
	/// @return the http request handler for this path, or an empty function if none
	virtual CWebsocketServer::OnRequestReceivedCb FindHttpHandler(const std::string& path) = 0;
	virtual uint32_t NextConnectionId() = 0;
	virtual void PostCallback(std::function<void()> fn) = 0;
	virtual void LogError(const std::string& message) = 0;
};

// ---------------------------------------------------------------------------
// Websocket session (templated over plain/ssl stream)
// ---------------------------------------------------------------------------

template <class TStream>
class CWsSession : public IWsSession, public std::enable_shared_from_this<CWsSession<TStream>>
{
	static constexpr bool IS_SSL = std::is_same<TStream, CSslStream>::value;

  public:
	CWsSession(TStream&& stream, const CWebsocketServer::CServerSettings& settings,
			   std::weak_ptr<IServerEvents> events)
		: mWs(std::move(stream))
		, mSettings(settings)
		, mEvents(std::move(events))
	{
	}

	/// Performs the websocket accept for an already-read upgrade request, then registers
	/// with the server and starts reading. Must be called on the stream's executor
	void Accept(const CHttpRequest& request)
	{
		mTarget = std::string(request.target());

		// The websocket stream manages timeouts from here on
		beast::get_lowest_layer(mWs).expires_never();
		websocket::stream_base::timeout timeoutOptions{};
		timeoutOptions.handshake_timeout = std::chrono::seconds(mSettings.handshakeTimeoutS);
		if (mSettings.enablePings)
		{
			timeoutOptions.idle_timeout = std::chrono::seconds(mSettings.idleTimeoutS);
			timeoutOptions.keep_alive_pings = true;
		}
		else
		{
			timeoutOptions.idle_timeout = websocket::stream_base::none();
			timeoutOptions.keep_alive_pings = false;
		}
		mWs.set_option(timeoutOptions);
		mWs.set_option(websocket::stream_base::decorator([](websocket::response_type& response) {
			response.set(http::field::server, std::string(BOOST_BEAST_VERSION_STRING) + " CWebsocketServer");
		}));

		auto self = this->shared_from_this();
		auto requestCopy = std::make_shared<CHttpRequest>(request);
		mWs.async_accept(*requestCopy,
						 [self, requestCopy](beast::error_code ec) { self->OnAccept(ec); });
	}

	void Send(SPayload payload) override
	{
		auto self = this->shared_from_this();
		net::post(mWs.get_executor(), [self, payload = std::move(payload)]() mutable {
			if (!self->mOpen || self->mCloseRequested)
			{
				return;
			}
			if (self->mSettings.maxSessionBacklog > 0 &&
				self->mWriteQueue.size() >= self->mSettings.maxSessionBacklog)
			{
				if (!self->mDropLogged)
				{
					self->mDropLogged = true;
					self->LogError("Session backlog full (" +
								   std::to_string(self->mSettings.maxSessionBacklog) +
								   "); dropping payload(s) for client " + std::to_string(self->mClientId));
				}
				return;
			}
			self->mWriteQueue.push_back(std::move(payload));
			if (self->mWriteQueue.size() == 1)
			{
				self->DoWrite();
			}
		});
	}

	void Close() override
	{
		auto self = this->shared_from_this();
		net::post(mWs.get_executor(), [self]() {
			self->mCloseRequested = true;
			if (!self->mOpen)
			{
				beast::get_lowest_layer(self->mWs).cancel();
				return;
			}
			self->MaybeSendClose();
		});
	}

  private:
	void OnAccept(beast::error_code ec)
	{
		auto events = mEvents.lock();
		if (ec || !events)
		{
			if (ec && ec != net::error::operation_aborted && events)
			{
				events->LogError(DescribeError("websocket accept", ec));
			}
			return;
		}
		mOpen = true;
		mClientId = events->RegisterWsClient(this->shared_from_this(), mTarget, IS_SSL);
		if (mClientId == 0)
		{
			mCloseRequested = true;
			MaybeSendClose();
			return;
		}
		if (mCloseRequested)
		{
			MaybeSendClose();
		}
		DoRead();
	}

	void DoRead()
	{
		mWs.async_read(mReadBuffer,
					   beast::bind_front_handler(&CWsSession::OnRead, this->shared_from_this()));
	}

	void OnRead(beast::error_code ec, size_t)
	{
		if (ec)
		{
			HandleClosed(ec);
			return;
		}
		if (auto events = mEvents.lock())
		{
			if (mWs.got_text())
			{
				events->OnWsMessage(mClientId, beast::buffers_to_string(mReadBuffer.data()));
			}
			else
			{
				const auto data = mReadBuffer.data();
				const auto* begin = static_cast<const uint8_t*>(data.data());
				events->OnWsContent(mClientId, std::vector<uint8_t>(begin, begin + data.size()));
			}
		}
		mReadBuffer.consume(mReadBuffer.size());
		DoRead();
	}

	void DoWrite()
	{
		const SPayload& payload = mWriteQueue.front();
		mWs.text(payload.isText);
		mWs.async_write(payload.Buffer(),
						beast::bind_front_handler(&CWsSession::OnWrite, this->shared_from_this()));
	}

	void OnWrite(beast::error_code ec, size_t)
	{
		if (ec)
		{
			HandleClosed(ec);
			return;
		}
		mWriteQueue.pop_front();
		if (!mWriteQueue.empty())
		{
			DoWrite();
		}
		else if (mCloseRequested)
		{
			MaybeSendClose();
		}
	}

	/// Initiates the closing handshake once no write is in flight (Beast allows
	/// only one concurrent write-type operation, and async_close is one)
	void MaybeSendClose()
	{
		if (mCloseSent || !mWriteQueue.empty())
		{
			return;
		}
		mCloseSent = true;
		auto self = this->shared_from_this();
		mWs.async_close(websocket::close_code::normal, [self](beast::error_code) {
			// The pending async_read completes with websocket::error::closed,
			// which drives HandleClosed
		});
	}

	void HandleClosed(const beast::error_code& ec)
	{
		mWriteQueue.clear();
		if (!mOpen.exchange(false))
		{
			return;
		}
		// Abrupt client death (eof/reset) is routine for a server; only log surprises
		const bool expected = ec == websocket::error::closed || ec == net::error::operation_aborted ||
							  ec == net::error::eof || ec == net::error::connection_reset ||
							  ec == beast::error::timeout || mCloseRequested;
		if (auto events = mEvents.lock())
		{
			if (!expected)
			{
				events->LogError(DescribeError("client " + std::to_string(mClientId) + " connection", ec));
			}
			events->OnWsClosed(mClientId);
		}
	}

	void LogError(const std::string& message)
	{
		if (auto events = mEvents.lock())
		{
			events->LogError(message);
		}
	}

	websocket::stream<TStream> mWs;
	const CWebsocketServer::CServerSettings mSettings;
	const std::weak_ptr<IServerEvents> mEvents;

	beast::flat_buffer mReadBuffer;
	std::deque<SPayload> mWriteQueue;
	std::string mTarget;
	uint32_t mClientId = 0;

	std::atomic<bool> mOpen{false};
	bool mCloseRequested = false;
	bool mCloseSent = false;
	bool mDropLogged = false;
};

// ---------------------------------------------------------------------------
// Http connection: reads requests, dispatches websocket upgrades and plain
// http requests (templated over plain/ssl stream)
// ---------------------------------------------------------------------------

template <class TStream>
class CHttpConnection : public std::enable_shared_from_this<CHttpConnection<TStream>>
{
	static constexpr bool IS_SSL = std::is_same<TStream, CSslStream>::value;

  public:
	/// Plain constructor. The buffer may carry bytes already read from the socket
	CHttpConnection(beast::tcp_stream&& stream, beast::flat_buffer&& buffer,
					const CWebsocketServer::CServerSettings& settings, std::weak_ptr<IServerEvents> events)
		: mStream(std::move(stream))
		, mBuffer(std::move(buffer))
		, mSettings(settings)
		, mEvents(std::move(events))
	{
	}

	/// Ssl constructor. The buffer may carry bytes already read from the socket, which are
	/// consumed by the ssl handshake
	CHttpConnection(beast::tcp_stream&& stream, ssl::context& sslCtx, beast::flat_buffer&& buffer,
					const CWebsocketServer::CServerSettings& settings, std::weak_ptr<IServerEvents> events)
		: mStream(std::move(stream), sslCtx)
		, mBuffer(std::move(buffer))
		, mSettings(settings)
		, mEvents(std::move(events))
	{
	}

	void Run()
	{
		if constexpr (IS_SSL)
		{
			beast::get_lowest_layer(mStream).expires_after(std::chrono::seconds(mSettings.handshakeTimeoutS));
			auto self = this->shared_from_this();
			mStream.async_handshake(ssl::stream_base::server, mBuffer.data(),
									[self](beast::error_code ec, size_t bytesUsed) {
										if (ec)
										{
											self->LogUnexpected("ssl handshake", ec);
											return;
										}
										self->mBuffer.consume(bytesUsed);
										self->DoRead();
									});
		}
		else
		{
			DoRead();
		}
	}

  private:
	void DoRead()
	{
		mParser.emplace();
		mParser->body_limit(HTTP_BODY_LIMIT);
		beast::get_lowest_layer(mStream).expires_after(std::chrono::seconds(mSettings.handshakeTimeoutS));
		http::async_read(mStream, mBuffer, *mParser,
						 beast::bind_front_handler(&CHttpConnection::OnRead, this->shared_from_this()));
	}

	void OnRead(beast::error_code ec, size_t)
	{
		if (ec)
		{
			if (ec != http::error::end_of_stream)
			{
				LogUnexpected("http read", ec);
			}
			return; // connection object dies, closing the socket
		}
		CHttpRequest request = mParser->release();
		auto events = mEvents.lock();
		if (!events)
		{
			return;
		}

		if (websocket::is_upgrade(request))
		{
			std::string path;
			CWebsocketServer::HttpMsg::HttpQueryParameters ignored;
			ParseTarget(std::string(request.target()), path, ignored);
			if (!events->UpgradeAllowed(path))
			{
				SendErrorResponse(request, 404, "Not Found",
								  CWebsocketServer::HttpErrorMessage(
									  "Not Found", "Websocket uri '" + path + "' is not allowed"),
								  false);
				return;
			}
			auto session = std::make_shared<CWsSession<TStream>>(std::move(mStream), mSettings, mEvents);
			session->Accept(request);
			return; // this connection object dies; the ws session owns the stream now
		}

		HandleHttpRequest(std::move(request), events);
	}

	void HandleHttpRequest(CHttpRequest&& request, const std::shared_ptr<IServerEvents>& events)
	{
		auto msg = std::make_shared<CWebsocketServer::HttpMsg>();
		msg->body = std::move(request.body());
		msg->keepAlive = request.keep_alive();
		msg->version = request.version();
		ParseTarget(std::string(request.target()), msg->uri, msg->queryParameters);
		msg->method = std::string(request.method_string());
		const auto contentType = request[http::field::content_type];
		if (!contentType.empty())
		{
			msg->contextType = std::string(contentType);
		}
		msg->clientId = events->NextConnectionId();

		auto handler = events->FindHttpHandler(msg->uri);
		if (!handler)
		{
			SendErrorResponse(request, 404, "Not Found",
							  CWebsocketServer::HttpErrorMessage(
								  "Not Found", "No handler registered for '" + msg->uri + "'"),
							  request.keep_alive());
			return;
		}

		auto self = this->shared_from_this();
		CWebsocketServer::OnReplyFn replyFn = [self](const CWebsocketServer::HttpMsgPtr& response) {
			net::post(self->mStream.get_executor(), [self, response]() { self->SendResponse(response); });
		};
		auto wrapper = std::make_shared<CWebsocketServer::CHttpWrapper>(msg, replyFn);
		events->PostCallback([handler, wrapper]() { handler(wrapper); });
		// No further reads until the handler replies (SendResponse resumes the read loop)
	}

	void SendResponse(const CWebsocketServer::HttpMsgPtr& msg)
	{
		auto response = std::make_shared<CHttpResponse>();
		response->version(msg->version == 0 ? 11 : msg->version);
		response->result(msg->responseCode);
		if (!msg->reason.empty())
		{
			response->reason(msg->reason);
		}
		response->keep_alive(msg->keepAlive);
		response->set(http::field::content_type, msg->contextType);
		response->body() = msg->body;
		response->prepare_payload();
		DoWrite(response);
	}

	void SendErrorResponse(const CHttpRequest& request, unsigned status, const std::string& reason,
						   const std::string& jsonBody, bool keepAlive)
	{
		auto response = std::make_shared<CHttpResponse>();
		response->version(request.version());
		response->result(status);
		response->reason(reason);
		response->keep_alive(keepAlive);
		response->set(http::field::content_type, "application/json");
		response->body().assign(jsonBody.begin(), jsonBody.end());
		response->prepare_payload();
		DoWrite(response);
	}

	void DoWrite(const std::shared_ptr<CHttpResponse>& response)
	{
		beast::get_lowest_layer(mStream).expires_after(std::chrono::seconds(mSettings.handshakeTimeoutS));
		auto self = this->shared_from_this();
		http::async_write(mStream, *response, [self, response](beast::error_code ec, size_t) {
			if (ec)
			{
				self->LogUnexpected("http write", ec);
				return;
			}
			if (response->keep_alive())
			{
				self->DoRead();
			}
			else
			{
				self->DoShutdown();
			}
		});
	}

	void DoShutdown()
	{
		if constexpr (IS_SSL)
		{
			auto self = this->shared_from_this();
			mStream.async_shutdown([self](beast::error_code) {});
		}
		else
		{
			beast::error_code ec;
			mStream.socket().shutdown(tcp::socket::shutdown_send, ec);
		}
	}

	void LogUnexpected(const std::string& stage, const beast::error_code& ec)
	{
		if (ec == net::error::operation_aborted || ec == beast::error::timeout)
		{
			return;
		}
		if (auto events = mEvents.lock())
		{
			events->LogError(DescribeError(stage, ec));
		}
	}

	TStream mStream;
	beast::flat_buffer mBuffer;
	const CWebsocketServer::CServerSettings mSettings;
	const std::weak_ptr<IServerEvents> mEvents;
	std::optional<http::request_parser<http::vector_body<uint8_t>>> mParser;
};

// ---------------------------------------------------------------------------
// Ssl detector: peeks the first bytes to route a connection to the ssl or
// plain http connection (used when sslContext is set but not required)
// ---------------------------------------------------------------------------

class CDetectSession : public std::enable_shared_from_this<CDetectSession>
{
  public:
	CDetectSession(beast::tcp_stream&& stream, ssl::context& sslCtx,
				   const CWebsocketServer::CServerSettings& settings, std::weak_ptr<IServerEvents> events)
		: mStream(std::move(stream))
		, mSslCtx(sslCtx)
		, mSettings(settings)
		, mEvents(std::move(events))
	{
	}

	void Run()
	{
		mStream.expires_after(std::chrono::seconds(mSettings.handshakeTimeoutS));
		auto self = shared_from_this();
		beast::async_detect_ssl(mStream, mBuffer, [self](beast::error_code ec, bool isSsl) {
			if (ec)
			{
				return;
			}
			if (isSsl)
			{
				std::make_shared<CHttpConnection<CSslStream>>(std::move(self->mStream), self->mSslCtx,
															  std::move(self->mBuffer), self->mSettings,
															  self->mEvents)
					->Run();
			}
			else
			{
				std::make_shared<CHttpConnection<CPlainStream>>(std::move(self->mStream),
																std::move(self->mBuffer), self->mSettings,
																self->mEvents)
					->Run();
			}
		});
	}

  private:
	beast::tcp_stream mStream;
	ssl::context& mSslCtx;
	beast::flat_buffer mBuffer;
	const CWebsocketServer::CServerSettings mSettings;
	const std::weak_ptr<IServerEvents> mEvents;
};

} // namespace

// ---------------------------------------------------------------------------
// CWebsocketServer::CServerSettings
// ---------------------------------------------------------------------------

CWebsocketServer::CServerSettings::CServerSettings()
	: handshakeTimeoutS(30)
	, idleTimeoutS(30)
	, enablePings(true)
	, numThreads(1)
	, sslContext()
	, sslRequired(false)
	, allowedUris()
	, maxSessionBacklog(StandardMaxOutstandingWrites)
	, internalMutex(std::make_shared<std::mutex>())
{
}

// ---------------------------------------------------------------------------
// CWebsocketServer::CHttpWrapper
// ---------------------------------------------------------------------------

CWebsocketServer::CHttpWrapper::CHttpWrapper(const HttpMsgPtr& http, const OnReplyFn& fn)
	: mHttpMsg(http)
	, mReplyFn(fn)
{
}

CWebsocketServer::CHttpWrapper::~CHttpWrapper()
{
	if (mResponseSent || !mReplyFn || !mHttpMsg)
	{
		return;
	}
	mHttpMsg->responseCode = 400;
	mHttpMsg->reason = "Bad Request";
	mHttpMsg->contextType = "application/json";
	const std::string body = HttpErrorMessage("Bad Request", "The request was not handled");
	mHttpMsg->body.assign(body.begin(), body.end());
	try
	{
		mReplyFn(mHttpMsg);
	}
	catch (...)
	{
	}
}

void CWebsocketServer::CHttpWrapper::Reply()
{
	if (mResponseSent)
	{
		std::cerr << "CHttpWrapper::Reply called more than once; ignoring\n" << std::flush;
		return;
	}
	mResponseSent = true;
	if (mReplyFn)
	{
		mReplyFn(mHttpMsg);
	}
}

// ---------------------------------------------------------------------------
// CWebsocketServer::CImpl
// ---------------------------------------------------------------------------

class CWebsocketServer::CImpl : public IServerEvents, public std::enable_shared_from_this<CImpl>
{
  public:
	CImpl(const std::string& name, const CServerSettings& settings)
		: mName(name)
		, mSettings(settings)
		, mMutex(settings.internalMutex ? settings.internalMutex : std::make_shared<std::mutex>())
		, mOwnsPool(!settings.ioPool)
		, mPool(settings.ioPool ? settings.ioPool
								: std::make_shared<CIoPool>(std::max(1, settings.numThreads)))
	{
		// Sessions and connections keep a settings copy; strip the pool reference so
		// orphaned sessions never form an ownership cycle keeping a stopped pool alive
		mSessionSettings = settings;
		mSessionSettings.ioPool.reset();
	}

	~CImpl() override
	{
		Shutdown();
	}

	/// The workqueue is started outside the constructor so shared_from_this is
	/// valid by the time any IO can run
	void StartThreads()
	{
		mWorkQueue.Start();
	}

	void Shutdown()
	{
		bool expected = false;
		if (!mShutdown.compare_exchange_strong(expected, true))
		{
			return;
		}
		Stop();
		if (mOwnsPool)
		{
			// Private pool: prompt teardown, abandoning any in-flight operations
			mPool->Stop();
		}
		// Borrowed pool: the pool keeps running; closed sessions wind down
		// asynchronously and their events are dropped once the queue stops
		mWorkQueue.Stop();
	}

	void Start(const std::string& address, unsigned short port, const CClientCallbacks& clientCbs)
	{
		if (mShutdown)
		{
			throw std::runtime_error("CWebsocketServer is shut down");
		}
		auto acceptor = std::make_shared<tcp::acceptor>(net::make_strand(mPool->Context()));
		const tcp::endpoint endpoint(net::ip::make_address(address), port);
		acceptor->open(endpoint.protocol());
		acceptor->set_option(net::socket_base::reuse_address(true));
		acceptor->bind(endpoint);
		acceptor->listen(net::socket_base::max_listen_connections);
		{
			std::lock_guard<std::mutex> lock(*mMutex);
			if (mAcceptor)
			{
				throw std::runtime_error("CWebsocketServer is already started");
			}
			mAcceptor = acceptor;
			mPort = acceptor->local_endpoint().port();
			mClientCbs = clientCbs;
		}
		DoAccept(acceptor);
	}

	void Stop()
	{
		std::shared_ptr<tcp::acceptor> acceptor;
		std::map<uint32_t, SClientEntry> entries;
		{
			std::lock_guard<std::mutex> lock(*mMutex);
			acceptor.swap(mAcceptor);
			mPort = 0;
			entries.swap(mClients);
			mUriCounts.clear();
		}
		if (acceptor)
		{
			net::post(acceptor->get_executor(), [acceptor]() {
				beast::error_code ec;
				acceptor->close(ec);
			});
		}
		for (auto& entry : entries)
		{
			entry.second.session->Close();
		}
		// Per the header contract, closed callbacks run in the calling thread here
		for (auto& entry : entries)
		{
			NotifyClosedSync(entry.first, entry.second.uri);
		}
	}

	unsigned short Port() const
	{
		std::lock_guard<std::mutex> lock(*mMutex);
		return mPort;
	}

	void RemoveClient(uint32_t clientId)
	{
		SClientEntry entry;
		{
			std::lock_guard<std::mutex> lock(*mMutex);
			auto it = mClients.find(clientId);
			if (it == mClients.end())
			{
				return;
			}
			entry = std::move(it->second);
			mClients.erase(it);
			DecrementUriCountLocked(entry.uri);
		}
		entry.session->Close();
		NotifyClosedSync(clientId, entry.uri);
	}

	void SendToClient(uint32_t clientId, SPayload payload)
	{
		std::shared_ptr<IWsSession> session;
		{
			std::lock_guard<std::mutex> lock(*mMutex);
			auto it = mClients.find(clientId);
			if (it != mClients.end())
			{
				session = it->second.session;
			}
		}
		if (!session)
		{
			LogError("Cannot send: client " + std::to_string(clientId) + " is not connected");
			return;
		}
		session->Send(std::move(payload));
	}

	/// Sends to all clients, or to clients whose matched registered uri equals *uri
	void Broadcast(const std::string* uri, const SPayload& payload)
	{
		std::vector<std::shared_ptr<IWsSession>> sessions;
		{
			std::lock_guard<std::mutex> lock(*mMutex);
			for (const auto& entry : mClients)
			{
				if (!uri || entry.second.uri == *uri)
				{
					sessions.push_back(entry.second.session);
				}
			}
		}
		for (const auto& session : sessions)
		{
			session->Send(payload); // payload shares its data; no copies per client
		}
	}

	bool AnyClients(const std::string* uri)
	{
		std::lock_guard<std::mutex> lock(*mMutex);
		if (!uri)
		{
			return !mClients.empty();
		}
		auto it = mUriCounts.find(*uri);
		return it != mUriCounts.end() && it->second > 0;
	}

	std::map<uint32_t, bool> GetConnectedClientIds()
	{
		std::map<uint32_t, bool> ids;
		std::lock_guard<std::mutex> lock(*mMutex);
		for (const auto& entry : mClients)
		{
			ids[entry.first] = entry.second.isSsl;
		}
		return ids;
	}

	void AddUri(const std::string& uri, const CUriCallbacks& callbacks)
	{
		std::lock_guard<std::mutex> lock(*mMutex);
		mUris[uri] = callbacks;
	}

	void RemoveUri(const std::string& uri)
	{
		std::lock_guard<std::mutex> lock(*mMutex);
		mUris.erase(uri);
	}

	// IServerEvents (invoked from IO threads)

	uint32_t RegisterWsClient(const std::shared_ptr<IWsSession>& session, const std::string& target,
							  bool isSsl) override
	{
		std::string path;
		HttpMsg::HttpQueryParameters ignoredParams;
		ParseTarget(target, path, ignoredParams);

		uint32_t clientId = 0;
		std::string matchedUri;
		size_t uriCount = 0;
		OnClientAddedCbFn addedCb;
		OnConnectionChangeCbFn connectionChangeCb;
		{
			std::lock_guard<std::mutex> lock(*mMutex);
			if (mShutdown || !mAcceptor)
			{
				return 0; // stopping; reject the client
			}
			clientId = mNextConnectionId++;
			matchedUri = LongestUriMatchLocked(path);
			mClients[clientId] = SClientEntry{session, matchedUri, isSsl};
			if (!matchedUri.empty())
			{
				uriCount = ++mUriCounts[matchedUri];
				connectionChangeCb = mUris[matchedUri].mOnConnectionChange;
			}
			addedCb = mClientCbs.mOnClientAddedCb;
		}

		// uriAppend is the target beyond the matched uri; the query string rides along
		std::string base = matchedUri;
		if (base.empty() && mSettings.allowedUris)
		{
			for (const auto& allowed : *mSettings.allowedUris)
			{
				if (UriMatches(allowed, path) && allowed.size() > base.size())
				{
					base = allowed;
				}
			}
		}
		const std::string uriAppend = target.substr(std::min(base.size(), target.size()));

		if (addedCb)
		{
			PostCallback([addedCb, clientId, uriAppend, isSsl]() { addedCb(clientId, uriAppend, isSsl); });
		}
		if (connectionChangeCb)
		{
			PostCallback([connectionChangeCb, uriCount]() { connectionChangeCb(uriCount); });
		}
		return clientId;
	}

	void OnWsClosed(uint32_t clientId) override
	{
		std::string uri;
		size_t uriCount = 0;
		OnClientClosedCbFn closedCb;
		OnConnectionChangeCbFn connectionChangeCb;
		{
			std::lock_guard<std::mutex> lock(*mMutex);
			auto it = mClients.find(clientId);
			if (it == mClients.end())
			{
				return; // already removed via Stop()/RemoveClient(); callback was delivered there
			}
			uri = it->second.uri;
			mClients.erase(it);
			uriCount = DecrementUriCountLocked(uri);
			closedCb = mClientCbs.mOnClientClosedCb;
			if (!uri.empty())
			{
				auto uriIt = mUris.find(uri);
				if (uriIt != mUris.end())
				{
					connectionChangeCb = uriIt->second.mOnConnectionChange;
				}
			}
		}
		if (closedCb)
		{
			PostCallback([closedCb, clientId]() { closedCb(clientId); });
		}
		if (connectionChangeCb)
		{
			PostCallback([connectionChangeCb, uriCount]() { connectionChangeCb(uriCount); });
		}
	}

	void OnWsMessage(uint32_t clientId, std::string message) override
	{
		OnClientMessageReceivedCbFn globalCb;
		OnClientMessageReceivedCbFn uriCb;
		LookupReceiveCallbacksLocked(clientId, globalCb, uriCb, true);
		if (!globalCb && !uriCb)
		{
			return;
		}
		PostCallback([globalCb, uriCb, clientId, message = std::move(message)]() {
			if (globalCb)
			{
				globalCb(clientId, message);
			}
			if (uriCb)
			{
				uriCb(clientId, message);
			}
		});
	}

	void OnWsContent(uint32_t clientId, std::vector<uint8_t> content) override
	{
		OnClientContentReceivedCbFn globalCb;
		OnClientContentReceivedCbFn uriCb;
		LookupReceiveCallbacksLocked(clientId, globalCb, uriCb, false);
		if (!globalCb && !uriCb)
		{
			return;
		}
		PostCallback([globalCb, uriCb, clientId, content = std::move(content)]() {
			if (globalCb)
			{
				globalCb(clientId, content);
			}
			if (uriCb)
			{
				uriCb(clientId, content);
			}
		});
	}

	bool UpgradeAllowed(const std::string& path) override
	{
		if (!mSettings.allowedUris)
		{
			return true;
		}
		for (const auto& allowed : *mSettings.allowedUris)
		{
			if (UriMatches(allowed, path))
			{
				return true;
			}
		}
		return false;
	}

	OnRequestReceivedCb FindHttpHandler(const std::string& path) override
	{
		std::lock_guard<std::mutex> lock(*mMutex);
		const std::string matched = LongestUriMatchLocked(path);
		if (matched.empty())
		{
			return nullptr;
		}
		return mUris[matched].mOnRequestReceivedCb;
	}

	uint32_t NextConnectionId() override
	{
		std::lock_guard<std::mutex> lock(*mMutex);
		return mNextConnectionId++;
	}

	void PostCallback(std::function<void()> fn) override
	{
		mWorkQueue.Post([this, fn = std::move(fn)]() { Invoke(fn); });
	}

	void LogError(const std::string& message) override
	{
		// Single insertion of a pre-built string keeps lines intact across threads
		const std::string line = (mName.empty() ? message : "[" + mName + "] " + message) + "\n";
		std::cerr << line << std::flush;
	}

  private:
	struct SClientEntry
	{
		std::shared_ptr<IWsSession> session;
		std::string uri; //!< matched registered uri at connect time ("" if none)
		bool isSsl = false;
	};

	void DoAccept(const std::shared_ptr<tcp::acceptor>& acceptor)
	{
		std::weak_ptr<CImpl> weakSelf = shared_from_this();
		acceptor->async_accept(net::make_strand(mPool->Context()),
							   [weakSelf, acceptor](beast::error_code ec, tcp::socket socket) {
								   auto self = weakSelf.lock();
								   if (!self || !acceptor->is_open())
								   {
									   return;
								   }
								   if (ec)
								   {
									   if (ec != net::error::operation_aborted)
									   {
										   self->LogError(DescribeError("accept", ec));
										   self->DoAccept(acceptor);
									   }
									   return;
								   }
								   self->OnAccept(std::move(socket));
								   self->DoAccept(acceptor);
							   });
	}

	void OnAccept(tcp::socket&& socket)
	{
		beast::tcp_stream stream(std::move(socket));
		std::weak_ptr<IServerEvents> events = shared_from_this();
		if (mSettings.sslContext && *mSettings.sslContext)
		{
			ssl::context& sslCtx = (*mSettings.sslContext)->Context();
			if (mSettings.sslRequired)
			{
				std::make_shared<CHttpConnection<CSslStream>>(std::move(stream), sslCtx, beast::flat_buffer(),
															  mSessionSettings, events)
					->Run();
			}
			else
			{
				std::make_shared<CDetectSession>(std::move(stream), sslCtx, mSessionSettings, events)->Run();
			}
		}
		else
		{
			std::make_shared<CHttpConnection<CPlainStream>>(std::move(stream), beast::flat_buffer(),
															mSessionSettings, events)
				->Run();
		}
	}

	/// Longest registered uri that path lies within, or "" (call with mMutex held)
	std::string LongestUriMatchLocked(const std::string& path)
	{
		std::string best;
		for (const auto& entry : mUris)
		{
			if (UriMatches(entry.first, path) && entry.first.size() > best.size())
			{
				best = entry.first;
			}
		}
		return best;
	}

	/// Decrements the connection count for uri and returns the new count (call with mMutex held)
	size_t DecrementUriCountLocked(const std::string& uri)
	{
		if (uri.empty())
		{
			return 0;
		}
		auto it = mUriCounts.find(uri);
		if (it == mUriCounts.end() || it->second == 0)
		{
			return 0;
		}
		return --it->second;
	}

	void LookupReceiveCallbacksLocked(uint32_t clientId, OnClientMessageReceivedCbFn& globalMsgCb,
									  OnClientMessageReceivedCbFn& uriMsgCb, bool wantMessage)
	{
		OnClientContentReceivedCbFn unusedGlobal;
		OnClientContentReceivedCbFn unusedUri;
		LookupReceiveCallbacks(clientId, wantMessage, globalMsgCb, uriMsgCb, unusedGlobal, unusedUri);
	}

	void LookupReceiveCallbacksLocked(uint32_t clientId, OnClientContentReceivedCbFn& globalContentCb,
									  OnClientContentReceivedCbFn& uriContentCb, bool wantMessage)
	{
		OnClientMessageReceivedCbFn unusedGlobal;
		OnClientMessageReceivedCbFn unusedUri;
		LookupReceiveCallbacks(clientId, wantMessage, unusedGlobal, unusedUri, globalContentCb, uriContentCb);
	}

	void LookupReceiveCallbacks(uint32_t clientId, bool wantMessage, OnClientMessageReceivedCbFn& globalMsgCb,
								OnClientMessageReceivedCbFn& uriMsgCb,
								OnClientContentReceivedCbFn& globalContentCb,
								OnClientContentReceivedCbFn& uriContentCb)
	{
		std::lock_guard<std::mutex> lock(*mMutex);
		if (wantMessage)
		{
			globalMsgCb = mClientCbs.mOnClientMessageReceivedCb;
		}
		else
		{
			globalContentCb = mClientCbs.mOnClientContentReceivedCb;
		}
		auto it = mClients.find(clientId);
		if (it == mClients.end() || it->second.uri.empty())
		{
			return;
		}
		auto uriIt = mUris.find(it->second.uri);
		if (uriIt == mUris.end())
		{
			return;
		}
		if (wantMessage)
		{
			uriMsgCb = uriIt->second.mOnClientMessageReceivedCb;
		}
		else
		{
			uriContentCb = uriIt->second.mOnClientContentReceivedCb;
		}
	}

	/// Invokes the closed (and uri connection-change) callbacks in the calling thread,
	/// per the Stop()/RemoveClient() contract
	void NotifyClosedSync(uint32_t clientId, const std::string& uri)
	{
		OnClientClosedCbFn closedCb;
		OnConnectionChangeCbFn connectionChangeCb;
		size_t uriCount = 0;
		{
			std::lock_guard<std::mutex> lock(*mMutex);
			closedCb = mClientCbs.mOnClientClosedCb;
			if (!uri.empty())
			{
				auto uriIt = mUris.find(uri);
				if (uriIt != mUris.end())
				{
					connectionChangeCb = uriIt->second.mOnConnectionChange;
				}
				auto countIt = mUriCounts.find(uri);
				uriCount = countIt != mUriCounts.end() ? countIt->second : 0;
			}
		}
		if (closedCb)
		{
			Invoke([&]() { closedCb(clientId); });
		}
		if (connectionChangeCb)
		{
			Invoke([&]() { connectionChangeCb(uriCount); });
		}
	}

	template <class TFn>
	void Invoke(const TFn& fn)
	{
		try
		{
			fn();
		}
		catch (const std::exception& e)
		{
			LogError(std::string("Unhandled exception in callback: ") + e.what());
		}
		catch (...)
		{
			LogError("Unhandled exception in callback");
		}
	}

	const std::string mName;
	const CServerSettings mSettings;
	const std::shared_ptr<std::mutex> mMutex;

	CServerSettings mSessionSettings; //!< settings copy handed to sessions/connections (ioPool cleared)
	const bool mOwnsPool;
	const CIoPoolPtr mPool;
	detail::CWorkQueue mWorkQueue;
	std::atomic<bool> mShutdown{false};

	// All state below is guarded by *mMutex
	std::shared_ptr<tcp::acceptor> mAcceptor;
	unsigned short mPort = 0;
	uint32_t mNextConnectionId = 1;
	CClientCallbacks mClientCbs;
	std::map<uint32_t, SClientEntry> mClients;
	std::map<std::string, CUriCallbacks> mUris;
	std::map<std::string, size_t> mUriCounts;
};

// ---------------------------------------------------------------------------
// CWebsocketServer::CUriWrapper
// ---------------------------------------------------------------------------

CWebsocketServer::CUriWrapper::CUriWrapper(const CWebsocketServerPtr& server, const std::string& uri,
										   const CUriCallbacks& callbacks)
	: mServer(server)
	, mUri(uri)
{
	if (mServer)
	{
		mServer->AddUri(mUri, callbacks);
	}
}

CWebsocketServer::CUriWrapper::~CUriWrapper()
{
	if (mServer)
	{
		mServer->RemoveUri(mUri);
	}
}

void CWebsocketServer::CUriWrapper::SendContent(const std::vector<uint8_t>& content)
{
	if (mServer)
	{
		mServer->SendContent(mUri, content);
	}
}

void CWebsocketServer::CUriWrapper::SendContent(const DelayedContentEvalFn& content)
{
	if (mServer)
	{
		mServer->SendContent(mUri, content);
	}
}

void CWebsocketServer::CUriWrapper::SendMessage(const std::string& message)
{
	if (mServer)
	{
		mServer->SendMessage(mUri, message);
	}
}

void CWebsocketServer::CUriWrapper::SendMessage(const DelayedMessageEvalFn& message)
{
	if (mServer)
	{
		mServer->SendMessage(mUri, message);
	}
}

bool CWebsocketServer::CUriWrapper::ClientsConnected()
{
	return mServer && mServer->ClientsConnected(mUri);
}

// ---------------------------------------------------------------------------
// CWebsocketServer
// ---------------------------------------------------------------------------

CWebsocketServer::CWebsocketServer()
	: CWebsocketServer("CWebsocketServer")
{
}

CWebsocketServer::CWebsocketServer(const std::string& name)
	: CWebsocketServer(name, CServerSettings())
{
}

CWebsocketServer::CWebsocketServer(const std::string& name, const CServerSettings& settings)
	: mImpl(std::make_shared<CImpl>(name, settings))
{
	mImpl->StartThreads();
}

CWebsocketServer::~CWebsocketServer()
{
	if (mImpl)
	{
		mImpl->Shutdown();
	}
}

void CWebsocketServer::Start()
{
	Start("0.0.0.0", 8080, CClientCallbacks());
}

void CWebsocketServer::Start(const CClientCallbacks& clientCbs)
{
	Start("0.0.0.0", 8080, clientCbs);
}

void CWebsocketServer::Start(unsigned short port, const CClientCallbacks& clientCbs)
{
	Start("0.0.0.0", port, clientCbs);
}

void CWebsocketServer::Start(const std::string& address, unsigned short port, const CClientCallbacks& clientCbs)
{
	mImpl->Start(address, port, clientCbs);
}

void CWebsocketServer::Stop()
{
	mImpl->Stop();
}

unsigned short CWebsocketServer::Port() const
{
	return mImpl->Port();
}

void CWebsocketServer::RemoveClient(uint32_t clientId)
{
	mImpl->RemoveClient(clientId);
}

void CWebsocketServer::SendContent(uint32_t clientId, const std::vector<uint8_t>& content)
{
	mImpl->SendToClient(clientId, MakeContentPayload(content));
}

void CWebsocketServer::SendContent(const std::vector<uint8_t>& content)
{
	mImpl->Broadcast(nullptr, MakeContentPayload(content));
}

void CWebsocketServer::SendContent(const std::string& uri, const std::vector<uint8_t>& content)
{
	mImpl->Broadcast(&uri, MakeContentPayload(content));
}

void CWebsocketServer::SendContent(const DelayedContentEvalFn& content)
{
	if (!content || !mImpl->AnyClients(nullptr))
	{
		return;
	}
	std::vector<uint8_t> payload;
	content(payload);
	mImpl->Broadcast(nullptr, MakeContentPayload(std::move(payload)));
}

void CWebsocketServer::SendContent(const std::string& uri, const DelayedContentEvalFn& content)
{
	if (!content || !mImpl->AnyClients(&uri))
	{
		return;
	}
	std::vector<uint8_t> payload;
	content(payload);
	mImpl->Broadcast(&uri, MakeContentPayload(std::move(payload)));
}

void CWebsocketServer::SendMessage(uint32_t clientId, const std::string& message)
{
	mImpl->SendToClient(clientId, MakeTextPayload(message));
}

void CWebsocketServer::SendMessage(const std::string& message)
{
	mImpl->Broadcast(nullptr, MakeTextPayload(message));
}

void CWebsocketServer::SendMessage(const std::string& uri, const std::string& message)
{
	mImpl->Broadcast(&uri, MakeTextPayload(message));
}

void CWebsocketServer::SendMessage(const DelayedMessageEvalFn& message)
{
	if (!message || !mImpl->AnyClients(nullptr))
	{
		return;
	}
	std::string payload;
	message(payload);
	mImpl->Broadcast(nullptr, MakeTextPayload(std::move(payload)));
}

void CWebsocketServer::SendMessage(const std::string& uri, const DelayedMessageEvalFn& message)
{
	if (!message || !mImpl->AnyClients(&uri))
	{
		return;
	}
	std::string payload;
	message(payload);
	mImpl->Broadcast(&uri, MakeTextPayload(std::move(payload)));
}

std::map<uint32_t, bool> CWebsocketServer::GetConnectedClientIds()
{
	return mImpl->GetConnectedClientIds();
}

bool CWebsocketServer::ClientsConnected()
{
	return mImpl->AnyClients(nullptr);
}

bool CWebsocketServer::ClientsConnected(const std::string& uri)
{
	return mImpl->AnyClients(&uri);
}

void CWebsocketServer::AddUri(const std::string& uri, const CUriCallbacks& callbacks)
{
	mImpl->AddUri(uri, callbacks);
}

void CWebsocketServer::RemoveUri(const std::string& uri)
{
	mImpl->RemoveUri(uri);
}

} // namespace websocketclient
