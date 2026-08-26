#include "websocket/CWebsocketClient.h"

#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <future>
#include <mutex>
#include <sstream>
#include <thread>
#include <type_traits>
#include <utility>

#include <boost/asio/dispatch.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>

namespace websocketclient
{

namespace
{

namespace beast = boost::beast;
namespace websocket = boost::beast::websocket;
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
using tcp = boost::asio::ip::tcp;

const char* const DEFAULT_HOST = "127.0.0.1";
const size_t DEFAULT_PORT = 8080;
const char* const DEFAULT_RESOURCE = "/";

using CPlainStream = websocket::stream<beast::tcp_stream>;
using CSslStream = websocket::stream<ssl::stream<beast::tcp_stream>>;

std::string DescribeError(const std::string& stage, const beast::error_code& ec)
{
	std::ostringstream description;
	description << stage << " failed: " << ec.message() << " (" << ec << ")";
	return description.str();
}

/// A single outgoing frame. Text is stored by value; binary payloads are shared
/// so SendContent(shared_ptr) never copies the caller's buffer
struct SOutgoingMessage
{
	bool isText = false;
	std::string text;
	std::shared_ptr<const std::vector<uint8_t>> binary;

	net::const_buffer Buffer() const
	{
		return isText ? net::buffer(text) : net::buffer(*binary);
	}
};

/// Sink for session events, implemented by CWebsocketClient::CImpl.
/// Sessions hold this weakly so a dying client never has callbacks fired into it
class ISessionEvents
{
  public:
	virtual ~ISessionEvents() = default;
	virtual void OnSessionMessage(const std::string& message) = 0;
	virtual void OnSessionContent(const std::vector<uint8_t>& content) = 0;
	virtual void OnSessionDisconnect() = 0;
	virtual void LogSessionError(const std::string& message) = 0;
};

/// One websocket connection attempt/lifetime. A new session is created per Connect()
class ISession
{
  public:
	virtual ~ISession() = default;
	virtual void AsyncConnect(const std::string& host, size_t port, const std::string& resource,
							  std::function<void(bool)> onComplete) = 0;
	virtual void Send(SOutgoingMessage message) = 0;
	virtual void Close() = 0;
	virtual bool IsOpen() const = 0;
};

/// Session templated on the websocket stream type (plain TCP or TLS).
/// All stream access happens on mStrand; only mOpen is touched cross-thread
template <class TStream>
class CSession : public ISession, public std::enable_shared_from_this<CSession<TStream>>
{
	static constexpr bool IS_SSL = std::is_same<TStream, CSslStream>::value;

  public:
	/// Plain (non-TLS) session
	CSession(net::io_context& ioc, const CWebsocketClient::CClientSettings& settings,
			 std::weak_ptr<ISessionEvents> events)
		: mSettings(settings)
		, mEvents(std::move(events))
		, mStrand(net::make_strand(ioc))
		, mResolver(mStrand)
		, mWs(mStrand)
	{
	}

	/// TLS session. Keeps the ssl context alive for the session's lifetime
	CSession(net::io_context& ioc, const CWebsocketClient::CClientSettings& settings,
			 std::weak_ptr<ISessionEvents> events, const sslcontext::ISslContextPtr& sslContext)
		: mSettings(settings)
		, mEvents(std::move(events))
		, mSslContext(sslContext)
		, mStrand(net::make_strand(ioc))
		, mResolver(mStrand)
		, mWs(mStrand, sslContext->Context())
	{
	}

	void AsyncConnect(const std::string& host, size_t port, const std::string& resource,
					  std::function<void(bool)> onComplete) override
	{
		auto self = this->shared_from_this();
		net::dispatch(mStrand, [self, host, port, resource, onComplete]() mutable {
			self->mHost = host;
			self->mPort = port;
			self->mResource = resource.empty() ? DEFAULT_RESOURCE : resource;
			self->mHostHeader = host + ":" + std::to_string(port);
			self->mOnConnectComplete = std::move(onComplete);
			self->mResolver.async_resolve(self->mHost, std::to_string(self->mPort),
										  beast::bind_front_handler(&CSession::OnResolve, self));
		});
	}

	void Send(SOutgoingMessage message) override
	{
		auto self = this->shared_from_this();
		net::post(mStrand, [self, message = std::move(message)]() mutable {
			if (!self->mOpen || self->mCloseRequested)
			{
				self->LogError("Send dropped: not connected");
				return;
			}
			self->mWriteQueue.push_back(std::move(message));
			// If a write is already in flight, OnWrite will drain the queue
			if (self->mWriteQueue.size() == 1)
			{
				self->DoWrite();
			}
		});
	}

	void Close() override
	{
		auto self = this->shared_from_this();
		net::post(mStrand, [self]() {
			self->mCloseRequested = true;
			if (!self->mOpen)
			{
				// Still connecting (or already closed): abort any pending operations
				self->mResolver.cancel();
				beast::get_lowest_layer(self->mWs).cancel();
				return;
			}
			self->MaybeSendClose();
		});
	}

	bool IsOpen() const override
	{
		return mOpen;
	}

  private:
	void OnResolve(beast::error_code ec, tcp::resolver::results_type results)
	{
		if (ec)
		{
			CompleteConnect(false, "resolve", ec);
			return;
		}
		// Covers the TCP connect and (for TLS) the ssl handshake below
		beast::get_lowest_layer(mWs).expires_after(std::chrono::seconds(mSettings.handshakeTimeoutS));
		beast::get_lowest_layer(mWs).async_connect(results,
												   beast::bind_front_handler(&CSession::OnTcpConnect,
																			 this->shared_from_this()));
	}

	void OnTcpConnect(beast::error_code ec, tcp::resolver::results_type::endpoint_type)
	{
		if (ec)
		{
			CompleteConnect(false, "connect", ec);
			return;
		}
		if constexpr (IS_SSL)
		{
			// SNI is required by many servers to select the right certificate
			if (!SSL_set_tlsext_host_name(mWs.next_layer().native_handle(), mHost.c_str()))
			{
				const beast::error_code sniEc{static_cast<int>(::ERR_get_error()), net::error::get_ssl_category()};
				CompleteConnect(false, "SNI", sniEc);
				return;
			}
			mWs.next_layer().async_handshake(ssl::stream_base::client,
											 beast::bind_front_handler(&CSession::OnSslHandshake,
																	   this->shared_from_this()));
		}
		else
		{
			DoWebsocketHandshake();
		}
	}

	void OnSslHandshake(beast::error_code ec)
	{
		if (ec)
		{
			CompleteConnect(false, "ssl handshake", ec);
			return;
		}
		DoWebsocketHandshake();
	}

	void DoWebsocketHandshake()
	{
		// The websocket stream manages timeouts from here on
		beast::get_lowest_layer(mWs).expires_never();

		websocket::stream_base::timeout timeoutOptions{};
		timeoutOptions.handshake_timeout = std::chrono::seconds(mSettings.handshakeTimeoutS);
		if (mSettings.enablePings)
		{
			// Beast pings after idle_timeout/2 with no traffic, and disconnects at idle_timeout
			timeoutOptions.idle_timeout = std::chrono::seconds(mSettings.idleTimeoutS);
			timeoutOptions.keep_alive_pings = true;
		}
		else
		{
			timeoutOptions.idle_timeout = websocket::stream_base::none();
			timeoutOptions.keep_alive_pings = false;
		}
		mWs.set_option(timeoutOptions);
		mWs.set_option(websocket::stream_base::decorator([](websocket::request_type& request) {
			request.set(beast::http::field::user_agent,
						std::string(BOOST_BEAST_VERSION_STRING) + " CWebsocketClient");
		}));

		mWs.async_handshake(mHostHeader, mResource,
							beast::bind_front_handler(&CSession::OnWebsocketHandshake, this->shared_from_this()));
	}

	void OnWebsocketHandshake(beast::error_code ec)
	{
		if (ec)
		{
			CompleteConnect(false, "websocket handshake", ec);
			return;
		}
		mOpen = true;
		CompleteConnect(true, "", {});
		if (mCloseRequested)
		{
			MaybeSendClose();
		}
		DoRead();
	}

	void CompleteConnect(bool success, const std::string& stage, const beast::error_code& ec)
	{
		if (!success && ec != net::error::operation_aborted)
		{
			LogError(DescribeError(stage, ec));
		}
		if (mOnConnectComplete)
		{
			auto onComplete = std::move(mOnConnectComplete);
			mOnConnectComplete = nullptr;
			onComplete(success);
		}
	}

	void DoRead()
	{
		mWs.async_read(mReadBuffer, beast::bind_front_handler(&CSession::OnRead, this->shared_from_this()));
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
				events->OnSessionMessage(beast::buffers_to_string(mReadBuffer.data()));
			}
			else
			{
				const auto data = mReadBuffer.data();
				const auto* begin = static_cast<const uint8_t*>(data.data());
				events->OnSessionContent(std::vector<uint8_t>(begin, begin + data.size()));
			}
		}
		mReadBuffer.consume(mReadBuffer.size());
		DoRead();
	}

	void DoWrite()
	{
		const SOutgoingMessage& message = mWriteQueue.front();
		mWs.text(message.isText);
		mWs.async_write(message.Buffer(),
						beast::bind_front_handler(&CSession::OnWrite, this->shared_from_this()));
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
			// which drives the disconnect notification in HandleClosed
		});
	}

	void HandleClosed(const beast::error_code& ec)
	{
		mWriteQueue.clear();
		if (!mOpen.exchange(false))
		{
			return;
		}
		const bool expected = ec == websocket::error::closed || ec == net::error::operation_aborted ||
							  mCloseRequested;
		if (!expected)
		{
			LogError(DescribeError("connection", ec));
		}
		if (auto events = mEvents.lock())
		{
			events->OnSessionDisconnect();
		}
	}

	void LogError(const std::string& message)
	{
		if (auto events = mEvents.lock())
		{
			events->LogSessionError(message);
		}
	}

	const CWebsocketClient::CClientSettings mSettings;
	const std::weak_ptr<ISessionEvents> mEvents;
	const sslcontext::ISslContextPtr mSslContext;

	net::strand<net::io_context::executor_type> mStrand;
	tcp::resolver mResolver;
	TStream mWs;
	beast::flat_buffer mReadBuffer;
	std::deque<SOutgoingMessage> mWriteQueue;

	std::string mHost;
	size_t mPort = DEFAULT_PORT;
	std::string mResource;
	std::string mHostHeader;
	std::function<void(bool)> mOnConnectComplete;

	std::atomic<bool> mOpen{false};
	bool mCloseRequested = false;
	bool mCloseSent = false;
};

} // namespace

// ---------------------------------------------------------------------------
// CWebsocketClient::CClientSettings
// ---------------------------------------------------------------------------

CWebsocketClient::CClientSettings::CClientSettings()
	: handshakeTimeoutS(30)
	, idleTimeoutS(30)
	, enablePings(true)
	, numThreads(1)
	, sslContext()
{
}

// ---------------------------------------------------------------------------
// CWebsocketClient::CImpl
// ---------------------------------------------------------------------------

class CWebsocketClient::CImpl : public ISessionEvents, public std::enable_shared_from_this<CImpl>
{
  public:
	CImpl(const std::string& name, const core::errorlogger::CLogger& logger, const CClientSettings& settings)
		: mName(name)
		, mLogger(logger)
		, mSettings(settings)
		, mIoc(std::max(1, (settings.numThreads + 1) * 2))
		, mWorkGuard(net::make_work_guard(mIoc))
	{
	}

	~CImpl() override
	{
		Shutdown();
	}

	/// Threads are started outside the constructor so shared_from_this is valid
	/// by the time any IO can run
	void Start()
	{
		const int threadCount = std::max(1, (mSettings.numThreads + 1) * 2);
		mThreads.reserve(threadCount);
		for (int i = 0; i < threadCount; ++i)
		{
			mThreads.emplace_back([this]() { mIoc.run(); });
		}
	}

	void Shutdown()
	{
		bool expected = false;
		if (!mShutdown.compare_exchange_strong(expected, true))
		{
			return;
		}
		std::shared_ptr<ISession> session;
		{
			std::lock_guard<std::mutex> lock(mSessionMutex);
			session = std::move(mSession);
		}
		if (session)
		{
			session->Close();
		}
		mWorkGuard.reset();
		mIoc.stop();
		for (std::thread& thread : mThreads)
		{
			if (!thread.joinable())
			{
				continue;
			}
			if (thread.get_id() == std::this_thread::get_id())
			{
				thread.detach();
			}
			else
			{
				thread.join();
			}
		}
	}

	bool Connect(const std::string& host, size_t port, const std::string& resource)
	{
		if (mShutdown)
		{
			return false;
		}
		auto result = std::make_shared<std::promise<bool>>();
		std::future<bool> future = result->get_future();
		DoConnect(host, port, resource, [result](bool success) { result->set_value(success); });
		// The handshake timeouts bound each connect stage; this outer wait only
		// guards against a stuck resolver so a synchronous call can never hang
		const auto safetyTimeout = std::chrono::seconds(mSettings.handshakeTimeoutS * 3 + 5);
		if (future.wait_for(safetyTimeout) != std::future_status::ready)
		{
			mLogger.Error(Prefixed("Connect timed out"));
			return false;
		}
		try
		{
			return future.get();
		}
		catch (const std::future_error&)
		{
			return false;
		}
	}

	void AsyncConnect(const std::string& host, size_t port, const std::string& resource)
	{
		if (mShutdown)
		{
			return;
		}
		DoConnect(host, port, resource, nullptr);
	}

	bool IsConnected() const
	{
		std::lock_guard<std::mutex> lock(mSessionMutex);
		return mSession && mSession->IsOpen();
	}

	void Close()
	{
		std::shared_ptr<ISession> session;
		{
			std::lock_guard<std::mutex> lock(mSessionMutex);
			session = mSession;
		}
		if (session)
		{
			session->Close();
		}
	}

	void Send(SOutgoingMessage message)
	{
		std::shared_ptr<ISession> session;
		{
			std::lock_guard<std::mutex> lock(mSessionMutex);
			session = mSession;
		}
		if (!session || !session->IsOpen())
		{
			mLogger.Error(Prefixed("Cannot send: not connected"));
			return;
		}
		session->Send(std::move(message));
	}

	void RegisterConnectCallback(boost::function<void(void)> cb)
	{
		std::lock_guard<std::mutex> lock(mCallbackMutex);
		mOnConnect = std::move(cb);
	}

	void RegisterDisconnectCallback(boost::function<void(void)> cb)
	{
		std::lock_guard<std::mutex> lock(mCallbackMutex);
		mOnDisconnect = std::move(cb);
	}

	void RegisterMessageCallback(boost::function<void(const std::string&)> cb)
	{
		std::lock_guard<std::mutex> lock(mCallbackMutex);
		mOnMessage = std::move(cb);
	}

	void RegisterContentCallback(boost::function<void(const std::vector<uint8_t>&)> cb)
	{
		std::lock_guard<std::mutex> lock(mCallbackMutex);
		mOnContent = std::move(cb);
	}

	// ISessionEvents (invoked from IO threads)

	void OnSessionMessage(const std::string& message) override
	{
		boost::function<void(const std::string&)> cb;
		{
			std::lock_guard<std::mutex> lock(mCallbackMutex);
			cb = mOnMessage;
		}
		InvokeCallback("message", cb, message);
	}

	void OnSessionContent(const std::vector<uint8_t>& content) override
	{
		boost::function<void(const std::vector<uint8_t>&)> cb;
		{
			std::lock_guard<std::mutex> lock(mCallbackMutex);
			cb = mOnContent;
		}
		InvokeCallback("content", cb, content);
	}

	void OnSessionDisconnect() override
	{
		boost::function<void(void)> cb;
		{
			std::lock_guard<std::mutex> lock(mCallbackMutex);
			cb = mOnDisconnect;
		}
		InvokeCallback("disconnect", cb);
	}

	void LogSessionError(const std::string& message) override
	{
		mLogger.Error(Prefixed(message));
	}

  private:
	void DoConnect(const std::string& host, size_t port, const std::string& resource,
				   std::function<void(bool)> onComplete)
	{
		auto session = CreateSession();
		{
			std::lock_guard<std::mutex> lock(mSessionMutex);
			if (mSession)
			{
				mSession->Close();
			}
			mSession = session;
		}
		std::weak_ptr<CImpl> weakSelf = shared_from_this();
		session->AsyncConnect(host, port, resource, [weakSelf, session, onComplete](bool success) {
			if (auto self = weakSelf.lock())
			{
				if (success)
				{
					self->NotifyConnected();
				}
				else
				{
					std::lock_guard<std::mutex> lock(self->mSessionMutex);
					if (self->mSession == session)
					{
						self->mSession.reset();
					}
				}
			}
			if (onComplete)
			{
				onComplete(success);
			}
		});
	}

	std::shared_ptr<ISession> CreateSession()
	{
		std::weak_ptr<ISessionEvents> events = shared_from_this();
		if (mSettings.sslContext && *mSettings.sslContext)
		{
			return std::make_shared<CSession<CSslStream>>(mIoc, mSettings, events, *mSettings.sslContext);
		}
		return std::make_shared<CSession<CPlainStream>>(mIoc, mSettings, events);
	}

	void NotifyConnected()
	{
		boost::function<void(void)> cb;
		{
			std::lock_guard<std::mutex> lock(mCallbackMutex);
			cb = mOnConnect;
		}
		InvokeCallback("connect", cb);
	}

	template <class TCallback, class... TArgs>
	void InvokeCallback(const char* what, const TCallback& cb, const TArgs&... args)
	{
		if (!cb)
		{
			return;
		}
		try
		{
			cb(args...);
		}
		catch (const std::exception& e)
		{
			mLogger.Error(Prefixed(std::string("Unhandled exception in ") + what + " callback: " + e.what()));
		}
		catch (...)
		{
			mLogger.Error(Prefixed(std::string("Unhandled exception in ") + what + " callback"));
		}
	}

	std::string Prefixed(const std::string& message) const
	{
		return mName.empty() ? message : ("[" + mName + "] " + message);
	}

	const std::string mName;
	const core::errorlogger::CLogger mLogger;
	const CClientSettings mSettings;

	net::io_context mIoc;
	net::executor_work_guard<net::io_context::executor_type> mWorkGuard;
	std::vector<std::thread> mThreads;
	std::atomic<bool> mShutdown{false};

	mutable std::mutex mSessionMutex;
	std::shared_ptr<ISession> mSession;

	std::mutex mCallbackMutex;
	boost::function<void(void)> mOnConnect;
	boost::function<void(void)> mOnDisconnect;
	boost::function<void(const std::string&)> mOnMessage;
	boost::function<void(const std::vector<uint8_t>&)> mOnContent;
};

// ---------------------------------------------------------------------------
// CWebsocketClient
// ---------------------------------------------------------------------------

CWebsocketClient::CWebsocketClient()
	: mImpl(std::make_shared<CImpl>("", core::errorlogger::CLogger(), CClientSettings()))
{
	mImpl->Start();
}

CWebsocketClient::CWebsocketClient(const CClientSettings& settings)
	: mImpl(std::make_shared<CImpl>("", core::errorlogger::CLogger(), settings))
{
	mImpl->Start();
}

CWebsocketClient::CWebsocketClient(const std::string& name)
	: mImpl(std::make_shared<CImpl>(name, core::errorlogger::CLogger(), CClientSettings()))
{
	mImpl->Start();
}

CWebsocketClient::CWebsocketClient(const std::string& name, const CClientSettings& settings)
	: mImpl(std::make_shared<CImpl>(name, core::errorlogger::CLogger(), settings))
{
	mImpl->Start();
}

CWebsocketClient::CWebsocketClient(const core::errorlogger::CLogger& errorLogger)
	: mImpl(std::make_shared<CImpl>("", errorLogger, CClientSettings()))
{
	mImpl->Start();
}

CWebsocketClient::CWebsocketClient(const core::errorlogger::CLogger& errorLogger, const CClientSettings& settings)
	: mImpl(std::make_shared<CImpl>("", errorLogger, settings))
{
	mImpl->Start();
}

CWebsocketClient::~CWebsocketClient()
{
	if (mImpl)
	{
		mImpl->Shutdown();
	}
}

bool CWebsocketClient::Connect(const std::string& host)
{
	return Connect(host, DEFAULT_PORT, DEFAULT_RESOURCE);
}

bool CWebsocketClient::Connect(const std::string& host, size_t port)
{
	return Connect(host, port, DEFAULT_RESOURCE);
}

bool CWebsocketClient::Connect(const std::string& host, const std::string& resource)
{
	return Connect(host, DEFAULT_PORT, resource);
}

bool CWebsocketClient::Connect(const std::string& host, size_t port, const std::string& resource)
{
	return mImpl->Connect(host.empty() ? DEFAULT_HOST : host, port, resource);
}

void CWebsocketClient::AsyncConnect(const std::string& host)
{
	AsyncConnect(host, DEFAULT_PORT, DEFAULT_RESOURCE);
}

void CWebsocketClient::AsyncConnect(const std::string& host, size_t port)
{
	AsyncConnect(host, port, DEFAULT_RESOURCE);
}

void CWebsocketClient::AsyncConnect(const std::string& host, const std::string& resource)
{
	AsyncConnect(host, DEFAULT_PORT, resource);
}

void CWebsocketClient::AsyncConnect(const std::string& host, size_t port, const std::string& resource)
{
	mImpl->AsyncConnect(host.empty() ? DEFAULT_HOST : host, port, resource);
}

bool CWebsocketClient::IsConnected() const
{
	return mImpl->IsConnected();
}

void CWebsocketClient::Close()
{
	mImpl->Close();
}

void CWebsocketClient::SendMessage(const std::string& message)
{
	SOutgoingMessage outgoing;
	outgoing.isText = true;
	outgoing.text = message;
	mImpl->Send(std::move(outgoing));
}

void CWebsocketClient::SendContent(const std::vector<uint8_t>& content)
{
	SendContent(std::make_shared<std::vector<uint8_t>>(content));
}

void CWebsocketClient::SendContent(const std::shared_ptr<std::vector<uint8_t>>& content)
{
	if (!content)
	{
		return;
	}
	SOutgoingMessage outgoing;
	outgoing.isText = false;
	outgoing.binary = content;
	mImpl->Send(std::move(outgoing));
}

void CWebsocketClient::RegisterConnectCallback(boost::function<void(void)> cb)
{
	mImpl->RegisterConnectCallback(std::move(cb));
}

void CWebsocketClient::RegisterDisconnectCallback(boost::function<void(void)> cb)
{
	mImpl->RegisterDisconnectCallback(std::move(cb));
}

void CWebsocketClient::RegisterMessageCallback(boost::function<void(const std::string&)> cb)
{
	mImpl->RegisterMessageCallback(std::move(cb));
}

void CWebsocketClient::RegisterContentCallback(boost::function<void(const std::vector<uint8_t>&)> cb)
{
	mImpl->RegisterContentCallback(std::move(cb));
}

} // namespace websocketclient
