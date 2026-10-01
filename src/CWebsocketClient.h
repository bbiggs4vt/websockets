#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <boost/function.hpp>
#include <boost/optional.hpp>

#include "CIoPool.h"
#include "ISslContext.h"

namespace websocketclient
{

// Basic Websocket Client
class CWebsocketClient
{
  public:
	/// @note Client settings are expected to be reused/common across many websocket clients
	struct CClientSettings
	{
		/// How many seconds the client/server are allowed to complete the websocket handshake
		/// (also bounds the TCP connect and ssl handshake; 0 disables these timeouts)
		uint16_t handshakeTimeoutS;
		/// How many seconds the server's connection may be idle before auto-disconnect
		/// (0 disables idle auto-disconnect)
		/// @note With enablePings set, a ping is sent once the connection has been idle for idleTimeoutS/2
		uint16_t idleTimeoutS;
		/// Whether keep-alive pings are sent when the connection is idle (pings only occur when
		/// idleTimeoutS is nonzero). With pings disabled, a nonzero idleTimeoutS still
		/// auto-disconnects an idle connection
		bool enablePings;
		/// If set, used to establish a ssl connection
		boost::optional<sslcontext::ISslContextPtr> sslContext;
		/// Pool the client's IO runs on; when unset, the process-wide CIoPool::Default() is used,
		/// so all default-configured clients and servers share one set of IO threads. The client
		/// holds a reference to its pool for its lifetime and never stops it. Callbacks are
		/// delivered via this client's own single-threaded workqueue, so a slow callback never
		/// stalls the shared pool
		CIoPoolPtr ioPool;

		/// Creates ClientSettings with defaults (e.g. {30, 30, true, (empty), (empty)})
		CClientSettings();
	};

	/// Constructor w/ default settings
	CWebsocketClient();
	/// @param[in] settings client settings to use
	CWebsocketClient(const CClientSettings& settings);
	/// @param[in] name identifier for debugging
	CWebsocketClient(const std::string& name);
	/// @param[in] name identifier for debugging
	/// @param[in] settings client settings to use
	CWebsocketClient(const std::string& name, const CClientSettings& settings);
	/// Destructor. Disconnects from the server (if connected) and stops all IO threads
	~CWebsocketClient();

	/// @see Connect(...)
	bool Connect(const std::string& host);
	/// @see Connect(...)
	bool Connect(const std::string& host, size_t port);
	/// @see Connect(...)
	bool Connect(const std::string& host, const std::string& resource);
	/// @param[in] host address of host to connect to (default = "127.0.0.1")
	/// @param[in] port port of host to (default = 8080)
	/// @param[in] resource uri of path to GET (e.g. /server/path/to/websocket)
	/// @return true if connection was successful, otherwise false
	bool Connect(const std::string& host, size_t port, const std::string& resource);

	/// @see AsyncConnect(...)
	void AsyncConnect(const std::string& host);
	/// @see AsyncConnect(...)
	void AsyncConnect(const std::string& host, size_t port);
	/// @see AsyncConnect(...)
	void AsyncConnect(const std::string& host, const std::string& resource);
	/// Connects to the server without blocking. The connect callback (if registered) is invoked on success
	/// @param[in] host address of host to connect to (default = "127.0.0.1")
	/// @param[in] port port of host to (default = 8080)
	/// @param[in] resource uri of path to GET (e.g. /server/path/to/websocket)
	void AsyncConnect(const std::string& host, size_t port, const std::string& resource);

	/// @return true if client believes it's connected to server
	bool IsConnected() const;
	/// Instructs client to disconnect from server. May be called multiple times
	void Close();
	/// Sends a text payload to the server
	void SendMessage(const std::string& message);
	/// Sends a binary payload to the server
	void SendContent(const std::vector<uint8_t>& content);
	/// Sends a binary payload to the server
	void SendContent(const std::shared_ptr<std::vector<uint8_t>>& content);
	/// Sets a header included in the websocket handshake (upgrade) request of subsequent
	/// Connect/AsyncConnect calls (e.g. "Authorization", "Sec-WebSocket-Protocol"). Setting a
	/// header again replaces its value; names are used verbatim, so use consistent casing.
	/// Headers are applied after the standard upgrade fields - overriding protocol-critical
	/// fields (Host, Upgrade, Connection, Sec-WebSocket-*) may break the handshake
	/// @param[in] name header name
	/// @param[in] value header value
	void SetHandshakeHeader(const std::string& name, const std::string& value);
	/// Removes all headers previously set via SetHandshakeHeader
	void ClearHandshakeHeaders();
	/// @note Callbacks are delivered via a single-threaded workqueue and will not occur concurrently
	/// @param[in] cb called when this client connects/is connected to the server
	void RegisterConnectCallback(boost::function<void(void)> cb);
	/// @param[in] cb called when this client disconnects/is disconnected from the server, and
	///            when a connect attempt fails (Connect returning false or AsyncConnect not
	///            establishing), so reconnect logic can key off this single callback
	void RegisterDisconnectCallback(boost::function<void(void)> cb);
	/// @param[in] cb called when this client receives a text payload from the server
	void RegisterMessageCallback(boost::function<void(const std::string&)> cb);
	/// @param[in] cb called when this client receives a binary payload from the server
	void RegisterContentCallback(boost::function<void(const std::vector<uint8_t>&)> cb);

  private:
	class CImpl;
	std::shared_ptr<CImpl> mImpl;
};

typedef std::shared_ptr<CWebsocketClient> CWebsocketClientPtr;

} // namespace websocketclient
