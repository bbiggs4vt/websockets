#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "ISslContext.h"

namespace websocketclient
{

class CWebsocketServer;
typedef std::shared_ptr<CWebsocketServer> CWebsocketServerPtr;

/// Default cap on the number of outstanding (queued but unwritten) payloads per client session
static const size_t StandardMaxOutstandingWrites = 1024;

// Basic Websocket Server
class CWebsocketServer
{
  public:
	/// @param[in] clientId Unique id assigned by server to this client
	/// @param[in] uriAppend Remainder of the request target beyond the matched registered URI,
	///            including any query string (e.g. registered "/alpha", target "/alpha/extra?x=1"
	///            yields "/extra?x=1"). If no URI matched, this is the full request target
	/// @param[in] isSsl true if connection uses ssl, false if it's unsecured/no ssl
	typedef std::function<void(uint32_t clientId, const std::string& uriAppend, bool isSsl)> OnClientAddedCbFn;
	/// @param[in] clientId Unique id of client disconnecting/being removed
	typedef std::function<void(uint32_t clientId)> OnClientClosedCbFn;
	/// @param[in] clientId Unique id of client message was received from
	/// @param[in] message text payload received from client
	typedef std::function<void(uint32_t clientId, const std::string& message)> OnClientMessageReceivedCbFn;
	/// @param[in] clientId Unique id of client content was received from
	/// @param[in] content binary payload received from client
	typedef std::function<void(uint32_t clientId, const std::vector<uint8_t>& content)> OnClientContentReceivedCbFn;

	// Simplified request/response type to avoid dragging boost::beast to other locations in the code
	struct HttpMsg
	{
		/// Request/response body bytes
		std::vector<uint8_t> body;

		/// Whether the connection should stay open after the response (mirrors the request by default)
		bool keepAlive = true;
		/// HTTP version encoded as major * 10 + minor (e.g. 11 for HTTP/1.1)
		uint32_t version = 11;

		std::string uri;    ///< Request target path without the query string; ignored for response
		std::string method; ///< Request method (e.g. "GET", "POST"); ignored for response

		std::string reason = "OK";  ///< Response status reason text; ignored for request
		uint32_t responseCode = 200; ///< Response status code; ignored for request

		/// Id of the connection this request originated from
		/// @note HTTP connections receive ids from the same counter as websocket clients but are
		///       transient: they never appear in GetConnectedClientIds() or the client callbacks
		uint32_t clientId = 0;

		// if from put/post we need to know how to parse the data
		// if response then this must match the format of the data
		std::string contextType = "application/json";

		// for GET messages this contains the uri parameters i.e. {{para1,data1}} from something/foo/?para1=data1
		typedef std::unordered_map<std::string, std::string> HttpQueryParameters;
		HttpQueryParameters queryParameters;
	};
	typedef std::shared_ptr<HttpMsg> HttpMsgPtr;

	/// @param[in] response the http message to send back to the client
	typedef std::function<void(const HttpMsgPtr&)> OnReplyFn;

	/// Wraps a received http request together with the means to respond to it
	class CHttpWrapper
	{
	  public:
		/// @param[in] http the received request; also used as the response when replying
		/// @param[in] fn callback that sends the response back to the client
		CHttpWrapper(const HttpMsgPtr& http, const OnReplyFn& fn);
		/// Destructor
		/// @note If a response has not been sent then this function will
		///       attempt to respond to the client with 'bad request'
		~CHttpWrapper();

		/// Returns the http message; this is the message used when replying to the client,
		/// so mutate it (responseCode, body, contextType, ...) before calling Reply()
		HttpMsgPtr HttpMsg()
		{
			return mHttpMsg;
		}
		/// Replies to the client with the current http message. Only the first call sends;
		/// subsequent calls are ignored (with an error logged)
		void Reply();

	  private:
		HttpMsgPtr mHttpMsg;        //!< Http message
		OnReplyFn mReplyFn;         //!< Callback that lets us send a response
		bool mResponseSent{false};  //!< If a reply was sent
	};
	typedef std::shared_ptr<CHttpWrapper> CHttpWrapperPtr;

	/// Builds the standard error message HTL uses when responding to an invalid
	/// HTTP request. This lets us avoid generating OpenApi code (see lagniapp).
	/// @param[in] shortMsg brief error summary (must be JSON-string safe)
	/// @param[in] longMsg detailed error description (must be JSON-string safe)
	/// @return the error payload as a JSON string
	static std::string HttpErrorMessage(const std::string& shortMsg, const std::string& longMsg)
	{
		std::ostringstream message;
		message << "{\"message\": { \"shortMessage\": \"" << shortMsg << "\", \"longMessage\": \"" << longMsg
				<< "\"}}";
		return message.str();
	}

	/// @param[in] httpMsg the http wrapper for a request received from a client
	typedef std::function<void(const CHttpWrapperPtr& httpMsg)> OnRequestReceivedCb;

	/// @note No functions are required by the server. The server will track clientIds and any callback may be left unset
	/// @note Callbacks are delivered via a single-threaded workqueue and will not occur concurrently
	///       (with the exception of the closedCb during Stop() or RemoveClient() which occurs in the calling thread)
	struct CClientCallbacks
	{
		OnClientAddedCbFn mOnClientAddedCb; //!< Called when a new client successfully connects to the server
		OnClientClosedCbFn mOnClientClosedCb; //!< Called when a client disconnects or is removed by the server
		OnClientMessageReceivedCbFn mOnClientMessageReceivedCb; //!< Called when a message (text payload) is received from a client
		OnClientContentReceivedCbFn mOnClientContentReceivedCb; //!< Called when content (binary payload) is received from a client
	};

	/// @param[in] numConnections number of clients connected to this uri
	typedef std::function<void(size_t numConnections)> OnConnectionChangeCbFn;

	// Callbacks for registered URIs. Delivered via the same single-threaded workqueue as
	// CClientCallbacks (same exception for connection changes during Stop()/RemoveClient())
	struct CUriCallbacks
	{
		OnConnectionChangeCbFn mOnConnectionChange; //!< Called when the number of clients connected to this uri changes
		OnClientMessageReceivedCbFn mOnClientMessageReceivedCb; //!< Called when a message (text payload) is received from a client on this uri
		OnClientContentReceivedCbFn mOnClientContentReceivedCb; //!< Called when content (binary payload) is received from a client on this uri
		OnRequestReceivedCb mOnRequestReceivedCb; //!< Called when a plain http (non-websocket) request is received for this uri
	};

	/// typedef for delayed execution of content function (allows you to bind up or create lambda for evaluation).
	/// The function is only invoked if at least one matching client is connected; it should fill the
	/// passed (empty) vector with the binary payload to send
	typedef std::function<void(std::vector<uint8_t>&)> DelayedContentEvalFn;

	/// typedef for delayed execution of message function (allows you to bind up or create lambda for evaluation).
	/// The function is only invoked if at least one matching client is connected; it should fill the
	/// passed (empty) string with the text payload to send
	typedef std::function<void(std::string&)> DelayedMessageEvalFn;

	/// @note Server settings are expected to be reused/common across many websocket servers
	struct CServerSettings
	{
		/// How many seconds the client/server are allowed to complete the websocket handshake
		/// (also bounds the ssl handshake and each plain-http request read)
		uint16_t handshakeTimeoutS;
		/// How many seconds the client's connection may be idle before auto-disconnect. Auto-disconnect is disabled if enablePings is false
		/// @note If no content is received from a client, a ping will be sent if the connection has been idle for idleTimeoutS/2
		uint16_t idleTimeoutS;
		/// Whether or not the server will ping the client when idle (also enables idleTimeoutS)
		bool enablePings;
		/// Number of threads to use for IO. This influences the number of threads accepting client connections and receiving client messages/content.
		/// The server employs max(1, numThreads) IO threads plus one dedicated callback workqueue thread
		/// @note CClientCallbacks are *always* pipelined through a single-threaded workqueue regardless of thread count
		int numThreads;
		/// If set, used to establish a ssl session w/ clients
		std::optional<sslcontext::ISslContextPtr> sslContext;
		/// If true, clients will only be accepted if they successfully establish a ssl connection. If false
		/// (and sslContext is set) the server auto-detects ssl vs plain per connection. Ignored if sslContext is empty
		bool sslRequired;
		/// List of allowed URIs; if set, websocket upgrade requests whose target does not prefix-match
		/// one of these are rejected. Plain http requests are not filtered by this list
		std::optional<std::set<std::string>> allowedUris;
		/// Max number of outstanding (queued but unwritten) payloads per client session before further
		/// payloads to that client are dropped (0 = infinite). Drops are logged once per connection
		size_t maxSessionBacklog;

		/// Internal mutex for server state, shared between servers constructed from the same settings.
		/// Left null, each server creates its own
		std::shared_ptr<std::mutex> internalMutex;

		/// Sets defaults for server settings { 30, 30, true, 1, (empty), false, (empty), StandardMaxOutstandingWrites }
		CServerSettings();
	};
	typedef std::shared_ptr<CServerSettings> CServerSettingsPtr;

	/// Class to encompass URI specific functionality: registers the uri (with callbacks) on
	/// construction and removes it on destruction
	class CUriWrapper
	{
	  public:
		/// @param[in] server the server to register the uri with
		/// @param[in] uri the uri to register
		/// @param[in] callbacks callbacks to invoke for activity on this uri
		CUriWrapper(const CWebsocketServerPtr& server, const std::string& uri, const CUriCallbacks& callbacks);
		/// Destructor. Unregisters the uri (existing connections stay open)
		~CUriWrapper();

		/// Sends a binary payload to all clients connected to this uri
		/// @param[in] content binary payload to send
		void SendContent(const std::vector<uint8_t>& content);
		/// Sends a binary payload to all clients connected to this uri; the payload is only
		/// built (by invoking content) if at least one client is connected
		/// @param[in] content function that produces the binary payload to send
		void SendContent(const DelayedContentEvalFn& content);
		/// Sends a text payload to all clients connected to this uri
		/// @param[in] message text payload to send
		void SendMessage(const std::string& message);
		/// Sends a text payload to all clients connected to this uri; the payload is only
		/// built (by invoking message) if at least one client is connected
		/// @param[in] message function that produces the text payload to send
		void SendMessage(const DelayedMessageEvalFn& message);

		// TODO add receive stuff and maybe client id options

		/// @return true if any clients are connected to this uri
		bool ClientsConnected();

	  private:
		CWebsocketServerPtr mServer; //!< server pointer
		std::string mUri;            //!< our URI
	};
	typedef std::shared_ptr<CUriWrapper> CUriWrapperPtr;

	/// Constructs a websocket server w/ defaults (name == "CWebsocketServer")
	/// @see CWebsocketServer(...)
	CWebsocketServer();
	/// @see CWebsocketServer(...)
	CWebsocketServer(const std::string& name);
	/// @param[in] name used for logging and naming threads
	/// @param[in] settings server settings to use
	CWebsocketServer(const std::string& name, const CServerSettings& settings);
	/// Destructor. Stops the server (if started) and stops all IO/callback threads
	~CWebsocketServer();

	/// Starts the websocket server using all default values ("0.0.0.0", port 8080, no callbacks)
	/// @note This overload is primarily for testing since default servers will collide
	void Start();
	/// @see Start(...)
	/// @note This overload is primarily for testing since default servers will collide
	void Start(const CClientCallbacks& clientCbs);
	/// @see Start(...)
	void Start(unsigned short port, const CClientCallbacks& clientCbs);
	/// Starts the websocket server
	/// @param[in] address ip address of websocket server. Typically this is the permissive "0.0.0.0" unless connections are limited to a particular network interface
	/// @param[in] port port to run websocket on (0 selects an ephemeral port, see Port()). Multiple websockets may utilize the same port (typically 8080) if uris are unique
	/// @param[in] clientCbs callbacks for client activity; any may be left unset
	/// @throw std::exception derivative if the server cannot be started (e.g. address in use, already started)
	void Start(const std::string& address, unsigned short port, const CClientCallbacks& clientCbs);
	/// Stops the server. May be called multiple times; the server may be started again afterwards
	/// @note OnClientClosedCb will be called w/i the calling thread for any clients disconnected
	void Stop();
	/// @return the port the server is listening on, or 0 if not started. Useful when Start() was
	///         given port 0 to select an ephemeral port
	unsigned short Port() const;
	/// Immediately disconnects specified client
	/// @note received messages/content from this client already in the queue will still be processed after this function returns
	/// @note OnClientClosedCb is called w/i the calling thread
	/// @param[in] clientId which client to disconnect
	void RemoveClient(uint32_t clientId);
	/// Sends a binary payload to a single specific client
	/// @param[in] clientId client to send binary payload to
	/// @param[in] content binary payload to send to client
	void SendContent(uint32_t clientId, const std::vector<uint8_t>& content);
	/// Sends a binary payload to all connected clients
	/// @param[in] content binary payload to send to all connected clients
	void SendContent(const std::vector<uint8_t>& content);
	/// Sends a binary payload to all clients connected to the given uri
	/// @param[in] uri registered uri whose clients receive the payload
	/// @param[in] content binary payload to send
	void SendContent(const std::string& uri, const std::vector<uint8_t>& content);
	/// Sends a binary payload to all connected clients; the payload is only built (by invoking
	/// content) if at least one client is connected
	/// @param[in] content function that produces the binary payload to send
	void SendContent(const DelayedContentEvalFn& content);
	/// Sends a binary payload to all clients connected to the given uri; the payload is only
	/// built (by invoking content) if at least one matching client is connected
	/// @param[in] uri registered uri whose clients receive the payload
	/// @param[in] content function that produces the binary payload to send
	void SendContent(const std::string& uri, const DelayedContentEvalFn& content);
	/// Sends a text payload to a single specific client
	/// @param[in] clientId client to send text payload to
	/// @param[in] message text payload to send to client
	void SendMessage(uint32_t clientId, const std::string& message);
	/// Sends a text payload to all connected clients
	/// @param[in] message text payload to send to all clients
	void SendMessage(const std::string& message);
	/// Sends a text payload to all clients connected to the given uri
	/// @param[in] uri registered uri whose clients receive the payload
	/// @param[in] message text payload to send
	void SendMessage(const std::string& uri, const std::string& message);
	/// Sends a text payload to all connected clients; the payload is only built (by invoking
	/// message) if at least one client is connected
	/// @param[in] message function that produces the text payload to send
	void SendMessage(const DelayedMessageEvalFn& message);
	/// Sends a text payload to all clients connected to the given uri; the payload is only
	/// built (by invoking message) if at least one matching client is connected
	/// @param[in] uri registered uri whose clients receive the payload
	/// @param[in] message function that produces the text payload to send
	void SendMessage(const std::string& uri, const DelayedMessageEvalFn& message);
	/// Gets list of connected clients and if they're connected via ssl
	/// @return map of connected client ids to whether they're connected via ssl
	std::map<uint32_t, bool> GetConnectedClientIds();
	/// @return true if any clients are connected
	bool ClientsConnected();
	/// @return true if any clients are connected to the given registered uri
	bool ClientsConnected(const std::string& uri);
	/// Method will add a URI to the list of accepted connections
	/// @note Registered uris are matched by path-segment prefix: "/alpha" matches "/alpha" and
	///       "/alpha/beta" but not "/alphabet". The longest matching uri wins
	/// @param[in] uri URI
	/// @param[in] callbacks callbacks to invoke for activity on this uri; any may be left unset
	void AddUri(const std::string& uri, const CUriCallbacks& callbacks);
	/// Method will remove a URI from the list of accepted connections
	/// @details will not close current websocket connections
	/// @param[in] uri URI
	void RemoveUri(const std::string& uri);

  private:
	class CImpl;
	std::shared_ptr<CImpl> mImpl;
};

} // namespace websocketclient
