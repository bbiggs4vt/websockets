#pragma once

#include <memory>
#include <string>

#include <boost/asio/ssl/context.hpp>

namespace sslcontext
{

/// Abstraction over an ssl context so applications can control certificate
/// handling, verification, ciphers, etc. independently of the websocket client
class ISslContext
{
  public:
	virtual ~ISslContext() = default;

	/// @return the underlying asio ssl context used to establish TLS connections
	virtual boost::asio::ssl::context& Context() = 0;
};

typedef std::shared_ptr<ISslContext> ISslContextPtr;

/// Basic TLS client context (TLS 1.2+) using the system's default CA paths
class CSslContext : public ISslContext
{
  public:
	/// @param[in] verifyPeer whether to verify the server's certificate (disable for self-signed/dev servers)
	explicit CSslContext(bool verifyPeer = true);

	boost::asio::ssl::context& Context() override;

  private:
	boost::asio::ssl::context mContext;
};

/// Convenience factory
/// @param[in] verifyPeer whether to verify the server's certificate
/// @return a ready-to-use TLS client context
ISslContextPtr MakeSslContext(bool verifyPeer = true);

} // namespace sslcontext
