#include "sslcontext/ISslContext.h"

namespace sslcontext
{

CSslContext::CSslContext(bool verifyPeer)
	: mContext(boost::asio::ssl::context::tls_client)
{
	mContext.set_options(boost::asio::ssl::context::default_workarounds | boost::asio::ssl::context::no_sslv2 |
						 boost::asio::ssl::context::no_sslv3 | boost::asio::ssl::context::no_tlsv1 |
						 boost::asio::ssl::context::no_tlsv1_1);
	if (verifyPeer)
	{
		mContext.set_default_verify_paths();
		mContext.set_verify_mode(boost::asio::ssl::verify_peer);
	}
	else
	{
		mContext.set_verify_mode(boost::asio::ssl::verify_none);
	}
}

boost::asio::ssl::context& CSslContext::Context()
{
	return mContext;
}

ISslContextPtr MakeSslContext(bool verifyPeer)
{
	return std::make_shared<CSslContext>(verifyPeer);
}

} // namespace sslcontext
