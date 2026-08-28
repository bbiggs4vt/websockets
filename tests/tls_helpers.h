#pragma once

// TLS helpers for the wire tests: generates a self-signed certificate in
// process (no cert files, no openssl CLI) and provides ISslContext
// implementations for a test server and for clients that trust it.

#include <memory>
#include <stdexcept>
#include <string>

#include <boost/asio/buffer.hpp>
#include <boost/asio/ssl/context.hpp>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include "ISslContext.h"

namespace testhelpers
{

struct SCertificate
{
	std::string certPem;
	std::string keyPem;
};

/// Generates a fresh self-signed RSA certificate for CN/SAN 127.0.0.1, valid one hour
inline SCertificate GenerateSelfSignedCertificate()
{
	using CKeyPtr = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;
	using CCertPtr = std::unique_ptr<X509, decltype(&X509_free)>;
	using CBioPtr = std::unique_ptr<BIO, decltype(&BIO_free)>;

	CKeyPtr key(EVP_PKEY_Q_keygen(nullptr, nullptr, "RSA", static_cast<size_t>(2048)), EVP_PKEY_free);
	if (!key)
	{
		throw std::runtime_error("RSA key generation failed");
	}

	CCertPtr cert(X509_new(), X509_free);
	if (!cert)
	{
		throw std::runtime_error("X509_new failed");
	}
	X509_set_version(cert.get(), 2);
	ASN1_INTEGER_set(X509_get_serialNumber(cert.get()), 1);
	X509_gmtime_adj(X509_getm_notBefore(cert.get()), 0);
	X509_gmtime_adj(X509_getm_notAfter(cert.get()), 60 * 60);
	X509_set_pubkey(cert.get(), key.get());

	X509_NAME* name = X509_get_subject_name(cert.get());
	X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
							   reinterpret_cast<const unsigned char*>("127.0.0.1"), -1, -1, 0);
	X509_set_issuer_name(cert.get(), name);

	X509V3_CTX v3ctx;
	X509V3_set_ctx(&v3ctx, cert.get(), cert.get(), nullptr, nullptr, 0);
	for (const auto& extension : {std::pair<int, const char*>{NID_basic_constraints, "critical,CA:TRUE"},
								  std::pair<int, const char*>{NID_subject_alt_name, "IP:127.0.0.1"}})
	{
		X509_EXTENSION* ext = X509V3_EXT_conf_nid(nullptr, &v3ctx, extension.first, extension.second);
		if (!ext)
		{
			throw std::runtime_error("X509 extension creation failed");
		}
		X509_add_ext(cert.get(), ext, -1);
		X509_EXTENSION_free(ext);
	}

	if (!X509_sign(cert.get(), key.get(), EVP_sha256()))
	{
		throw std::runtime_error("certificate signing failed");
	}

	const auto bioToString = [](BIO* bio) {
		char* data = nullptr;
		const long length = BIO_get_mem_data(bio, &data);
		return std::string(data, static_cast<size_t>(length));
	};

	SCertificate result;
	CBioPtr certBio(BIO_new(BIO_s_mem()), BIO_free);
	if (!certBio || !PEM_write_bio_X509(certBio.get(), cert.get()))
	{
		throw std::runtime_error("certificate PEM encoding failed");
	}
	result.certPem = bioToString(certBio.get());

	CBioPtr keyBio(BIO_new(BIO_s_mem()), BIO_free);
	if (!keyBio ||
		!PEM_write_bio_PrivateKey(keyBio.get(), key.get(), nullptr, nullptr, 0, nullptr, nullptr))
	{
		throw std::runtime_error("key PEM encoding failed");
	}
	result.keyPem = bioToString(keyBio.get());
	return result;
}

/// @return the certificate generated once for this test run
inline const SCertificate& TestCertificate()
{
	static const SCertificate CERTIFICATE = GenerateSelfSignedCertificate();
	return CERTIFICATE;
}

/// Server-side ssl context serving the test certificate
class CTestServerSslContext : public sslcontext::ISslContext
{
  public:
	CTestServerSslContext()
		: mContext(boost::asio::ssl::context::tls_server)
	{
		mContext.set_options(boost::asio::ssl::context::default_workarounds |
							 boost::asio::ssl::context::no_sslv2 | boost::asio::ssl::context::no_sslv3);
		const SCertificate& certificate = TestCertificate();
		mContext.use_certificate_chain(boost::asio::buffer(certificate.certPem));
		mContext.use_private_key(boost::asio::buffer(certificate.keyPem),
								 boost::asio::ssl::context::pem);
	}

	boost::asio::ssl::context& Context() override
	{
		return mContext;
	}

  private:
	boost::asio::ssl::context mContext;
};

/// Client-side ssl context that verifies the peer and trusts (only) the test certificate
class CTrustingClientSslContext : public sslcontext::ISslContext
{
  public:
	CTrustingClientSslContext()
		: mContext(boost::asio::ssl::context::tls_client)
	{
		mContext.add_certificate_authority(boost::asio::buffer(TestCertificate().certPem));
		mContext.set_verify_mode(boost::asio::ssl::verify_peer);
	}

	boost::asio::ssl::context& Context() override
	{
		return mContext;
	}

  private:
	boost::asio::ssl::context mContext;
};

} // namespace testhelpers
