#pragma once

#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>

namespace testhelpers
{

namespace beast = boost::beast;
namespace websocket = boost::beast::websocket;
namespace net = boost::asio;
using tcp = net::ip::tcp;

inline int gFailures = 0;

inline void Check(bool condition, const std::string& what)
{
	if (condition)
	{
		std::cout << "PASS: " << what << "\n";
	}
	else
	{
		std::cerr << "FAIL: " << what << "\n";
		++gFailures;
	}
}

template <class T>
bool WaitFor(std::future<T>& future, std::chrono::seconds timeout = std::chrono::seconds(10))
{
	return future.wait_for(timeout) == std::future_status::ready;
}

/// Websocket echo server that accepts connections sequentially (one at a time)
/// and echoes frames (preserving text/binary) until each peer closes or drops.
/// Tolerates abrupt disconnects so shutdown tests can kill clients mid-frame.
class CEchoServer
{
  public:
	CEchoServer()
		: mAcceptor(mIoc, tcp::endpoint(tcp::v4(), 0))
	{
		mPort = mAcceptor.local_endpoint().port();
		mThread = std::thread([this]() { Run(); });
	}

	~CEchoServer()
	{
		Stop();
	}

	void Stop()
	{
		if (mStopped.exchange(true))
		{
			return;
		}
		// Unblock the accept loop with a throwaway connection
		try
		{
			net::io_context ioc;
			tcp::socket socket(ioc);
			socket.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), mPort));
		}
		catch (...)
		{
		}
		if (mThread.joinable())
		{
			mThread.join();
		}
	}

	uint16_t Port() const
	{
		return mPort;
	}

  private:
	void Run()
	{
		for (;;)
		{
			tcp::socket socket(mIoc);
			beast::error_code ec;
			mAcceptor.accept(socket, ec);
			if (ec || mStopped)
			{
				return;
			}
			try
			{
				HandleConnection(std::move(socket));
			}
			catch (...)
			{
				// Abrupt client death mid-handshake or mid-frame is expected in shutdown tests
			}
			if (mStopped)
			{
				return;
			}
		}
	}

	static void HandleConnection(tcp::socket socket)
	{
		websocket::stream<tcp::socket> ws(std::move(socket));
		ws.accept();
		beast::flat_buffer buffer;
		for (;;)
		{
			beast::error_code ec;
			ws.read(buffer, ec);
			if (ec)
			{
				break; // clean close or abrupt drop; either way this connection is done
			}
			ws.text(ws.got_text());
			ws.write(buffer.data());
			buffer.consume(buffer.size());
		}
	}

	net::io_context mIoc;
	tcp::acceptor mAcceptor;
	uint16_t mPort = 0;
	std::atomic<bool> mStopped{false};
	std::thread mThread;
};

/// Accepts TCP connections but never speaks: the client's TCP connect succeeds
/// and its websocket handshake then hangs until it times out or is cancelled.
/// Used to park clients mid-connect for shutdown/close-during-connect tests.
class CSilentServer
{
  public:
	CSilentServer()
		: mAcceptor(mIoc, tcp::endpoint(tcp::v4(), 0))
	{
		mPort = mAcceptor.local_endpoint().port();
		mThread = std::thread([this]() { Run(); });
	}

	~CSilentServer()
	{
		mStopped = true;
		try
		{
			net::io_context ioc;
			tcp::socket socket(ioc);
			socket.connect(tcp::endpoint(net::ip::make_address("127.0.0.1"), mPort));
		}
		catch (...)
		{
		}
		if (mThread.joinable())
		{
			mThread.join();
		}
	}

	uint16_t Port() const
	{
		return mPort;
	}

  private:
	void Run()
	{
		for (;;)
		{
			tcp::socket socket(mIoc);
			beast::error_code ec;
			mAcceptor.accept(socket, ec);
			if (ec || mStopped)
			{
				return;
			}
			mSockets.push_back(std::move(socket)); // hold open, never respond
		}
	}

	net::io_context mIoc;
	tcp::acceptor mAcceptor;
	std::vector<tcp::socket> mSockets;
	uint16_t mPort = 0;
	std::atomic<bool> mStopped{false};
	std::thread mThread;
};

} // namespace testhelpers
