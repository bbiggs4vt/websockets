#pragma once

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

namespace websocketclient
{

/// Shared IO thread pool. Multiple CWebsocketClient/CWebsocketServer instances may run their
/// IO on one pool (via CClientSettings::ioPool / CServerSettings::ioPool) instead of each
/// instance owning its own threads. Instances hold a reference to the pool they were given,
/// so the pool outlives them under normal destruction order; the application should keep its
/// own reference for as long as it keeps creating instances with it.
/// @note With a shared pool, do not block a callback for long: the pool's threads drive the
///       IO of every instance sharing it
class CIoPool
{
  public:
	/// Starts the pool and its threads
	/// @param[in] numThreads number of IO threads to run (clamped to >= 1)
	explicit CIoPool(int numThreads);
	/// Destructor. Stops the pool
	~CIoPool();

	/// Stops the pool: outstanding IO is abandoned and the threads are joined. Idempotent.
	/// @note Do not call from a pool thread (e.g. from inside a callback)
	void Stop();

	/// @return the io context driven by this pool's threads
	boost::asio::io_context& Context();

  private:
	boost::asio::io_context mIoc;
	boost::asio::executor_work_guard<boost::asio::io_context::executor_type> mWorkGuard;
	std::vector<std::thread> mThreads;
	std::atomic<bool> mStopped{false};
};

typedef std::shared_ptr<CIoPool> CIoPoolPtr;

} // namespace websocketclient
