#pragma once

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

namespace websocketclient
{

class CIoPool;
typedef std::shared_ptr<CIoPool> CIoPoolPtr;

/// Shared IO thread pool. All CWebsocketClient/CWebsocketServer instances run their IO on a
/// pool: the process-wide Default() pool unless CClientSettings::ioPool /
/// CServerSettings::ioPool names another one. Instances hold a reference to the pool they
/// use, so the pool outlives them under normal destruction order; an application creating
/// its own pool should keep its own reference for as long as it keeps creating instances
/// with it.
/// @note Do not block a callback for long: a pool's threads drive the IO of every instance
///       sharing it (callbacks run on per-instance workqueues, but a blocking callback still
///       delays that instance's later callbacks)
class CIoPool
{
  public:
	/// Starts the pool and its threads
	/// @param[in] numThreads number of IO threads to run (clamped to >= 1)
	explicit CIoPool(int numThreads);
	/// Destructor. Stops the pool
	~CIoPool();

	/// Returns the lazily-created process-wide default pool, sized to the hardware
	/// concurrency (min 2 threads). Used by every client/server whose settings leave
	/// ioPool unset
	/// @return the default pool
	static CIoPoolPtr Default();

	/// Stops the pool: outstanding IO is abandoned and the threads are joined. Idempotent.
	/// @note Do not call from a pool thread (e.g. from inside a callback), and do not stop
	///       the Default() pool while instances may still be using it
	void Stop();

	/// @return the io context driven by this pool's threads
	boost::asio::io_context& Context();

  private:
	boost::asio::io_context mIoc;
	boost::asio::executor_work_guard<boost::asio::io_context::executor_type> mWorkGuard;
	std::vector<std::thread> mThreads;
	std::atomic<bool> mStopped{false};
};

} // namespace websocketclient
