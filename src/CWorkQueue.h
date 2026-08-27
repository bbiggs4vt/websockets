#pragma once

// Internal single-threaded callback workqueue shared by the client and server
// implementations. Not part of the public API.

#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>

namespace websocketclient
{
namespace detail
{

/// Runs posted functions in order on one dedicated thread. Stop() drains
/// already-posted work before joining, so a caller returning from Stop() knows
/// no further callbacks will run (unless Stop() was called from the worker
/// thread itself, in which case the worker is detached to avoid self-join)
class CWorkQueue
{
  public:
	~CWorkQueue()
	{
		Stop();
	}

	void Start()
	{
		mThread = std::thread([this]() { Run(); });
	}

	void Post(std::function<void()> fn)
	{
		{
			std::lock_guard<std::mutex> lock(mMutex);
			if (mStopped)
			{
				return;
			}
			mQueue.push_back(std::move(fn));
		}
		mCondition.notify_one();
	}

	/// Stops the queue after draining already-posted work
	void Stop()
	{
		{
			std::lock_guard<std::mutex> lock(mMutex);
			if (mStopped)
			{
				return;
			}
			mStopped = true;
		}
		mCondition.notify_one();
		if (mThread.joinable())
		{
			if (mThread.get_id() == std::this_thread::get_id())
			{
				mThread.detach();
			}
			else
			{
				mThread.join();
			}
		}
	}

  private:
	void Run()
	{
		for (;;)
		{
			std::function<void()> fn;
			{
				std::unique_lock<std::mutex> lock(mMutex);
				mCondition.wait(lock, [this]() { return mStopped || !mQueue.empty(); });
				if (mQueue.empty())
				{
					return; // stopped and drained
				}
				fn = std::move(mQueue.front());
				mQueue.pop_front();
			}
			fn();
		}
	}

	std::mutex mMutex;
	std::condition_variable mCondition;
	std::deque<std::function<void()>> mQueue;
	bool mStopped = false;
	std::thread mThread;
};

} // namespace detail
} // namespace websocketclient
