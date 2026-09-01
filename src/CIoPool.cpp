#include "CIoPool.h"

#include <algorithm>

namespace websocketclient
{

CIoPool::CIoPool(int numThreads)
	: mIoc(std::max(1, numThreads))
	, mWorkGuard(boost::asio::make_work_guard(mIoc))
{
	const int threadCount = std::max(1, numThreads);
	mThreads.reserve(threadCount);
	for (int i = 0; i < threadCount; ++i)
	{
		mThreads.emplace_back([this]() { mIoc.run(); });
	}
}

CIoPool::~CIoPool()
{
	Stop();
}

CIoPoolPtr CIoPool::Default()
{
	// A function-local static keeps initialization thread-safe; borrowers hold their own
	// references, so even an instance outliving this static keeps the pool alive until
	// it is done with it
	static const CIoPoolPtr DEFAULT_POOL =
		std::make_shared<CIoPool>(static_cast<int>(std::max(2u, std::thread::hardware_concurrency())));
	return DEFAULT_POOL;
}

void CIoPool::Stop()
{
	bool expected = false;
	if (!mStopped.compare_exchange_strong(expected, true))
	{
		return;
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

boost::asio::io_context& CIoPool::Context()
{
	return mIoc;
}

} // namespace websocketclient
