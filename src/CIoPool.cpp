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
