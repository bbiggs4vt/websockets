#include "core/errorlogger/CLogger.h"

#include <chrono>
#include <ctime>
#include <iostream>
#include <sstream>

namespace core
{
namespace errorlogger
{

namespace
{

std::string BuildLine(const std::string& name, const char* level, const std::string& message)
{
	const auto now = std::chrono::system_clock::now();
	const std::time_t nowTimeT = std::chrono::system_clock::to_time_t(now);
	std::tm nowTm{};
#ifdef _WIN32
	localtime_s(&nowTm, &nowTimeT);
#else
	localtime_r(&nowTimeT, &nowTm);
#endif
	char timestamp[32] = {0};
	std::strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", &nowTm);

	std::ostringstream line;
	line << "[" << timestamp << "][" << level << "]";
	if (!name.empty())
	{
		line << "[" << name << "]";
	}
	line << " " << message << "\n";
	return line.str();
}

} // namespace

CLogger::CLogger() = default;

CLogger::CLogger(const std::string& name)
	: mName(name)
{
}

void CLogger::Error(const std::string& message) const
{
	// Single stream insertion of a pre-built string keeps lines intact across threads
	std::cerr << BuildLine(mName, "ERROR", message) << std::flush;
}

void CLogger::Info(const std::string& message) const
{
	std::cout << BuildLine(mName, "INFO", message) << std::flush;
}

} // namespace errorlogger
} // namespace core
