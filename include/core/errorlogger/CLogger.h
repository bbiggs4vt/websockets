#pragma once

#include <string>

namespace core
{
namespace errorlogger
{

/// Minimal error logger used by CWebsocketClient.
/// Writes timestamped messages to stderr (errors) and stdout (info).
/// Copyable so it can be shared across many components.
class CLogger
{
  public:
	/// Creates a logger with no prefix
	CLogger();
	/// @param[in] name prefix prepended to every log line (e.g. component name)
	explicit CLogger(const std::string& name);

	/// Logs an error message
	/// @param[in] message text to log
	void Error(const std::string& message) const;
	/// Logs an informational message
	/// @param[in] message text to log
	void Info(const std::string& message) const;

  private:
	std::string mName;
};

} // namespace errorlogger
} // namespace core
