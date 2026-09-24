// Copyright 2026 Northern.tech AS
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

#ifndef MENDER_COMMON_VARLINK_HPP
#define MENDER_COMMON_VARLINK_HPP

#include <common/config.h>

#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#ifdef MENDER_USE_BOOST_ASIO
#include <boost/asio.hpp>
#endif // MENDER_USE_BOOST_ASIO

#include <common/error.hpp>
#include <common/events.hpp>
#include <common/io.hpp>
#include <common/json.hpp>
#include <common/log.hpp>
#include <common/optional.hpp>

namespace mender {
namespace common {
namespace varlink {

#ifdef MENDER_USE_BOOST_ASIO
namespace asio = boost::asio;
#endif // MENDER_USE_BOOST_ASIO

namespace error = mender::common::error;
namespace events = mender::common::events;
namespace json = mender::common::json;
namespace log = mender::common::log;

constexpr char kMessageSeparator = '\0';
// Largest frame we accept before dropping the peer. libvarlink allows 16 MiB;
// our calls and replies are a few hundred bytes.
constexpr size_t kMaxFrameSize = 1024 * 1024;

// Standard varlink service error names.
constexpr const char *kErrorMethodNotFound = "org.varlink.service.MethodNotFound";
constexpr const char *kErrorInvalidParameter = "org.varlink.service.InvalidParameter";
constexpr const char *kErrorInterfaceNotFound = "org.varlink.service.InterfaceNotFound";

enum VarlinkErrorCode {
	NoError = 0,
	TransportError,
};
class VarlinkErrorCategoryClass : public std::error_category {
public:
	const char *name() const noexcept override;
	std::string message(int code) const override;
};
extern const VarlinkErrorCategoryClass VarlinkErrorCategory;
error::Error MakeError(VarlinkErrorCode code, const std::string &msg);

struct MethodCall {
	std::string method;
	json::Json parameters;
	bool more {false};
	bool oneway {false};
};

struct Reply {
	json::Json parameters {json::Json::Object()};
	bool continues {false};
	std::string error_name; // non-empty => error reply

	// No more frames follow: an error (always terminal) or a reply without continues.
	bool IsTerminal() const {
		return !error_name.empty() || !continues;
	}
};

Reply ErrorReply(const std::string &error_name, const json::KeyValueMap &parameters = {});

// Sends a reply frame as given. Intermediate frames (continues=true) are only
// accepted when the client asked for a stream with "more"; returns false for
// those otherwise, for oneway calls, after the terminal frame, and once the
// connection is gone.
using Replier = std::function<bool(const Reply &)>;
// Returns the terminal reply, or nullopt to reply later through the Replier
// (deferred single reply, or a stream that ends when the handler emits a frame
// without continues).
using MethodHandler = std::function<optional<Reply>(const MethodCall &call, const Replier &emit)>;

// Broadcasts to every client holding a "more" call open: the varlink equivalent
// of a D-Bus signal. To subscribe a client, the method handler calls Add() with
// the Replier it was given and returns nullopt, which keeps the call open. Each
// Emit() then sends one continues=true frame to every subscriber and drops
// those whose connection is gone.
class Subscribers {
public:
	void Add(Replier emit);
	void Emit(Reply reply);
	size_t Size() const {
		return subscribers_.size();
	}

private:
	std::vector<Replier> subscribers_;
};

// Frames a peer may leave undrained, or pipeline behind an unfinished call,
// before we drop it.
constexpr size_t kMaxQueuedFrames = 1024;

class Connection;
using ConnectionPtr = std::shared_ptr<Connection>;

// A hand-rolled varlink server over a unix stream socket. Messages are JSON
// objects separated by single NUL bytes.
class Server : public events::EventLoopObject, virtual public io::Canceller {
public:
	Server(events::EventLoop &loop);
	~Server();

	Server(const Server &) = delete;
	Server &operator=(const Server &) = delete;

	// Handler for a fully qualified method name, e.g. "io.mender.Update1.GetState".
	void AddMethodHandler(const std::string &method, MethodHandler handler);

	// Service metadata reported by the standard org.varlink.service.GetInfo call,
	// so any varlink client (e.g. `varlinkctl info`) can introspect the service.
	void SetServiceInfo(
		const std::string &vendor,
		const std::string &product,
		const std::string &version,
		const std::string &url);
	// Publish a varlink interface's IDL description (returned by
	// org.varlink.service.GetInterfaceDescription and listed by GetInfo).
	void AddInterface(const std::string &name, const std::string &description);

	// Binds socket_path with mode 0660 (parent dir 0750 if we have to create it).
	// Replaces a stale socket file left by a crashed run, but returns TransportError
	// if another process is still listening on it.
	error::Error Listen(const std::string &socket_path);

	void Cancel() override;

private:
	void AsyncAccept();
	void RemoveConnection(const ConnectionPtr &conn);
	// Registers the standard org.varlink.service interface and its
	// GetInfo/GetInterfaceDescription methods.
	void RegisterServiceInterface();
	// True when an IDL was published for name or a handler is registered under it.
	bool HasInterface(const std::string &name) const;

#ifdef MENDER_USE_BOOST_ASIO
	asio::io_context &ctx_;
	asio::local::stream_protocol::acceptor acceptor_;
#endif // MENDER_USE_BOOST_ASIO
	events::Timer accept_retry_timer_;
	std::set<ConnectionPtr> connections_;
	std::unordered_map<std::string, MethodHandler> methods_;
	std::string socket_path_;
	std::string svc_vendor_ {"Northern.tech"};
	std::string svc_product_;
	std::string svc_version_;
	std::string svc_url_;
	std::map<std::string, std::string> interface_descriptions_;
	log::Logger logger_;

	friend class Connection;
};

} // namespace varlink
} // namespace common
} // namespace mender

#endif // MENDER_COMMON_VARLINK_HPP
