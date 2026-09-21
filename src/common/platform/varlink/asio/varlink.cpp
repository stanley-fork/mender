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

#include <common/platform/varlink.hpp>

#include <sys/stat.h> // umask()

#include <cassert>
#include <chrono>
#include <deque>
#include <istream>
#include <memory>
#include <string>
#include <utility>

#include <boost/asio.hpp>

#include <common/error.hpp>
#include <common/events.hpp>
#include <common/json.hpp>
#include <common/log.hpp>
#include <common/path.hpp>

namespace mender {
namespace common {
namespace varlink {

namespace path = mender::common::path;

// Serializes a Reply into a NUL-terminated varlink frame.
static std::string SerializeFrame(const Reply &reply) {
	json::Json frame = json::Json::Object();
	if (!reply.error_name.empty()) {
		frame.Set("error", reply.error_name);
	}
	frame.Set("parameters", reply.parameters);
	// An error reply is always terminal; sd-varlink drops the connection on
	// error+continues, so never emit that combination.
	if (!reply.IsTerminal()) {
		frame.Set("continues", true);
	}
	return frame.Dump(-1) + kMessageSeparator;
}

//
// Connection
//

// A single accepted varlink connection. Lives as long as the read/write loop is
// active. Held by the Server via shared_ptr in a set; removes itself on EOF/error.
class Connection : public std::enable_shared_from_this<Connection> {
public:
	Connection(Server &server, asio::local::stream_protocol::socket socket);

	void ReadFrame();
	void Close();

private:
	void ReadFrameHandler(const boost::system::error_code &ec);
	void Dispatch(const std::string &message);
	// Dispatches queued messages that arrived (pipelined) while a call was
	// outstanding, one at a time, stopping again if one of them defers.
	void ProcessQueuedMessages();
	// Serializes a Reply into a NUL-terminated frame and queues the write. Returns
	// false when the connection is closing or the peer stopped draining the queue.
	bool WriteReply(const Reply &reply);
	// Drains the outbound queue, one async_write in flight at a time.
	void DoWrite();

	Server &server_;
	asio::local::stream_protocol::socket socket_;
	// Bounded so a peer that never sends a NUL cannot grow memory without limit;
	// async_read_until fails with not_found at the cap and we drop the connection.
	asio::streambuf buf_ {kMaxFrameSize};
	// Outbound frame queue; the front frame is the one async_write in flight
	// (concurrent async_write on one socket is undefined behaviour).
	std::deque<std::string> write_queue_;
	// Async handlers check it and bail after teardown.
	bool cancelled_ {false};
	// Set from the moment a call's handler runs until its terminal reply goes
	// out -- returned, or emitted before or after returning nullopt. varlink
	// requires strict per-connection reply ordering
	// (https://varlink.org/FAQ: "all messages are strictly in order on the same
	// connection"), so a pipelined call that arrives before then must wait rather
	// than jump ahead (see pending_messages_). We still keep reading while
	// outstanding -- only DISPATCH is deferred -- so EOF/disconnect is still
	// noticed immediately (a live subscription's whole cleanup path depends on it).
	bool call_outstanding_ {false};
	bool processing_queue_ {false};
	// Calls that arrived while call_outstanding_ was set, queued in arrival order.
	// Capped at kMaxQueuedFrames calls and kMaxFrameSize bytes. Calls queued behind
	// one that never finishes (e.g. a Subscribe stream) are never answered, so a
	// peer that keeps sending them is dropped.
	std::deque<std::string> pending_messages_;
	size_t pending_bytes_ {0};
	log::Logger logger_;
};

Connection::Connection(Server &server, asio::local::stream_protocol::socket socket) :
	server_ {server},
	socket_ {std::move(socket)},
	logger_ {"varlink"} {
}

void Connection::Close() {
	cancelled_ = true;
	boost::system::error_code ec;
	socket_.close(ec);
	if (ec) {
		logger_.Warning("Error closing varlink connection: " + ec.message());
	}
}

void Connection::ReadFrame() {
	auto self = shared_from_this();
	asio::async_read_until(
		socket_, buf_, kMessageSeparator, [self](const boost::system::error_code &ec, size_t) {
			if (!self->cancelled_) {
				self->ReadFrameHandler(ec);
			}
		});
}

void Connection::ReadFrameHandler(const boost::system::error_code &ec) {
	if (ec) {
		if (ec == asio::error::not_found) {
			logger_.Warning(
				"Varlink peer sent a frame larger than " + std::to_string(kMaxFrameSize)
				+ " bytes, dropping connection");
		} else if (ec != asio::error::eof && ec != asio::error::operation_aborted) {
			logger_.Debug("Error reading varlink frame: " + ec.message());
		}
		server_.RemoveConnection(shared_from_this());
		return;
	}

	// async_read_until may read past the separator; only consume up to and including it.
	std::istream is(&buf_);
	std::string message;
	std::getline(is, message, kMessageSeparator);

	if (call_outstanding_) {
		// Pipelined behind a deferred call: queue, don't dispatch (see call_outstanding_).
		if (pending_messages_.size() >= kMaxQueuedFrames
			|| pending_bytes_ + message.size() > kMaxFrameSize) {
			logger_.Warning(
				"Varlink peer pipelined too many calls behind one that never completed, "
				"dropping connection");
			server_.RemoveConnection(shared_from_this());
			return;
		}
		pending_bytes_ += message.size();
		pending_messages_.push_back(std::move(message));
	} else {
		Dispatch(message);
	}

	if (cancelled_) {
		return;
	}

	ReadFrame();
}

void Connection::ProcessQueuedMessages() {
	// Re-entered from WriteReply while a queued handler is still running (it
	// emitted its terminal frame synchronously): the outer loop takes the next one.
	if (processing_queue_) {
		return;
	}
	processing_queue_ = true;
	while (!cancelled_ && !call_outstanding_ && !pending_messages_.empty()) {
		std::string message = std::move(pending_messages_.front());
		pending_messages_.pop_front();
		pending_bytes_ -= message.size();
		Dispatch(message);
	}
	processing_queue_ = false;
}

bool Connection::WriteReply(const Reply &reply) {
	if (cancelled_) {
		return false;
	}

	if (write_queue_.size() >= kMaxQueuedFrames) {
		logger_.Warning("Varlink peer is not reading replies, dropping connection");
		server_.RemoveConnection(shared_from_this());
		return false;
	}

	bool idle = write_queue_.empty();
	write_queue_.push_back(SerializeFrame(reply));
	if (idle) {
		DoWrite();
	}

	if (call_outstanding_ && reply.IsTerminal()) {
		call_outstanding_ = false;
		ProcessQueuedMessages();
	}

	return true;
}

// Writes the front frame; called when the queue goes from empty to non-empty and
// after each completed write. front() stays valid: push_back never invalidates
// deque references and we only pop after this write completes.
void Connection::DoWrite() {
	assert(!write_queue_.empty());
	auto self = shared_from_this();
	asio::async_write(
		socket_,
		asio::buffer(write_queue_.front()),
		[self](const boost::system::error_code &ec, size_t) {
			if (self->cancelled_) {
				return;
			}
			if (ec) {
				self->logger_.Debug("Error writing varlink reply: " + ec.message());
				self->server_.RemoveConnection(self);
				return;
			}
			self->write_queue_.pop_front();
			if (!self->write_queue_.empty()) {
				self->DoWrite();
			}
		});
}

void Connection::Dispatch(const std::string &message) {
	auto exp_json = json::Load(message);
	if (!exp_json) {
		// Not a varlink frame at all; like sd-varlink, hang up rather than guess a
		// reply (we cannot even tell whether the call was oneway).
		logger_.Warning("Dropping varlink peer that sent a malformed frame");
		server_.RemoveConnection(shared_from_this());
		return;
	}
	const json::Json &j = exp_json.value();

	MethodCall call;
	call.more = json::Get<bool>(j, "more", json::MissingOk::Yes).value_or(false);
	call.oneway = json::Get<bool>(j, "oneway", json::MissingOk::Yes).value_or(false);

	// Oneway calls get no reply, not even an error one, or the client would read
	// it as the reply to its next call.
	auto reply = [this, &call](const Reply &r) {
		if (!call.oneway) {
			WriteReply(r);
		}
	};

	auto exp_method = json::Get<std::string>(j, "method", json::MissingOk::No);
	if (!exp_method) {
		reply(ErrorReply(kErrorInvalidParameter, {{"parameter", "method"}}));
		return;
	}
	call.method = exp_method.value();

	auto handler = server_.methods_.find(call.method);
	if (handler == server_.methods_.end()) {
		// Like libvarlink: tell an unknown interface apart from an unknown method
		// on one we serve, so a client can distinguish "wrong service" from
		// "old service".
		auto dot = call.method.rfind('.');
		std::string interface = call.method.substr(0, dot == std::string::npos ? 0 : dot);
		if (!server_.HasInterface(interface)) {
			reply(ErrorReply(kErrorInterfaceNotFound, {{"interface", interface}}));
		} else {
			reply(ErrorReply(kErrorMethodNotFound, {{"method", call.method}}));
		}
		return;
	}

	// Method-Call declares parameters as `?object`: null is the same as absent.
	auto exp_params = j.Get("parameters");
	if (exp_params && !exp_params.value().IsNull()) {
		const json::Json &params = exp_params.value();
		if (!params.IsObject()) {
			reply(ErrorReply(kErrorInvalidParameter, {{"parameter", "parameters"}}));
			return;
		}
		call.parameters = params;
	}

	// Intermediate frames need a "more" call; a terminal frame may always be
	// emitted once (deferred reply). Holds the connection weakly so a long-lived
	// subscriber notices the disconnect (emit returns false) instead of keeping
	// it alive. `done` blocks stale emits after the terminal frame: sd-varlink
	// would take one as the next call's reply.
	std::weak_ptr<Connection> weak = shared_from_this();
	auto done = std::make_shared<bool>(false);
	bool more = call.more;
	bool oneway = call.oneway;
	Replier emit = [weak, done, more, oneway](const Reply &r) -> bool {
		auto conn = weak.lock();
		if (!conn || oneway || *done || (r.continues && !more)) {
			return false;
		}
		if (r.IsTerminal()) {
			*done = true;
		}
		return conn->WriteReply(r);
	};

	// Set before the handler runs so a synchronously emitted terminal frame clears it.
	call_outstanding_ = !oneway;
	optional<Reply> terminal = handler->second(call, emit);

	if (cancelled_) {
		return;
	}
	if (!terminal || *done) {
		// Deferred (pipelined calls wait until the terminal frame goes out), or
		// the handler already emitted its terminal frame: nothing more to send.
		return;
	}
	*done = true;
	// The returned reply is terminal by contract; never let it say otherwise.
	// WriteReply clears call_outstanding_ and drains the pipelined queue.
	terminal->continues = false;
	reply(*terminal);
}

//
// Server
//

Server::Server(events::EventLoop &loop) :
	ctx_ {GetAsioIoContext(loop)},
	acceptor_ {ctx_},
	accept_retry_timer_ {loop},
	logger_ {"varlink"} {
	RegisterServiceInterface();
}

Server::~Server() {
	Cancel();
}

error::Error Server::Listen(const std::string &socket_path) {
	// The endpoint constructor throws when the path exceeds sun_path (108 bytes).
	asio::local::stream_protocol::endpoint endpoint;
	try {
		endpoint = asio::local::stream_protocol::endpoint {socket_path};
	} catch (const boost::system::system_error &e) {
		return MakeError(
			TransportError, "Invalid varlink socket path " + socket_path + ": " + e.what());
	}

	// Normally systemd's RuntimeDirectory= owns the parent; if we have to create it
	// ourselves, keep it owner+group only (0750) like the socket.
	std::string parent = path::DirName(socket_path);
	if (!path::FileExists(parent)) {
		auto err = path::CreateDirectories(parent);
		if (err != error::NoError) {
			return err.WithContext("Could not create varlink socket directory");
		}
		err = path::Permissions(
			parent,
			{path::Perms::Owner_read,
			 path::Perms::Owner_write,
			 path::Perms::Owner_exec,
			 path::Perms::Group_read,
			 path::Perms::Group_exec});
		if (err != error::NoError) {
			return err.WithContext("Could not set permissions on varlink socket directory");
		}
	}

	boost::system::error_code ec;
	if (path::FileExists(socket_path)) {
		// A live server answers connect(); a stale file from a crashed run refuses.
		// Never unlink a socket another instance is serving on.
		asio::local::stream_protocol::socket probe {ctx_};
		probe.connect(endpoint, ec);
		if (!ec) {
			return MakeError(
				TransportError, "Varlink socket " + socket_path + " is in use by another process");
		}
		auto err = path::FileDelete(socket_path);
		if (err != error::NoError) {
			return err.WithContext("Could not remove stale varlink socket");
		}
	}

	acceptor_.open(endpoint.protocol(), ec);
	if (ec) {
		return MakeError(TransportError, "Could not open varlink acceptor: " + ec.message());
	}

	// bind() creates the socket file with the process umask applied, so clamp it
	// to owner+group (0660) for the call instead of chmod'ing afterwards and
	// leaving a window where the socket is world-connectable.
	mode_t old_umask = umask(0117);
	acceptor_.bind(endpoint, ec);
	umask(old_umask);
	if (ec) {
		return MakeError(
			TransportError, "Could not bind varlink socket " + socket_path + ": " + ec.message());
	}
	// bind() created the file; from here on Cancel() must unlink it.
	socket_path_ = socket_path;

	acceptor_.listen(asio::socket_base::max_listen_connections, ec);
	if (ec) {
		return MakeError(TransportError, "Could not listen on varlink socket: " + ec.message());
	}

	AsyncAccept();

	return error::NoError;
}

void Server::AsyncAccept() {
	acceptor_.async_accept(
		[this](const boost::system::error_code &ec, asio::local::stream_protocol::socket socket) {
			if (ec) {
				if (ec == asio::error::operation_aborted || !acceptor_.is_open()) {
					return;
				}
				// Log and keep accepting, or the service would stay silent until the
				// daemon restarts. Wait before retrying: on EMFILE accept keeps failing
				// until an fd is freed, so retrying at once spins.
				logger_.Error("Could not accept varlink connection: " + ec.message());
				accept_retry_timer_.AsyncWait(std::chrono::seconds(1), [this](error::Error err) {
					// Cancel() delivers operation_canceled; an already expired wait
					// can still fire after it, hence the acceptor check.
					if (err == error::NoError && acceptor_.is_open()) {
						AsyncAccept();
					}
				});
				return;
			}

			auto conn = std::make_shared<Connection>(*this, std::move(socket));
			connections_.insert(conn);
			conn->ReadFrame();

			AsyncAccept();
		});
}

void Server::RemoveConnection(const ConnectionPtr &conn) {
	conn->Close();
	connections_.erase(conn);
}

void Server::Cancel() {
	boost::system::error_code ec;
	if (acceptor_.is_open()) {
		acceptor_.close(ec);
	}
	accept_retry_timer_.Cancel();

	for (auto &conn : connections_) {
		conn->Close();
	}
	connections_.clear();

	if (!socket_path_.empty()) {
		path::FileDelete(socket_path_);
		socket_path_.clear();
	}
}

} // namespace varlink
} // namespace common
} // namespace mender
