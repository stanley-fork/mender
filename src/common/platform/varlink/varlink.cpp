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

#include <cassert>
#include <string>
#include <utility>
#include <vector>

#include <common/json.hpp>

namespace mender {
namespace common {
namespace varlink {

// https://varlink.org/Service
static const char *kServiceInterfaceDescription =
	R"(# The Varlink Service Interface is provided by every varlink service. It
# describes the service and the interfaces it implements.
interface org.varlink.service

# Get a list of all the interfaces a service provides and information
# about the implementation.
method GetInfo() -> (
  vendor: string,
  product: string,
  version: string,
  url: string,
  interfaces: []string
)

# Get the description of an interface that is implemented by this service.
method GetInterfaceDescription(interface: string) -> (description: string)

# The requested interface was not found.
error InterfaceNotFound (interface: string)

# The requested method was not found
error MethodNotFound (method: string)

# The interface defines the requested method, but the service does not
# implement it.
error MethodNotImplemented (method: string)

# One of the passed parameters is invalid.
error InvalidParameter (parameter: string)

# Client is denied access.
error PermissionDenied ()

# Method is expected to be called with 'more' set.
error ExpectedMore ()
)";

const VarlinkErrorCategoryClass VarlinkErrorCategory;

const char *VarlinkErrorCategoryClass::name() const noexcept {
	return "VarlinkErrorCategory";
}

std::string VarlinkErrorCategoryClass::message(int code) const {
	switch (code) {
	case NoError:
		return "Success";
	case TransportError:
		return "Varlink transport error";
	}
	assert(false);
	return "Unknown";
}

error::Error MakeError(VarlinkErrorCode code, const std::string &msg) {
	return error::Error(std::error_condition(code, VarlinkErrorCategory), msg);
}

Reply ErrorReply(const std::string &error_name, const json::KeyValueMap &parameters) {
	Reply r;
	r.error_name = error_name;
	r.parameters = json::Json::Object(parameters);
	return r;
}

void Subscribers::Add(Replier emit) {
	subscribers_.push_back(std::move(emit));
}

void Subscribers::Emit(Reply reply) {
	reply.continues = true;
	// An error frame is terminal and unblocks the peer's pipelined calls, whose
	// handlers may Add() to this list from inside emit(): index, and copy the
	// Replier out before calling it, so a push_back cannot invalidate either.
	for (size_t i = 0; i < subscribers_.size();) {
		Replier emit = subscribers_[i];
		if (emit(reply)) {
			i++;
		} else {
			subscribers_.erase(subscribers_.begin() + i);
		}
	}
}

void Server::AddMethodHandler(const std::string &method, MethodHandler handler) {
	methods_[method] = std::move(handler);
}

void Server::SetServiceInfo(
	const std::string &vendor,
	const std::string &product,
	const std::string &version,
	const std::string &url) {
	svc_vendor_ = vendor;
	svc_product_ = product;
	svc_version_ = version;
	svc_url_ = url;
}

void Server::AddInterface(const std::string &name, const std::string &description) {
	interface_descriptions_[name] = description;
}

bool Server::HasInterface(const std::string &name) const {
	if (interface_descriptions_.count(name) > 0) {
		return true;
	}
	// Handlers may be registered without publishing an IDL.
	const std::string prefix = name + ".";
	for (const auto &kv : methods_) {
		if (kv.first.compare(0, prefix.size(), prefix) == 0) {
			return true;
		}
	}
	return false;
}

void Server::RegisterServiceInterface() {
	// Standard introspection interface that every varlink service exposes, so
	// generic clients (varlinkctl, libvarlink, ...) can discover this service.
	AddInterface("org.varlink.service", kServiceInterfaceDescription);
	AddMethodHandler("org.varlink.service.GetInfo", [this](const MethodCall &, const Replier &) {
		std::vector<std::string> interfaces;
		for (const auto &kv : interface_descriptions_) {
			interfaces.push_back(kv.first);
		}
		Reply r;
		r.parameters.Set("vendor", svc_vendor_);
		r.parameters.Set("product", svc_product_);
		r.parameters.Set("version", svc_version_);
		r.parameters.Set("url", svc_url_);
		r.parameters.Set("interfaces", interfaces);
		return r;
	});
	AddMethodHandler(
		"org.varlink.service.GetInterfaceDescription",
		[this](const MethodCall &call, const Replier &) {
			auto exp_iface = call.parameters.Get("interface").and_then([](const json::Json &j) {
				return j.GetString();
			});
			if (!exp_iface) {
				return ErrorReply(kErrorInvalidParameter, {{"parameter", "interface"}});
			}
			auto it = interface_descriptions_.find(exp_iface.value());
			if (it == interface_descriptions_.end()) {
				return ErrorReply(kErrorInterfaceNotFound, {{"interface", exp_iface.value()}});
			}
			Reply r;
			r.parameters.Set("description", it->second);
			return r;
		});
}

} // namespace varlink
} // namespace common
} // namespace mender
