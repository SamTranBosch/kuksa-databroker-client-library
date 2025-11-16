// Copyright (c) 2025 Eclipse Foundation.
//
// This program and the accompanying materials are made available under the
// terms of the MIT License which is available at
// https://opensource.org/licenses/MIT.
//
// SPDX-License-Identifier: MIT

#include "vapiclient.hpp"
#include "KuksaClient.hpp"

VAPIClient& VAPIClient::instance() {
  static VAPIClient inst;
  return inst;
}

VAPIClient::VAPIClient() = default;

VAPIClient::~VAPIClient() {
  shutdown();
}

bool VAPIClient::connectToServer(const std::string &serverURI,
                                 const std::vector<std::string> &signalPaths) {
  std::lock_guard lock(mClientsMtx_);

  if (mClients_.count(serverURI)) {
    std::cout << "[VAPIClient] Already connected to " << serverURI << std::endl;
    return true;
  }

  // Build configuration
  KuksaClient::Config cfg;
  cfg.serverURI = serverURI;
  cfg.debug = false;
  cfg.signalPaths = signalPaths;

  try {
    auto client = std::make_unique<KuksaClient::KuksaClient>(cfg);
    client->connect();

    ClientEntry entry;
    entry.client = std::move(client);
    mClients_.try_emplace(serverURI, std::move(entry));

    std::cout << "[VAPIClient] Connected to " << serverURI << std::endl;
    return true;
  }
  catch (const std::exception &e) {
    std::cerr << "[VAPIClient] Failed to connect to "
              << serverURI << ": " << e.what() << std::endl;

    // Create client entry for future reconnection attempts
    try {
      auto client = std::make_unique<KuksaClient::KuksaClient>(cfg);
      ClientEntry entry;
      entry.client = std::move(client);
      mClients_.try_emplace(serverURI, std::move(entry));
      std::cout << "[VAPIClient] Created client entry for future reconnection to "
                << serverURI << std::endl;
    } catch (const std::exception &e2) {
      std::cerr << "[VAPIClient] Failed to create client entry: " << e2.what() << std::endl;
    }

    return false;
  }
}

KuksaClient::KuksaClient* VAPIClient::findClient(const std::string &serverURI) {
  std::lock_guard lock(mClientsMtx_);
  auto it = mClients_.find(serverURI);
  if (it == mClients_.end()) {
    std::cerr << "[VAPIClient] No client for server " << serverURI << std::endl;
    return nullptr;
  }
  return it->second.client.get();
}

KuksaClient::KuksaClient* VAPIClient::findClient(const std::string &serverURI) const {
  std::lock_guard lock(mClientsMtx_);
  auto it = mClients_.find(serverURI);
  if (it == mClients_.end()) {
    std::cerr << "[VAPIClient] No client for server " << serverURI << std::endl;
    return nullptr;
  }
  return it->second.client.get();
}

bool VAPIClient::getCurrentValue(const std::string &serverURI,
                                 const std::string &path,
                                 std::string       &outValue) {
  auto *c = findClient(serverURI);
  if (!c) return false;
  return c->getCurrentValue(path, outValue);
}

bool VAPIClient::getTargetValue(const std::string &serverURI,
                                const std::string &path,
                                std::string       &outValue) {
  auto *c = findClient(serverURI);
  if (!c) return false;
  return c->getTargetValue(path, outValue);
}

bool VAPIClient::subscribeCurrent(const std::string               &serverURI,
                                  const std::vector<std::string> &paths,
                                  SubscribeCallback               callback) {
  auto *c = findClient(serverURI);
  if (!c) return false;

  // Simply call subscribe for each path - KuksaClient handles all threading
  for (const auto &p : paths) {
    try {
      c->subscribeCurrentValue(p, callback);
    } catch (const std::exception& e) {
      std::cerr << "[VAPIClient] Failed to subscribe to current value for "
                << p << ": " << e.what() << std::endl;
    }
  }

  return true;
}

bool VAPIClient::subscribeTarget(const std::string               &serverURI,
                                 const std::vector<std::string> &paths,
                                 SubscribeCallback               callback) {
  auto *c = findClient(serverURI);
  if (!c) return false;

  // Simply call subscribe for each path - KuksaClient handles all threading
  for (const auto &p : paths) {
    try {
      c->subscribeTargetValue(p, callback);
    } catch (const std::exception& e) {
      std::cerr << "[VAPIClient] Failed to subscribe to target value for "
                << p << ": " << e.what() << std::endl;
    }
  }

  return true;
}

bool VAPIClient::isConnected(const std::string &serverURI) const {
  auto *c = findClient(serverURI);
  return c ? c->isConnected() : false;
}

void VAPIClient::setAutoReconnect(const std::string &serverURI, bool enabled) {
  auto *c = findClient(serverURI);
  if (c) {
    c->setAutoReconnect(enabled);
    std::cout << "[VAPIClient] Auto-reconnect "
              << (enabled ? "enabled" : "disabled")
              << " for " << serverURI << std::endl;
  }
}

bool VAPIClient::forceReconnect(const std::string &serverURI) {
  auto *c = findClient(serverURI);
  if (c) {
    std::cout << "[VAPIClient] Forcing reconnection to " << serverURI << std::endl;
    try {
      c->connect();
      return true;
    } catch (const std::exception& e) {
      std::cerr << "[VAPIClient] Reconnection failed: " << e.what() << std::endl;
      return false;
    }
  }
  return false;
}

void VAPIClient::shutdown() {
  std::cout << "[VAPIClient] Shutting down all clients..." << std::endl;

  std::lock_guard lock(mClientsMtx_);

  // KuksaClient destructor handles all cleanup automatically!
  // No manual thread management needed - RAII does it all
  mClients_.clear();

  std::cout << "[VAPIClient] Shutdown completed" << std::endl;
}

void VAPIClient::shutdownAsync() {
  std::cout << "[VAPIClient] Starting async shutdown..." << std::endl;

  // With the new design, async shutdown is the same as regular shutdown
  // KuksaClient destructor uses cancellation to quickly stop threads
  shutdown();

  std::cout << "[VAPIClient] Async shutdown completed" << std::endl;
}

//==============================================================================
// Template Method Implementations
//==============================================================================

template<typename T>
bool VAPIClient::getCurrentValueAs(const std::string &serverURI,
                                   const std::string &path,
                                   T                  &out) {
  auto *c = findClient(serverURI);
  return c ? c->getCurrentValue(path, out) : false;
}

template<typename T>
bool VAPIClient::getTargetValueAs(const std::string &serverURI,
                                  const std::string &path,
                                  T                  &out) {
  auto *c = findClient(serverURI);
  return c ? c->getTargetValue(path, out) : false;
}

template<typename T>
bool VAPIClient::setCurrentValue(const std::string &serverURI,
                                 const std::string &path,
                                 const T           &newValue) {
  auto *c = findClient(serverURI);
  if (!c) return false;
  c->setCurrentValue(path, newValue);
  return true;
}

template<typename T>
bool VAPIClient::setTargetValue(const std::string &serverURI,
                                const std::string &path,
                                const T           &newValue) {
  auto *c = findClient(serverURI);
  if (!c) return false;
  c->setTargetValue(path, newValue);
  return true;
}

//==============================================================================
// Explicit Template Instantiations
//==============================================================================

template bool VAPIClient::getCurrentValueAs<int>(const std::string&, const std::string&, int&);
template bool VAPIClient::getCurrentValueAs<float>(const std::string&, const std::string&, float&);
template bool VAPIClient::getCurrentValueAs<double>(const std::string&, const std::string&, double&);
template bool VAPIClient::getCurrentValueAs<bool>(const std::string&, const std::string&, bool&);
template bool VAPIClient::getCurrentValueAs<uint8_t>(const std::string&, const std::string&, uint8_t&);
template bool VAPIClient::getCurrentValueAs<std::string>(const std::string&, const std::string&, std::string&);

template bool VAPIClient::getTargetValueAs<int>(const std::string&, const std::string&, int&);
template bool VAPIClient::getTargetValueAs<float>(const std::string&, const std::string&, float&);
template bool VAPIClient::getTargetValueAs<double>(const std::string&, const std::string&, double&);
template bool VAPIClient::getTargetValueAs<bool>(const std::string&, const std::string&, bool&);
template bool VAPIClient::getTargetValueAs<uint8_t>(const std::string&, const std::string&, uint8_t&);
template bool VAPIClient::getTargetValueAs<std::string>(const std::string&, const std::string&, std::string&);

template bool VAPIClient::setCurrentValue<int>(const std::string&, const std::string&, const int&);
template bool VAPIClient::setCurrentValue<float>(const std::string&, const std::string&, const float&);
template bool VAPIClient::setCurrentValue<double>(const std::string&, const std::string&, const double&);
template bool VAPIClient::setCurrentValue<bool>(const std::string&, const std::string&, const bool&);
template bool VAPIClient::setCurrentValue<uint8_t>(const std::string&, const std::string&, const uint8_t&);
template bool VAPIClient::setCurrentValue<std::string>(const std::string&, const std::string&, const std::string&);

template bool VAPIClient::setTargetValue<int>(const std::string&, const std::string&, const int&);
template bool VAPIClient::setTargetValue<float>(const std::string&, const std::string&, const float&);
template bool VAPIClient::setTargetValue<double>(const std::string&, const std::string&, const double&);
template bool VAPIClient::setTargetValue<bool>(const std::string&, const std::string&, const bool&);
template bool VAPIClient::setTargetValue<uint8_t>(const std::string&, const std::string&, const uint8_t&);
template bool VAPIClient::setTargetValue<std::string>(const std::string&, const std::string&, const std::string&);
