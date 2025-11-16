// Copyright (c) 2025 Eclipse Foundation.
//
// This program and the accompanying materials are made available under the
// terms of the MIT License which is available at
// https://opensource.org/licenses/MIT.
//
// SPDX-License-Identifier: MIT

#include "KuksaClient.hpp"

// gRPC and Protocol Buffer includes
#include <grpcpp/grpcpp.h>
#include "kuksa/val/v1/val.grpc.pb.h"
#include "kuksa/val/v1/types.pb.h"

// Standard library includes
#include <sstream>
#include <thread>
#include <chrono>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <set>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <limits>

// JSON library (header-only)
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace KuksaClient {

//==============================================================================
// Helper Functions
//==============================================================================

/**
 * @brief Convert protobuf Datapoint to string representation
 */
std::string DataPointToString(const kuksa::val::v1::Datapoint &dp) {
  std::ostringstream oss;
  switch (dp.value_case()) {
    case kuksa::val::v1::Datapoint::kString:
      oss << dp.string();
      break;
    case kuksa::val::v1::Datapoint::kBool:
      oss << (dp.bool_() ? "true" : "false");
      break;
    case kuksa::val::v1::Datapoint::kInt32:
      oss << dp.int32();
      break;
    case kuksa::val::v1::Datapoint::kInt64:
      oss << dp.int64();
      break;
    case kuksa::val::v1::Datapoint::kUint32:
      oss << dp.uint32();
      break;
    case kuksa::val::v1::Datapoint::kUint64:
      oss << dp.uint64();
      break;
    case kuksa::val::v1::Datapoint::kFloat:
      oss << dp.float_();
      break;
    case kuksa::val::v1::Datapoint::kDouble:
      oss << dp.double_();
      break;
    default:
      oss << "";
      break;
  }
  return oss.str();
}

/**
 * @brief Set protobuf Datapoint from typed value (overloaded functions)
 */
void setValueImpl(kuksa::val::v1::Datapoint *dp, bool value) {
  dp->set_bool_(value);
}
void setValueImpl(kuksa::val::v1::Datapoint *dp, uint8_t value) {
  dp->set_uint32(value);
}
void setValueImpl(kuksa::val::v1::Datapoint *dp, uint16_t value) {
  dp->set_uint32(value);
}
void setValueImpl(kuksa::val::v1::Datapoint *dp, uint32_t value) {
  dp->set_uint32(value);
}
void setValueImpl(kuksa::val::v1::Datapoint *dp, uint64_t value) {
  dp->set_uint64(value);
}
void setValueImpl(kuksa::val::v1::Datapoint *dp, int8_t value) {
  dp->set_int32(value);
}
void setValueImpl(kuksa::val::v1::Datapoint *dp, int16_t value) {
  dp->set_int32(value);
}
void setValueImpl(kuksa::val::v1::Datapoint *dp, int32_t value) {
  dp->set_int32(value);
}
void setValueImpl(kuksa::val::v1::Datapoint *dp, int64_t value) {
  dp->set_int64(value);
}
void setValueImpl(kuksa::val::v1::Datapoint *dp, float value) {
  dp->set_float_(value);
}
void setValueImpl(kuksa::val::v1::Datapoint *dp, double value) {
  dp->set_double_(value);
}
void setValueImpl(kuksa::val::v1::Datapoint *dp, const std::string &value) {
  dp->set_string(value);
}

//==============================================================================
// Private Implementation Structure (pImpl)
//==============================================================================

/**
 * @brief Private implementation - hides all gRPC details from public header
 */
struct KuksaClient::Impl {
  // gRPC connection resources
  std::shared_ptr<grpc::Channel> channel;  // shared_ptr for safe copying
  std::shared_ptr<kuksa::val::v1::VAL::Stub> stub;  // shared_ptr for thread safety
};

/**
 * @brief Subscription context - shared between client and subscription thread
 *
 * This structure is critical for thread safety:
 * - shared_ptr allows both client and thread to access safely
 * - grpc::ClientContext can be cancelled from destructor
 * - No use-after-free even if thread outlives client (won't happen, but safe)
 */
struct KuksaClient::SubscriptionContext {
  std::string entryPath;
  std::string subscriptionKey;
  SubscribeCallback callback;
  FieldType field;

  // Thread control
  std::thread thread;
  std::atomic<bool> shouldStop{false};

  // gRPC resources for cancellation
  std::shared_ptr<grpc::ClientContext> grpcContext;
  std::mutex contextMutex;  // Protects grpcContext creation/access

  // Reference to parent client (for connection status)
  std::atomic<bool>* connected;
  std::atomic<bool>* parentShouldStop;

  SubscriptionContext(
    const std::string& path,
    const std::string& key,
    SubscribeCallback cb,
    FieldType f,
    std::atomic<bool>* conn,
    std::atomic<bool>* stop
  ) : entryPath(path),
      subscriptionKey(key),
      callback(std::move(cb)),
      field(f),
      connected(conn),
      parentShouldStop(stop) {}

  // Cancel active gRPC operation
  void cancel() {
    shouldStop.store(true);
    std::lock_guard<std::mutex> lock(contextMutex);
    if (grpcContext) {
      grpcContext->TryCancel();
    }
  }
};

//==============================================================================
// Constructors & Destructor
//==============================================================================

KuksaClient::KuksaClient(const Config &config)
    : pImpl_(std::make_unique<Impl>()),
      config_(config),
      serverURI_(config.serverURI),
      debug_(config.debug) {

  if (debug_) {
    std::cout << "[KuksaClient] Initializing client for " << serverURI_ << std::endl;
  }

  // Start reconnection thread
  reconnectThread_ = std::thread([this]() {
    int consecutiveFailures = 0;
    const int maxDelay = 60;

    while (!shouldStop_.load()) {
      std::unique_lock<std::mutex> lock(reconnectMutex_);
      reconnectCV_.wait_for(lock, std::chrono::seconds(5), [this]() {
        return shouldStop_.load() || (!connected_.load() && autoReconnect_.load());
      });

      if (shouldStop_.load()) break;

      if (!connected_.load() && autoReconnect_.load()) {
        lock.unlock();

        if (attemptReconnection()) {
          consecutiveFailures = 0;
          restartSubscriptions();
        } else {
          consecutiveFailures++;
          int delay = std::min(1 << std::min(consecutiveFailures - 1, 6), maxDelay);

          // Sleep in small chunks to respond quickly to shutdown
          for (int i = 0; i < delay && !shouldStop_.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
          }
        }
      }
    }

    if (debug_) {
      std::cout << "[KuksaClient] Reconnection thread exiting" << std::endl;
    }
  });
}

KuksaClient::KuksaClient(const std::string &configFile)
    : pImpl_(std::make_unique<Impl>()) {

  if (!parseConfig(configFile, config_)) {
    throw std::runtime_error("Failed to load configuration from " + configFile);
  }

  serverURI_ = config_.serverURI;
  debug_ = config_.debug;

  if (debug_) {
    std::cout << "[KuksaClient] Initializing client from config file" << std::endl;
  }

  // Start reconnection thread (same as above)
  reconnectThread_ = std::thread([this]() {
    int consecutiveFailures = 0;
    const int maxDelay = 60;

    while (!shouldStop_.load()) {
      std::unique_lock<std::mutex> lock(reconnectMutex_);
      reconnectCV_.wait_for(lock, std::chrono::seconds(5), [this]() {
        return shouldStop_.load() || (!connected_.load() && autoReconnect_.load());
      });

      if (shouldStop_.load()) break;

      if (!connected_.load() && autoReconnect_.load()) {
        lock.unlock();

        if (attemptReconnection()) {
          consecutiveFailures = 0;
          restartSubscriptions();
        } else {
          consecutiveFailures++;
          int delay = std::min(1 << std::min(consecutiveFailures - 1, 6), maxDelay);

          for (int i = 0; i < delay && !shouldStop_.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
          }
        }
      }
    }

    if (debug_) {
      std::cout << "[KuksaClient] Reconnection thread exiting" << std::endl;
    }
  });
}

KuksaClient::~KuksaClient() {
  if (debug_) {
    std::cout << "[KuksaClient] Destructor starting - cleaning up resources" << std::endl;
  }

  // Step 1: Signal all threads to stop
  shouldStop_.store(true);
  connected_.store(false);
  autoReconnect_.store(false);

  // Step 2: Cancel all subscription gRPC operations
  {
    std::lock_guard<std::mutex> lock(subscriptionsMutex_);
    if (debug_) {
      std::cout << "[KuksaClient] Cancelling " << subscriptionContexts_.size()
                << " subscription contexts" << std::endl;
    }
    for (auto& ctx : subscriptionContexts_) {
      if (ctx) {
        ctx->cancel();
      }
    }
  }

  // Step 3: Wake up reconnection thread
  {
    std::lock_guard<std::mutex> lock(reconnectMutex_);
    reconnectCV_.notify_all();
  }

  // Step 4: Join reconnection thread
  try {
    if (reconnectThread_.joinable()) {
      if (debug_) {
        std::cout << "[KuksaClient] Joining reconnection thread" << std::endl;
      }
      reconnectThread_.join();
    }
  } catch (const std::exception& e) {
    std::cerr << "[KuksaClient] Exception joining reconnection thread: "
              << e.what() << std::endl;
  }

  // Step 5: Join all subscription threads
  // Because we cancelled gRPC operations, threads will exit quickly - no timeout needed!
  {
    std::lock_guard<std::mutex> lock(subscriptionsMutex_);
    if (debug_) {
      std::cout << "[KuksaClient] Joining " << subscriptionContexts_.size()
                << " subscription threads" << std::endl;
    }

    for (auto& ctx : subscriptionContexts_) {
      if (ctx && ctx->thread.joinable()) {
        try {
          ctx->thread.join();
        } catch (const std::exception& e) {
          std::cerr << "[KuksaClient] Exception joining subscription thread: "
                    << e.what() << std::endl;
        }
      }
    }
    subscriptionContexts_.clear();
  }

  // Step 6: Clean up gRPC resources
  {
    std::lock_guard<std::mutex> lock(connectionMutex_);
    if (pImpl_) {
      pImpl_->stub.reset();
      pImpl_->channel.reset();
    }
  }

  // Step 7: Clear tracking structures
  {
    std::lock_guard<std::mutex> lock(subscriptionsMutex_);
    activeSubscriptions_.clear();
    activeSubscriptionKeys_.clear();
  }

  if (debug_) {
    std::cout << "[KuksaClient] Destructor completed successfully" << std::endl;
  }
}

//==============================================================================
// Connection Management
//==============================================================================

void KuksaClient::connect() {
  std::lock_guard<std::mutex> lock(connectionMutex_);

  if (shouldStop_.load()) {
    throw std::runtime_error("Client is shutting down");
  }

  if (debug_) {
    std::cout << "[KuksaClient] Connecting to " << serverURI_ << std::endl;
  }

  // Clean up existing connection
  if (pImpl_->stub) pImpl_->stub.reset();
  if (pImpl_->channel) pImpl_->channel.reset();

  // Create gRPC channel with optimized settings
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 30000);
  args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);
  args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
  args.SetInt(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 300000);
  args.SetInt(GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS, 60000);

  pImpl_->channel = grpc::CreateCustomChannel(
    serverURI_,
    grpc::InsecureChannelCredentials(),
    args
  );

  if (!pImpl_->channel) {
    throw std::runtime_error("Failed to create gRPC channel");
  }

  // Wait for channel to be ready
  auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
  if (!pImpl_->channel->WaitForConnected(deadline)) {
    pImpl_->channel.reset();
    throw std::runtime_error("Channel connection timeout");
  }

  pImpl_->stub = kuksa::val::v1::VAL::NewStub(pImpl_->channel);
  if (!pImpl_->stub) {
    throw std::runtime_error("Failed to create gRPC stub");
  }

  // Test connection with GetServerInfo
  kuksa::val::v1::GetServerInfoRequest request;
  kuksa::val::v1::GetServerInfoResponse response;
  grpc::ClientContext context;

  auto rpcDeadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
  context.set_deadline(rpcDeadline);

  grpc::Status status = pImpl_->stub->GetServerInfo(&context, request, &response);

  if (!status.ok()) {
    pImpl_->stub.reset();
    pImpl_->channel.reset();
    throw std::runtime_error("Connection test failed: " + status.error_message());
  }

  connected_.store(true);

  if (debug_) {
    std::cout << "[KuksaClient] Connected successfully to " << serverURI_ << std::endl;
    std::cout << "[KuksaClient] Server: " << response.name()
              << " v" << response.version() << std::endl;
  }
}

bool KuksaClient::isConnected() const {
  return connected_.load();
}

void KuksaClient::setAutoReconnect(bool enabled) {
  autoReconnect_.store(enabled);
  if (enabled && !connected_.load()) {
    reconnectCV_.notify_one();
  }
}

//==============================================================================
// Get/Set Operations
//==============================================================================

std::string KuksaClient::getValueInternal(const std::string &entryPath, FieldType field) {
  // Get stub reference safely
  std::shared_ptr<kuksa::val::v1::VAL::Stub> stubCopy;
  {
    std::lock_guard<std::mutex> lock(connectionMutex_);
    if (!pImpl_ || !pImpl_->stub || !connected_.load()) {
      if (debug_) {
        std::cerr << "[KuksaClient] Not connected, cannot get value for "
                  << entryPath << std::endl;
      }
      return "";
    }
    stubCopy = pImpl_->stub;
  }

  // Build request
  kuksa::val::v1::GetRequest request;
  auto* entryReq = request.add_entries();
  entryReq->set_path(entryPath);
  entryReq->set_view(field == FT_ACTUATOR_TARGET ?
                     kuksa::val::v1::VIEW_TARGET_VALUE :
                     kuksa::val::v1::VIEW_CURRENT_VALUE);

  // Execute RPC without holding connection lock
  kuksa::val::v1::GetResponse response;
  grpc::ClientContext context;
  auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
  context.set_deadline(deadline);

  grpc::Status status = stubCopy->Get(&context, request, &response);

  if (!status.ok()) {
    if (debug_) {
      std::cerr << "[KuksaClient] Get failed for " << entryPath << ": "
                << status.error_message() << std::endl;
    }

    // Trigger reconnection on network errors
    if (status.error_code() == grpc::StatusCode::UNAVAILABLE ||
        status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
      handleConnectionFailure();
    }
    return "";
  }

  if (response.error().code() != 0 || response.entries_size() == 0) {
    return "";
  }

  const auto &dataEntry = response.entries(0);
  const auto& dp = (field == FT_ACTUATOR_TARGET) ?
                    dataEntry.actuator_target() :
                    dataEntry.value();

  std::string result = DataPointToString(dp);

  if (debug_) {
    std::cout << "[KuksaClient] Get " << entryPath << " = " << result << std::endl;
  }

  return result;
}

template <typename T>
void KuksaClient::setValueInternalImpl(const std::string &entryPath,
                                       const T &newValue,
                                       FieldType field) {
  // Get stub reference safely
  std::shared_ptr<kuksa::val::v1::VAL::Stub> stubCopy;
  {
    std::lock_guard<std::mutex> lock(connectionMutex_);
    if (!pImpl_ || !pImpl_->stub || !connected_.load()) {
      if (debug_) {
        std::cerr << "[KuksaClient] Not connected, cannot set value for "
                  << entryPath << std::endl;
      }
      return;
    }
    stubCopy = pImpl_->stub;
  }

  // Build request
  kuksa::val::v1::SetRequest request;
  auto* update = request.add_updates();
  auto* dataEntry = update->mutable_entry();
  dataEntry->set_path(entryPath);

  if (field == FT_ACTUATOR_TARGET) {
    setValueImpl(dataEntry->mutable_actuator_target(), newValue);
    update->add_fields(kuksa::val::v1::FIELD_ACTUATOR_TARGET);
  } else {
    setValueImpl(dataEntry->mutable_value(), newValue);
    update->add_fields(kuksa::val::v1::FIELD_VALUE);
  }

  // Execute RPC without holding connection lock
  kuksa::val::v1::SetResponse response;
  grpc::ClientContext context;
  auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
  context.set_deadline(deadline);

  grpc::Status status = stubCopy->Set(&context, request, &response);

  if (!status.ok()) {
    if (debug_) {
      std::cerr << "[KuksaClient] Set failed for " << entryPath << ": "
                << status.error_message() << std::endl;
    }

    // Trigger reconnection on network errors
    if (status.error_code() == grpc::StatusCode::UNAVAILABLE ||
        status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
      handleConnectionFailure();
    }
    return;
  }

  if (response.error().code() != 0) {
    if (debug_) {
      std::cerr << "[KuksaClient] Set error: " << response.error().message() << std::endl;
    }
  } else if (debug_) {
    std::cout << "[KuksaClient] Set " << entryPath << " = " << newValue << std::endl;
  }
}

// Explicit template instantiations
template void KuksaClient::setValueInternalImpl<bool>(const std::string&, const bool&, FieldType);
template void KuksaClient::setValueInternalImpl<uint8_t>(const std::string&, const uint8_t&, FieldType);
template void KuksaClient::setValueInternalImpl<uint16_t>(const std::string&, const uint16_t&, FieldType);
template void KuksaClient::setValueInternalImpl<uint32_t>(const std::string&, const uint32_t&, FieldType);
template void KuksaClient::setValueInternalImpl<uint64_t>(const std::string&, const uint64_t&, FieldType);
template void KuksaClient::setValueInternalImpl<int8_t>(const std::string&, const int8_t&, FieldType);
template void KuksaClient::setValueInternalImpl<int16_t>(const std::string&, const int16_t&, FieldType);
template void KuksaClient::setValueInternalImpl<int32_t>(const std::string&, const int32_t&, FieldType);
template void KuksaClient::setValueInternalImpl<int64_t>(const std::string&, const int64_t&, FieldType);
template void KuksaClient::setValueInternalImpl<float>(const std::string&, const float&, FieldType);
template void KuksaClient::setValueInternalImpl<double>(const std::string&, const double&, FieldType);
template void KuksaClient::setValueInternalImpl<std::string>(const std::string&, const std::string&, FieldType);

//==============================================================================
// Subscription Operations
//==============================================================================

void KuksaClient::subscribeCurrentValue(const std::string &entryPath, SubscribeCallback callback) {
  subscribe(entryPath, FT_VALUE, callback);
}

void KuksaClient::subscribeTargetValue(const std::string &entryPath, SubscribeCallback callback) {
  subscribe(entryPath, FT_ACTUATOR_TARGET, callback);
}

void KuksaClient::subscribe(const std::string &entryPath, FieldType field, SubscribeCallback callback) {
  if (shouldStop_.load()) {
    if (debug_) {
      std::cout << "[KuksaClient] Client is stopping, ignoring subscription request" << std::endl;
    }
    return;
  }

  std::string subscriptionKey = entryPath + "_" + std::to_string(field);

  // Check for duplicate subscription
  {
    std::lock_guard<std::mutex> lock(subscriptionsMutex_);
    if (activeSubscriptionKeys_.count(subscriptionKey) > 0) {
      if (debug_) {
        std::cout << "[KuksaClient] Subscription already exists for "
                  << subscriptionKey << std::endl;
      }
      return;
    }
    activeSubscriptionKeys_.insert(subscriptionKey);
  }

  // Store subscription info for reconnection
  {
    std::lock_guard<std::mutex> lock(subscriptionsMutex_);
    activeSubscriptions_.push_back({entryPath, callback, field});
  }

  // Create subscription context
  auto ctx = std::make_shared<SubscriptionContext>(
    entryPath,
    subscriptionKey,
    callback,
    field,
    &connected_,
    &shouldStop_
  );

  // Start subscription thread
  ctx->thread = std::thread([this, ctx]() {
    this->subscriptionThreadFunc(ctx);
  });

  // Store context for cancellation
  {
    std::lock_guard<std::mutex> lock(subscriptionsMutex_);
    subscriptionContexts_.push_back(ctx);
  }

  if (debug_) {
    std::cout << "[KuksaClient] Started subscription for " << subscriptionKey << std::endl;
  }
}

void KuksaClient::subscriptionThreadFunc(std::shared_ptr<SubscriptionContext> ctx) {
  if (debug_) {
    std::cout << "[KuksaClient] Subscription thread started for "
              << ctx->subscriptionKey << std::endl;
  }

  while (!ctx->shouldStop.load() && !ctx->parentShouldStop->load()) {
    // Wait for connection
    while (!ctx->connected->load() && !ctx->shouldStop.load() && !ctx->parentShouldStop->load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    if (ctx->shouldStop.load() || ctx->parentShouldStop->load()) {
      break;
    }

    try {
      // Get stub reference safely
      std::shared_ptr<kuksa::val::v1::VAL::Stub> stubCopy;
      {
        std::lock_guard<std::mutex> lock(connectionMutex_);
        if (pImpl_ && pImpl_->stub && connected_.load()) {
          stubCopy = pImpl_->stub;
        }
      }

      if (!stubCopy) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        continue;
      }

      // Create gRPC context
      auto grpcCtx = std::make_shared<grpc::ClientContext>();
      {
        std::lock_guard<std::mutex> lock(ctx->contextMutex);
        ctx->grpcContext = grpcCtx;
      }

      // Build subscribe request
      kuksa::val::v1::SubscribeRequest request;
      auto* subEntry = request.add_entries();
      subEntry->set_path(ctx->entryPath);
      subEntry->set_view(kuksa::val::v1::VIEW_ALL);

      if (ctx->field == FT_ACTUATOR_TARGET) {
        subEntry->add_fields(kuksa::val::v1::FIELD_ACTUATOR_TARGET);
      } else {
        subEntry->add_fields(kuksa::val::v1::FIELD_VALUE);
      }

      // Create subscription stream
      auto reader = stubCopy->Subscribe(grpcCtx.get(), request);

      if (debug_) {
        std::cout << "[KuksaClient] Subscribed to " << ctx->entryPath << std::endl;
      }

      // Read updates
      kuksa::val::v1::SubscribeResponse response;
      while (!ctx->shouldStop.load() && !ctx->parentShouldStop->load()) {
        bool readSuccess = reader->Read(&response);

        if (!readSuccess) {
          if (debug_) {
            std::cout << "[KuksaClient] Subscription stream ended for "
                      << ctx->entryPath << std::endl;
          }
          break;
        }

        // Process updates
        for (int i = 0; i < response.updates_size(); ++i) {
          const auto &upd = response.updates(i);
          std::string updatePath = upd.entry().path();
          std::string updateValue;
          bool hasValue = false;

          // Extract value from response
          if (ctx->field == FT_ACTUATOR_TARGET && upd.entry().has_actuator_target()) {
            const auto& dp = upd.entry().actuator_target();
            if (dp.value_case() != kuksa::val::v1::Datapoint::VALUE_NOT_SET) {
              updateValue = DataPointToString(dp);
              hasValue = true;
            }
          } else if (ctx->field == FT_VALUE && upd.entry().has_value()) {
            const auto& dp = upd.entry().value();
            if (dp.value_case() != kuksa::val::v1::Datapoint::VALUE_NOT_SET) {
              updateValue = DataPointToString(dp);
              hasValue = true;
            }
          }

          // Invoke callback
          if (hasValue && ctx->callback && !ctx->shouldStop.load()) {
            try {
              ctx->callback(updatePath, updateValue, ctx->field);
            } catch (const std::exception& e) {
              std::cerr << "[KuksaClient] Exception in subscription callback: "
                        << e.what() << std::endl;
            }
          }
        }
      }

      // Clean up gRPC context
      {
        std::lock_guard<std::mutex> lock(ctx->contextMutex);
        ctx->grpcContext.reset();
      }

      grpc::Status status = reader->Finish();
      if (!status.ok() && !ctx->shouldStop.load()) {
        if (debug_) {
          std::cerr << "[KuksaClient] Subscription error: "
                    << status.error_message() << std::endl;
        }

        if (status.error_code() == grpc::StatusCode::UNAVAILABLE ||
            status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED) {
          handleConnectionFailure();
        }
      }

    } catch (const std::exception& e) {
      std::cerr << "[KuksaClient] Subscription exception: " << e.what() << std::endl;
    }

    // Retry delay if not stopping
    if (!ctx->shouldStop.load() && !ctx->parentShouldStop->load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
  }

  if (debug_) {
    std::cout << "[KuksaClient] Subscription thread exiting for "
              << ctx->subscriptionKey << std::endl;
  }
}

//==============================================================================
// Reconnection Helpers
//==============================================================================

bool KuksaClient::attemptReconnection() {
  std::lock_guard<std::mutex> lock(connectionMutex_);

  if (connected_.load() || shouldStop_.load()) {
    return connected_.load();
  }

  try {
    if (debug_) {
      std::cout << "[KuksaClient] Attempting reconnection to " << serverURI_ << std::endl;
    }

    // Clean up existing resources
    if (pImpl_) {
      pImpl_->stub.reset();
      pImpl_->channel.reset();
    }

    // Create new connection
    pImpl_->channel = grpc::CreateChannel(serverURI_, grpc::InsecureChannelCredentials());
    if (!pImpl_->channel) {
      return false;
    }

    pImpl_->stub = kuksa::val::v1::VAL::NewStub(pImpl_->channel);
    if (!pImpl_->stub) {
      return false;
    }

    // Test connection
    kuksa::val::v1::GetServerInfoRequest request;
    kuksa::val::v1::GetServerInfoResponse response;
    grpc::ClientContext context;

    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
    context.set_deadline(deadline);

    grpc::Status status = pImpl_->stub->GetServerInfo(&context, request, &response);

    if (status.ok()) {
      connected_.store(true);
      if (debug_) {
        std::cout << "[KuksaClient] Reconnected successfully" << std::endl;
      }
      return true;
    }

  } catch (const std::exception& e) {
    if (debug_) {
      std::cerr << "[KuksaClient] Reconnection exception: " << e.what() << std::endl;
    }
  }

  return false;
}

void KuksaClient::handleConnectionFailure() {
  if (connected_.load()) {
    connected_.store(false);
    if (debug_) {
      std::cerr << "[KuksaClient] Connection lost to " << serverURI_ << std::endl;
    }

    if (autoReconnect_.load()) {
      reconnectCV_.notify_one();
    }
  }
}

void KuksaClient::restartSubscriptions() {
  std::lock_guard<std::mutex> lock(subscriptionsMutex_);

  if (debug_) {
    std::cout << "[KuksaClient] Restarting subscriptions after reconnection" << std::endl;
  }

  // Subscription threads will automatically retry when they detect connection is back
  // No need to manually restart - the subscription loop handles it
}

//==============================================================================
// String Conversion Specializations
//==============================================================================

bool KuksaClient::convertString(const std::string &str, bool &out) {
  if (str == "true" || str == "1") {
    out = true;
    return true;
  } else if (str == "false" || str == "0") {
    out = false;
    return true;
  }
  return false;
}

bool KuksaClient::convertString(const std::string &str, uint8_t &out) {
  uint32_t temp = 0;
  if (!convertString(str, temp) || temp > std::numeric_limits<uint8_t>::max()) {
    return false;
  }
  out = static_cast<uint8_t>(temp);
  return true;
}

bool KuksaClient::convertString(const std::string &str, uint16_t &out) {
  uint32_t temp = 0;
  if (!convertString(str, temp) || temp > std::numeric_limits<uint16_t>::max()) {
    return false;
  }
  out = static_cast<uint16_t>(temp);
  return true;
}

bool KuksaClient::convertString(const std::string &str, uint32_t &out) {
  std::istringstream iss(str);
  iss >> out;
  return !iss.fail() && iss.eof();
}

//==============================================================================
// Configuration Parsing
//==============================================================================

bool KuksaClient::parseConfig(const std::string &filename, Config &config) {
  std::ifstream configFile(filename);
  if (!configFile.is_open()) {
    std::cerr << "[KuksaClient] Unable to open config file: " << filename << std::endl;
    return false;
  }

  try {
    json j;
    configFile >> j;

    config.serverURI = j.at("broker").at("serverURI").get<std::string>();
    config.debug = j.value("debug", false);

    if (j.contains("signal") && j["signal"].is_array()) {
      for (const auto &item : j["signal"]) {
        if (item.contains("path")) {
          config.signalPaths.push_back(item["path"].get<std::string>());
        }
      }
    }

    return true;
  } catch (const std::exception &e) {
    std::cerr << "[KuksaClient] Failed to parse config: " << e.what() << std::endl;
    return false;
  }
}

} // namespace KuksaClient
