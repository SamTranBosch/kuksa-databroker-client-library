#include "KuksaClient.hpp"

// Include gRPC and Proto headers here.
#include <grpcpp/grpcpp.h>
#include "kuksa/val/v1/val.grpc.pb.h"
#include "kuksa/val/v1/types.pb.h"

#include <sstream>
#include <thread>
#include <chrono>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <set>
#include <future>

// nlohmann/json (header-only)
#include <nlohmann/json.hpp>

// Convenience alias for JSON.
using json = nlohmann::json;

namespace KuksaClient {

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
        oss << "unknown or unset";
        break;
  }
  return oss.str();
}

//=============================================================================
// Overloaded setValueImpl Definitions
//=============================================================================
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

//=============================================================================
// Private Implementation (pImpl)
//=============================================================================
struct KuksaClient::Impl {
  // Create the gRPC channel and stub.
  std::shared_ptr<grpc::Channel> channel;
  std::unique_ptr<kuksa::val::v1::VAL::Stub> stub;
};

//=============================================================================
// Constructors & Destructor
//=============================================================================
KuksaClient::KuksaClient(const Config &config)
    : serverURI_(config.serverURI),
      debug_(config.debug),
      signalPaths_(config.signalPaths),
      config_(config),
      pImpl(std::make_unique<Impl>()) {
  // Start the reconnection thread
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
        lock.unlock(); // Release lock before reconnection attempt

        if (attemptReconnection()) {
          consecutiveFailures = 0;
          restartSubscriptions();
        } else {
          // Exponential backoff with jitter for failed reconnection
          consecutiveFailures++;
          int delay = std::min(1 << std::min(consecutiveFailures - 1, 6), maxDelay);

          // Sleep in smaller chunks to respond to shutdown quickly
          for (int i = 0; i < delay && !shouldStop_.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
          }
        }
      }
    }
  });
}

KuksaClient::KuksaClient(const std::string &configFile) : pImpl(std::make_unique<Impl>()) {
  if (!parseConfig(configFile, config_)) {
    throw std::runtime_error("Failed to load configuration from " + configFile);
  }
  serverURI_   = config_.serverURI;
  debug_       = config_.debug;
  signalPaths_ = config_.signalPaths;

  // Start the reconnection thread
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
        lock.unlock(); // Release lock before reconnection attempt

        if (attemptReconnection()) {
          consecutiveFailures = 0;
          restartSubscriptions();
        } else {
          // Exponential backoff with jitter for failed reconnection
          consecutiveFailures++;
          int delay = std::min(1 << std::min(consecutiveFailures - 1, 6), maxDelay);

          // Sleep in smaller chunks to respond to shutdown quickly
          for (int i = 0; i < delay && !shouldStop_.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
          }
        }
      }
    }
  });
}

KuksaClient::~KuksaClient() {
  std::cout << "KuksaClient destructor starting..." << std::endl;

  // Signal all threads to stop immediately
  shouldStop_.store(true);

  // Mark as disconnected to prevent new operations
  connected_.store(false);

  // Disable auto-reconnect to prevent race conditions
  autoReconnect_.store(false);

  // Wake up reconnection thread
  {
    std::lock_guard<std::mutex> lock(reconnectMutex_);
    reconnectCV_.notify_all();
  }

  // Give threads a moment to recognize shutdown signal
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Join reconnection thread first
  try {
    if (reconnectThread_.joinable()) {
      std::cout << "Joining reconnection thread..." << std::endl;
      reconnectThread_.join();
      std::cout << "Reconnection thread joined successfully" << std::endl;
    }
  } catch (const std::exception& e) {
    std::cerr << "Exception while joining reconnection thread: " << e.what() << std::endl;
  }

  // Join all subscription threads with timeout
  try {
    joinAllSubscriptionsWithTimeout();
  } catch (const std::exception& e) {
    std::cerr << "Exception while joining subscription threads: " << e.what() << std::endl;
  }

  // Clear subscription tracking after all threads are stopped
  {
    std::lock_guard<std::mutex> lock1(subscriptionsMutex_);
    std::lock_guard<std::mutex> lock2(subscriptionPathsMutex_);
    activeSubscriptions_.clear();
    activeSubscriptionPaths_.clear();
  }

  // Reset gRPC resources
  try {
    if (pImpl) {
      pImpl->stub.reset();
      pImpl->channel.reset();
    }
  } catch (const std::exception& e) {
    std::cerr << "Exception while cleaning up gRPC resources: " << e.what() << std::endl;
  }

  std::cout << "KuksaClient destructor completed" << std::endl;
}

//=============================================================================
// Public Member Functions Implementations
//=============================================================================

void KuksaClient::connect() {
  // Prevent multiple concurrent connection attempts
  std::lock_guard<std::mutex> lock(connectionMutex_);

  if (shouldStop_.load()) {
    throw std::runtime_error("Client is shutting down, cannot connect");
  }

  try {
    std::cout << "Connecting to " << serverURI_ << "..." << std::endl;

    // Clean up any existing connection first
    if (pImpl->stub) {
      pImpl->stub.reset();
    }
    if (pImpl->channel) {
      pImpl->channel.reset();
    }

    // Create new connection
    pImpl->channel = grpc::CreateChannel(serverURI_, grpc::InsecureChannelCredentials());
    if (!pImpl->channel) {
      throw std::runtime_error("Failed to create gRPC channel");
    }

    pImpl->stub = kuksa::val::v1::VAL::NewStub(pImpl->channel);
    if (!pImpl->stub) {
      throw std::runtime_error("Failed to create gRPC stub");
    }

    // Test the connection with a simple call
    kuksa::val::v1::GetServerInfoRequest request;
    kuksa::val::v1::GetServerInfoResponse response;
    grpc::ClientContext context;

    // Set a reasonable timeout for connection test
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
    context.set_deadline(deadline);

    grpc::Status status = pImpl->stub->GetServerInfo(&context, request, &response);

    if (!status.ok()) {
      throw std::runtime_error("Connection test failed: " + status.error_message());
    }

    connected_.store(true);
    std::cout << "Successfully connected to " << serverURI_ << std::endl;
  } catch (const std::exception& e) {
    connected_.store(false);
    // Clean up failed connection resources
    if (pImpl->stub) {
      pImpl->stub.reset();
    }
    if (pImpl->channel) {
      pImpl->channel.reset();
    }
    std::cerr << "Failed to connect to " << serverURI_ << ": " << e.what() << std::endl;
    throw;
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

bool KuksaClient::reconnect() {
  return attemptReconnection();
}

std::string KuksaClient::getCurrentValue(const std::string &entryPath) {
  return getValue(entryPath, GV_CURRENT, false);
}

std::string KuksaClient::getTargetValue(const std::string &entryPath) {
  return getValue(entryPath, GV_TARGET, true);
}

std::string KuksaClient::getValue(const std::string &entryPath, GetView view, bool target) {
  std::string valueStr = "";
  if (!pImpl->stub || !connected_.load()) {
      std::cerr << "Client not connected. Aborting getValue()" << std::endl;
      return valueStr;
  }
  kuksa::val::v1::GetRequest request;
  auto* entryReq = request.add_entries();
  entryReq->set_path(entryPath);

  if (view == GV_TARGET) {
    entryReq->set_view(kuksa::val::v1::VIEW_TARGET_VALUE);
  } else { // FT_VALUE
    entryReq->set_view(kuksa::val::v1::VIEW_CURRENT_VALUE);
  }
  
  kuksa::val::v1::GetResponse response;
  grpc::ClientContext context;
  grpc::Status status = pImpl->stub->Get(&context, request, &response);
  if (!status.ok()) {
      std::cerr << " Get() RPC failed: " << status.error_message() << std::endl;
      // Check if this is a connection failure
      if (status.error_code() == grpc::StatusCode::UNAVAILABLE ||
          status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED ||
          status.error_message().find("Socket closed") != std::string::npos) {
        handleConnectionFailure();
      }
      return valueStr;
  }
  if (response.error().code() != 0) {
      std::cerr << " Get() global error: " << response.error().message() << std::endl;
      return valueStr;
  }
  if (response.entries_size() == 0) {
      std::cerr << " Get(): No entries returned for \"" << entryPath << "\"." << std::endl;
      return valueStr;
  }
  
  // Retrieve and convert the DataEntry value.
  const auto &dataEntry = response.entries(0);
  if (view == GV_TARGET) {
    valueStr = DataPointToString(dataEntry.actuator_target());
  } else { // FT_VALUE
    valueStr = DataPointToString(dataEntry.value());
  }
  
  if (view == GV_TARGET) {
    std::cout << " GetTargetValue(): Value for \"" << entryPath << "\": " << valueStr << std::endl;
  } else { // FT_VALUE
    std::cout << " GetCurrentValue(): Value for \"" << entryPath << "\": " << valueStr << std::endl;
  }

  return valueStr;
}

void KuksaClient::streamUpdate(const std::string &entryPath, float newValue) {
  if (!pImpl->stub || !connected_.load()) {
    std::cerr << "Client not connected. Aborting streamUpdate()." << std::endl;
    return;
  }
  grpc::ClientContext context;
  auto stream = pImpl->stub->StreamedUpdate(&context);
  kuksa::val::v1::StreamedUpdateRequest request;
  auto* update = request.add_updates();
  auto* dataEntry = update->mutable_entry();
  dataEntry->set_path(entryPath);
  // We assume FIELD_VALUE (from the proto) for streaming updates.
  dataEntry->mutable_value()->set_float_(newValue);
  update->add_fields(kuksa::val::v1::FIELD_VALUE);

  if (!stream->Write(request)) {
    std::cerr << "streamUpdate: Failed to write request." << std::endl;
    return;
  }
  stream->WritesDone();

  kuksa::val::v1::StreamedUpdateResponse response;
  while (stream->Read(&response)) {
    if (response.error().code() != 0) {
      std::cerr << "streamUpdate response error: " << response.error().message() << std::endl;
    } else {
      std::cout << "streamUpdate: Received a response." << std::endl;
    }
  }
  grpc::Status status = stream->Finish();
  if (!status.ok()) {
    std::cerr << "streamUpdate RPC failed: " << status.error_message() << std::endl;
    // Check if this is a connection failure
    if (status.error_code() == grpc::StatusCode::UNAVAILABLE ||
        status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED ||
        status.error_message().find("Socket closed") != std::string::npos) {
      handleConnectionFailure();
    }
  } else {
    std::cout << "streamUpdate: Completed successfully." << std::endl;
  }
}

void KuksaClient::subscribeTargetValue(const std::string &entryPath,
  std::function<void(const std::string &, const std::string &, const int &)> userCallback) {
  subscribe(entryPath, userCallback, FT_ACTUATOR_TARGET);
}

void KuksaClient::subscribeCurrentValue(const std::string &entryPath,
  std::function<void(const std::string &, const std::string &, const int &)> userCallback) {
  subscribe(entryPath, userCallback, FT_VALUE);
}

void KuksaClient::subscribe(const std::string &entryPath,
    std::function<void(const std::string &, const std::string &, const int &)> userCallback, int field) {
  if (!pImpl->stub || !connected_.load()) {
    std::cerr << "Client not connected. Aborting subscribe()." << std::endl;
    return;
  }
  kuksa::val::v1::SubscribeRequest request;
  auto* subEntry = request.add_entries();
  subEntry->set_path(entryPath);
  // Here we use the proto’s own view for subscription.
  subEntry->set_view(kuksa::val::v1::VIEW_ALL);

  if (field == FT_ACTUATOR_TARGET) {
    subEntry->add_fields(kuksa::val::v1::FIELD_ACTUATOR_TARGET);
  } else { // FT_VALUE
    subEntry->add_fields(kuksa::val::v1::FIELD_VALUE);
  }
  
  grpc::ClientContext context;
  auto reader = pImpl->stub->Subscribe(&context, request);
  std::cout << "Subscription: Listening on \"" << entryPath << "\"." << std::endl;
  kuksa::val::v1::SubscribeResponse response;
  int updateCount = 0;
  while (reader->Read(&response)) {
    ++updateCount;
    std::cout << "Subscription: Received update #" << updateCount << " for \"" << entryPath << "\"" << std::endl;
    for (int i = 0; i < response.updates_size(); ++i) {
      const auto &upd = response.updates(i);
      std::string updatePath = upd.entry().path();
      // dataPointToString is used internally.
      std::string updateValue;
      if (field == FT_ACTUATOR_TARGET) {
        updateValue = getTargetValue(entryPath);
        std::cout << "  Update TargetValue: " << updatePath << " -> " << updateValue << std::endl;
      } else { // FT_VALUE
        updateValue = getCurrentValue(entryPath);
        std::cout << "  Update CurrentValue: " << updatePath << " -> " << updateValue << std::endl;
      }

      if (userCallback)
        userCallback(updatePath, updateValue, field);
    }
  }
  grpc::Status status = reader->Finish();
  if (!status.ok()) {
    std::cerr << "subscribe RPC failed: " << status.error_message() << std::endl;
    // Check if this is a connection failure
    if (status.error_code() == grpc::StatusCode::UNAVAILABLE ||
        status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED ||
        status.error_message().find("Socket closed") != std::string::npos) {
      handleConnectionFailure();
    }
  } else {
    std::cout << "Subscription: Stream finished for \"" << entryPath << "\"." << std::endl;
  }
}

void KuksaClient::subscribeWithReconnect(const std::string &entryPath,
                                         std::function<void(const std::string &, const std::string &, const int &)> userCallback,
                                         int field) {
  // Create unique key for this subscription (path + field type)
  std::string subscriptionKey = entryPath + "_" + std::to_string(field);

  // Enhanced thread-safe check and registration for duplicate subscriptions
  {
    std::unique_lock<std::mutex> lock(subscriptionPathsMutex_);

    // Wait for any pending subscription creation to complete for this key
    while (activeSubscriptionPaths_.count(subscriptionKey + "_PENDING") > 0 && !shouldStop_.load()) {
      lock.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      lock.lock();
    }

    if (activeSubscriptionPaths_.count(subscriptionKey) > 0) {
      std::cout << "Subscription already exists for " << subscriptionKey << ", skipping duplicate" << std::endl;
      return;
    }

    // Double-check for early termination
    if (shouldStop_.load()) {
      std::cout << "Client is shutting down, skipping subscription for " << subscriptionKey << std::endl;
      return;
    }

    // Mark as pending to block other threads while we set up this subscription
    activeSubscriptionPaths_.insert(subscriptionKey + "_PENDING");
    activeSubscriptionPaths_.insert(subscriptionKey);
  }

  // Store subscription info for restart after reconnection
  {
    std::lock_guard<std::mutex> lock(subscriptionsMutex_);
    activeSubscriptions_.push_back({entryPath, userCallback, field});
  }

  // Create shared pointer for thread-safe resource management
  auto threadData = std::make_shared<std::tuple<std::string, std::function<void(const std::string &, const std::string &, const int &)>, int, std::string>>(
    entryPath, userCallback, field, subscriptionKey);

  // Start subscription in a loop that handles reconnection
  subscriptionThreads_.emplace_back([this, threadData]() {
    const auto& [entryPath, userCallback, field, subscriptionKey] = *threadData;
    std::cout << "Starting subscription thread for " << subscriptionKey << std::endl;

    // Clear the pending marker now that thread is running
    {
      std::lock_guard<std::mutex> lock(subscriptionPathsMutex_);
      activeSubscriptionPaths_.erase(subscriptionKey + "_PENDING");
    }

    // Local gRPC resources for this thread - avoid shared access
    std::unique_ptr<grpc::ClientContext> context;
    std::unique_ptr<grpc::ClientReader<kuksa::val::v1::SubscribeResponse>> reader;
    std::atomic<bool> threadActive{true};

    // Cleanup handler to ensure proper resource cleanup on thread exit
    auto cleanup = [&]() {
      threadActive.store(false);
      if (reader) {
        try {
          reader.reset();
        } catch (...) {
          std::cerr << "Exception during reader cleanup for " << subscriptionKey << std::endl;
        }
      }
      if (context) {
        try {
          context.reset();
        } catch (...) {
          std::cerr << "Exception during context cleanup for " << subscriptionKey << std::endl;
        }
      }
    };

    while (!shouldStop_.load() && threadActive.load()) {
      if (connected_.load()) {
        try {
          std::cout << "Attempting to subscribe to " << entryPath << std::endl;

          // Create fresh gRPC resources for this attempt
          context = std::make_unique<grpc::ClientContext>();

          // Set a reasonable deadline to prevent hanging
          auto deadline = std::chrono::system_clock::now() + std::chrono::minutes(5);
          context->set_deadline(deadline);

          // Prepare subscription request
          kuksa::val::v1::SubscribeRequest request;
          auto* subEntry = request.add_entries();
          subEntry->set_path(entryPath);
          subEntry->set_view(kuksa::val::v1::VIEW_ALL);

          if (field == FT_ACTUATOR_TARGET) {
            subEntry->add_fields(kuksa::val::v1::FIELD_ACTUATOR_TARGET);
          } else {
            subEntry->add_fields(kuksa::val::v1::FIELD_VALUE);
          }

          // Thread-safe access to stub with local copy
          std::unique_ptr<kuksa::val::v1::VAL::Stub> localStub;
          {
            // Brief lock to copy stub pointer - minimize contention
            if (pImpl && pImpl->stub) {
              localStub = kuksa::val::v1::VAL::NewStub(pImpl->channel);
            }
          }

          if (localStub) {
            reader = localStub->Subscribe(context.get(), request);

            kuksa::val::v1::SubscribeResponse response;
            int updateCount = 0;

            while (!shouldStop_.load() && threadActive.load()) {
              try {
                bool readSuccess = reader->Read(&response);
                if (!readSuccess) {
                  std::cout << "Subscription stream ended for " << entryPath << std::endl;
                  break;
                }

                ++updateCount;
                std::cout << "Subscription: Received update #" << updateCount << " for \"" << entryPath << "\"" << std::endl;

                for (int i = 0; i < response.updates_size() && !shouldStop_.load(); ++i) {
                  const auto &upd = response.updates(i);
                  std::string updatePath = upd.entry().path();
                  std::string updateValue;

                  // Extract value directly from response instead of making new RPC calls
                  if (field == FT_ACTUATOR_TARGET && upd.entry().has_actuator_target()) {
                    updateValue = DataPointToString(upd.entry().actuator_target());
                  } else if (field == FT_VALUE && upd.entry().has_value()) {
                    updateValue = DataPointToString(upd.entry().value());
                  } else {
                    // Fallback to RPC call if data not in response
                    if (field == FT_ACTUATOR_TARGET) {
                      updateValue = getTargetValue(entryPath);
                    } else {
                      updateValue = getCurrentValue(entryPath);
                    }
                  }

                  if (userCallback && !shouldStop_.load()) {
                    try {
                      userCallback(updatePath, updateValue, field);
                    } catch (const std::exception& e) {
                      std::cerr << "Exception in subscription callback for " << entryPath << ": " << e.what() << std::endl;
                    }
                  }
                }
              } catch (const std::exception& e) {
                std::cerr << "Exception during subscription read for " << entryPath << ": " << e.what() << std::endl;
                break;
              }
            }

            // Check status after read loop ends
            try {
              grpc::Status status = reader->Finish();
              if (!status.ok() && !shouldStop_.load()) {
                std::cerr << "Subscription RPC failed for " << entryPath << ": " << status.error_message() << std::endl;
                if (status.error_code() == grpc::StatusCode::UNAVAILABLE ||
                    status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED ||
                    status.error_message().find("Socket closed") != std::string::npos) {
                  handleConnectionFailure();
                }
              }
            } catch (const std::exception& e) {
              std::cerr << "Exception during status finish for " << entryPath << ": " << e.what() << std::endl;
            }
          }

          std::cout << "Subscription ended for " << entryPath << std::endl;
        } catch (const std::exception& e) {
          std::cerr << "Subscription error for " << entryPath << ": " << e.what() << std::endl;
          if (!shouldStop_.load()) {
            handleConnectionFailure();
          }
        }

        // Clean up resources before retry
        cleanup();
      }

      // Wait before retrying if connection is down and not stopping
      if (!connected_.load() && !shouldStop_.load() && threadActive.load()) {
        std::cout << "Waiting to retry subscription for " << entryPath << "..." << std::endl;

        // Use shorter sleep intervals and check for shutdown
        for (int i = 0; i < 20 && !shouldStop_.load() && threadActive.load(); ++i) {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
      }
    }

    // Final cleanup
    cleanup();

    // Remove from active subscriptions when thread exits
    {
      std::lock_guard<std::mutex> lock(subscriptionPathsMutex_);
      activeSubscriptionPaths_.erase(subscriptionKey);
      activeSubscriptionPaths_.erase(subscriptionKey + "_PENDING");
    }

    std::cout << "Subscription thread ending for " << subscriptionKey << std::endl;
  });
}

void KuksaClient::subscribeAll(std::function<void(const std::string &, const std::string &, const int &)> userCallback) {
  // Subscribe to both TARGET and VALUE for each path, but only create one subscription per unique combination
  for (const auto &path : signalPaths_) {
    // Subscribe to target values
    subscribeWithReconnect(path, userCallback, FT_ACTUATOR_TARGET);

    // Subscribe to current values
    subscribeWithReconnect(path, userCallback, FT_VALUE);
  }
}

void KuksaClient::joinAllSubscriptions() {
  joinAllSubscriptionsWithTimeout();
}

void KuksaClient::joinAllSubscriptionsWithTimeout() {
  std::cout << "Joining " << subscriptionThreads_.size() << " subscription threads with timeout..." << std::endl;

  const auto timeout = std::chrono::seconds(5);
  size_t joinedCount = 0;
  size_t detachedCount = 0;

  for (auto &t : subscriptionThreads_) {
    try {
      if (t.joinable()) {
        // Try to join with timeout using async approach
        auto future = std::async(std::launch::async, [&t]() {
          t.join();
        });

        if (future.wait_for(timeout) == std::future_status::ready) {
          joinedCount++;
        } else {
          std::cerr << "Thread join timeout, detaching thread" << std::endl;
          t.detach();
          detachedCount++;
        }
      }
    } catch (const std::exception& e) {
      std::cerr << "Exception while joining subscription thread: " << e.what() << std::endl;
      try {
        if (t.joinable()) {
          t.detach();
          detachedCount++;
        }
      } catch (...) {
        std::cerr << "Failed to detach thread after join exception" << std::endl;
      }
    }
  }

  subscriptionThreads_.clear();
  std::cout << "Subscription threads cleanup completed - joined: " << joinedCount
            << ", detached: " << detachedCount << std::endl;
}

void KuksaClient::detachAllSubscriptions() {
  for (auto &t : subscriptionThreads_) {
    if (t.joinable())
      t.detach();
  }
  subscriptionThreads_.clear();
}

void KuksaClient::getServerInfo() {
  if (!pImpl->stub || !connected_.load()) {
    std::cerr << "Client not connected. Aborting getServerInfo()." << std::endl;
    return;
  }
  kuksa::val::v1::GetServerInfoRequest request;
  kuksa::val::v1::GetServerInfoResponse response;
  grpc::ClientContext context;
  grpc::Status status = pImpl->stub->GetServerInfo(&context, request, &response);
  if (!status.ok()) {
    std::cerr << "getServerInfo RPC failed: " << status.error_message() << std::endl;
    // Check if this is a connection failure
    if (status.error_code() == grpc::StatusCode::UNAVAILABLE ||
        status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED ||
        status.error_message().find("Socket closed") != std::string::npos) {
      handleConnectionFailure();
    }
    return;
  }
  std::cout << "Server Info:" << std::endl;
  std::cout << "  Name: " << response.name() << std::endl;
  std::cout << "  Version: " << response.version() << std::endl;
}

//=============================================================================
// Static Helper: Configuration Parsing
//=============================================================================
bool KuksaClient::parseConfig(const std::string &filename, Config &config) {
  std::ifstream configFile(filename);
  if (!configFile.is_open()) {
    std::cerr << "Unable to open config file: " << filename << std::endl;
    return false;
  }
  try {
    json j;
    configFile >> j;
    config.serverURI = j.at("broker").at("serverURI").get<std::string>();
    config.debug     = j.value("debug", false);
    if (j.contains("signal") && j["signal"].is_array()) {
      for (const auto &item : j["signal"]) {
        if (item.contains("path"))
          config.signalPaths.push_back(item["path"].get<std::string>());
      }
    }
  } catch (const std::exception &e) {
    std::cerr << "Failed to parse " << filename << ": " << e.what() << std::endl;
    return false;
  }
  return true;
}


//=============================================================================
// Template Helper Implementations
//=============================================================================
template <typename T>
void KuksaClient::setValueInternalImpl(const std::string &entryPath, const T &newValue, int field) {
  if (!pImpl->stub || !connected_.load()) {
    std::cerr << "Client not connected. Aborting setValue()." << std::endl;
    return;
  }

  kuksa::val::v1::SetRequest request;
  auto* update = request.add_updates();
  kuksa::val::v1::DataEntry* dataEntry = update->mutable_entry();
  dataEntry->set_path(entryPath);

  if (field == FT_ACTUATOR_TARGET) {
    setValueImpl(dataEntry->mutable_actuator_target(), newValue);
    update->add_fields(kuksa::val::v1::FIELD_ACTUATOR_TARGET);
  } else { // FT_VALUE
    setValueImpl(dataEntry->mutable_value(), newValue);
    update->add_fields(kuksa::val::v1::FIELD_VALUE);
  }

  kuksa::val::v1::SetResponse response;
  grpc::ClientContext context;
  grpc::Status status = pImpl->stub->Set(&context, request, &response);
  if (!status.ok()) {
    std::cerr << "Set() RPC failed: " << status.error_message() << std::endl;
    // Check if this is a connection failure
    if (status.error_code() == grpc::StatusCode::UNAVAILABLE ||
        status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED ||
        status.error_message().find("Socket closed") != std::string::npos) {
      handleConnectionFailure();
    }
    return;
  }
  if (response.error().code() != 0) {
    std::cerr << "Set() global error: " << response.error().message() << std::endl;
  } else {
    if (field == FT_ACTUATOR_TARGET) {
      std::cout << "SetTargetValue(): Updated \"" << entryPath << "\" - " << (int)newValue << std::endl;
    } else { // FT_VALUE
      std::cout << "SetCurrentValue(): Updated \"" << entryPath << "\" - " << (int)newValue << std::endl;
    }
  }
}

// Explicit instantiation for bool
template void KuksaClient::setValueInternalImpl<bool>(const std::string&, const bool&, int);
template void KuksaClient::setValueInternalImpl<uint8_t>(const std::string&, const uint8_t&, int);
template void KuksaClient::setValueInternalImpl<uint16_t>(const std::string&, const uint16_t&, int);
template void KuksaClient::setValueInternalImpl<uint32_t>(const std::string&, const uint32_t&, int);
template void KuksaClient::setValueInternalImpl<uint64_t>(const std::string&, const uint64_t&, int);
template void KuksaClient::setValueInternalImpl<int8_t>(const std::string&, const int8_t&, int);
template void KuksaClient::setValueInternalImpl<int16_t>(const std::string&, const int16_t&, int);
template void KuksaClient::setValueInternalImpl<int32_t>(const std::string&, const int32_t&, int);
template void KuksaClient::setValueInternalImpl<int64_t>(const std::string&, const int64_t&, int);
template void KuksaClient::setValueInternalImpl<float>(const std::string&, const float&, int);
template void KuksaClient::setValueInternalImpl<double>(const std::string&, const double&, int);

// Note: If you plan to use setValueInternalImpl with additional types,
// you can explicitly instantiate them here.

//=============================================================================
// Conversion Specializations Implementations
//=============================================================================
bool KuksaClient::convertString(const std::string &str, bool &out) {
  if (str == "true") {
    out = true;
    return true;
  } else if (str == "false") {
    out = false;
    return true;
  }
  std::istringstream iss(str);
  int temp = 0;
  iss >> temp;
  if (iss.fail() || !iss.eof())
    return false;
  out = (temp != 0);
  return true;
}

bool KuksaClient::convertString(const std::string &str, uint8_t &out) {
  uint32_t temp = 0;
  if (!convertString(str, temp))
    return false;
  if (temp > std::numeric_limits<uint8_t>::max())
    return false;
  out = static_cast<uint8_t>(temp);
  return true;
}

bool KuksaClient::convertString(const std::string &str, uint16_t &out) {
  uint32_t temp = 0;
  if (!convertString(str, temp))
    return false;
  if (temp > std::numeric_limits<uint16_t>::max())
    return false;
  out = static_cast<uint16_t>(temp);
  return true;
}

bool KuksaClient::convertString(const std::string &str, uint32_t &out) {
  std::istringstream iss(str);
  iss >> out;
  return !iss.fail() && iss.eof();
}

//=============================================================================
// Reconnection Helper Methods Implementation
//=============================================================================
bool KuksaClient::attemptReconnection() {
  static thread_local int reconnectAttempt = 0; // Thread-local to avoid race conditions
  const int maxDelay = 60; // Maximum delay in seconds

  // Use lock to prevent concurrent reconnection attempts
  std::lock_guard<std::mutex> lock(connectionMutex_);

  // Check if already connected or shutting down
  if (connected_.load() || shouldStop_.load()) {
    return connected_.load();
  }

  try {
    std::cout << "Attempting to reconnect to " << serverURI_
              << " (attempt " << ++reconnectAttempt << ")..." << std::endl;

    // Clean up existing resources
    if (pImpl->stub) {
      pImpl->stub.reset();
    }
    if (pImpl->channel) {
      pImpl->channel.reset();
    }

    // Create new connection
    pImpl->channel = grpc::CreateChannel(serverURI_, grpc::InsecureChannelCredentials());
    if (!pImpl->channel) {
      throw std::runtime_error("Failed to create gRPC channel during reconnection");
    }

    pImpl->stub = kuksa::val::v1::VAL::NewStub(pImpl->channel);
    if (!pImpl->stub) {
      throw std::runtime_error("Failed to create gRPC stub during reconnection");
    }

    // Test the connection with a simple call
    kuksa::val::v1::GetServerInfoRequest request;
    kuksa::val::v1::GetServerInfoResponse response;
    grpc::ClientContext context;

    // Set timeout for reconnection test
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
    context.set_deadline(deadline);

    grpc::Status status = pImpl->stub->GetServerInfo(&context, request, &response);

    if (status.ok()) {
      connected_.store(true);
      reconnectAttempt = 0; // Reset counter on successful connection
      std::cout << "Successfully reconnected to " << serverURI_ << std::endl;
      return true;
    } else {
      std::cerr << "Reconnection failed: " << status.error_message() << std::endl;
    }
  } catch (const std::exception& e) {
    std::cerr << "Reconnection exception: " << e.what() << std::endl;
    // Clean up failed resources
    if (pImpl->stub) {
      pImpl->stub.reset();
    }
    if (pImpl->channel) {
      pImpl->channel.reset();
    }
  }

  // Don't sleep while holding the lock - release and sleep outside
  return false;
}

void KuksaClient::handleConnectionFailure() {
  if (connected_.load()) {
    connected_.store(false);
    std::cerr << "Connection to " << serverURI_ << " lost. Auto-reconnect: "
              << (autoReconnect_.load() ? "enabled" : "disabled") << std::endl;

    if (autoReconnect_.load()) {
      reconnectCV_.notify_one();
    }
  }
}

void KuksaClient::restartSubscriptions() {
  std::lock_guard<std::mutex> lock(subscriptionsMutex_);
  std::cout << "Restarting " << activeSubscriptions_.size() << " subscriptions after reconnection" << std::endl;

  // Clear the active subscription paths to allow resubscription after reconnection
  {
    std::lock_guard<std::mutex> pathLock(subscriptionPathsMutex_);
    activeSubscriptionPaths_.clear();
    std::cout << "Cleared subscription path tracking for reconnection" << std::endl;
  }

  // The subscribeWithReconnect threads will automatically retry their subscriptions
  // when they detect connected_ is true again and can now re-register their paths
}

} // namespace KuksaClient
