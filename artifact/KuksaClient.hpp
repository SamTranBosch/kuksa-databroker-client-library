// Copyright (c) 2025 Eclipse Foundation.
//
// This program and the accompanying materials are made available under the
// terms of the MIT License which is available at
// https://opensource.org/licenses/MIT.
//
// SPDX-License-Identifier: MIT

/**
 * @file KuksaClient.hpp
 * @brief Thread-safe gRPC client for KUKSA Databroker
 *
 * This library provides a simplified, type-safe interface for communicating
 * with KUKSA Databroker using gRPC. It handles connection management,
 * automatic reconnection, and subscription lifecycle automatically.
 *
 * Key Features:
 * - Automatic connection management with exponential backoff retry
 * - Type-safe value get/set operations
 * - Thread-safe subscription management with automatic reconnection
 * - Clean RAII design - no manual resource cleanup needed
 * - Hidden gRPC implementation details (pImpl pattern)
 *
 * Example Usage:
 * @code
 *   KuksaClient::Config config;
 *   config.serverURI = "127.0.0.1:55555";
 *   config.signalPaths = {"Vehicle.Speed", "Vehicle.Gear"};
 *
 *   KuksaClient::KuksaClient client(config);
 *   client.connect();
 *
 *   // Get value with automatic type conversion
 *   float speed;
 *   if (client.getCurrentValue("Vehicle.Speed", speed)) {
 *       std::cout << "Speed: " << speed << " km/h" << std::endl;
 *   }
 *
 *   // Set value
 *   client.setCurrentValue("Vehicle.Gear", 3);
 *
 *   // Subscribe to updates
 *   client.subscribeCurrentValue("Vehicle.Speed",
 *       [](const std::string& path, const std::string& value, int field) {
 *           std::cout << path << " = " << value << std::endl;
 *       }
 *   );
 *
 *   // Destructor automatically handles cleanup - no manual join needed!
 * @endcode
 *
 * Thread Safety:
 * - All public methods are thread-safe
 * - Callbacks are invoked from subscription threads
 * - User callbacks should not block for extended periods
 *
 * Memory Safety:
 * - Uses RAII for all resource management
 * - Automatic cleanup on destruction
 * - No manual thread joining required
 * - Safe to destroy even with active subscriptions
 */

#ifndef KUKSA_CLIENT_HPP
#define KUKSA_CLIENT_HPP

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <set>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <sstream>
#include <thread>

namespace KuksaClient {

//==============================================================================
// Type Definitions
//==============================================================================

/**
 * @brief Field type identifier for value operations
 */
enum FieldType {
  FT_VALUE = 1,           ///< Current value field
  FT_ACTUATOR_TARGET = 2  ///< Actuator target value field
};

/**
 * @brief Callback signature for subscription updates
 *
 * @param entryPath The VSS path that was updated (e.g., "Vehicle.Speed")
 * @param value The new value as a string
 * @param field The field type (FT_VALUE or FT_ACTUATOR_TARGET)
 *
 * @note This callback is invoked from a subscription thread.
 *       Keep processing fast to avoid blocking other updates.
 *       Do not call blocking operations or long computations here.
 */
using SubscribeCallback = std::function<void(
    const std::string &entryPath,
    const std::string &value,
    const int &field
)>;

//==============================================================================
// Configuration
//==============================================================================

/**
 * @brief Configuration structure for KuksaClient
 */
struct Config {
  std::string serverURI;                 ///< Broker address (e.g., "127.0.0.1:55555")
  bool debug = false;                    ///< Enable debug logging
  std::vector<std::string> signalPaths;  ///< VSS paths for bulk operations
};

//==============================================================================
// Main Client Class
//==============================================================================

/**
 * @brief Thread-safe gRPC client for KUKSA Databroker
 *
 * This class provides a high-level interface to KUKSA Databroker with
 * automatic connection management, type-safe operations, and thread-safe
 * subscriptions with automatic reconnection.
 *
 * Design Patterns:
 * - pImpl: All gRPC details hidden in private implementation
 * - RAII: Automatic resource cleanup on destruction
 * - Thread-safe: All public methods can be called from multiple threads
 *
 * Lifecycle:
 * 1. Construct with Config or config file
 * 2. Call connect() to establish connection
 * 3. Use get/set/subscribe methods
 * 4. Destructor automatically cleans up all resources
 *
 * @note Non-copyable and non-movable (manages threads internally)
 */
class KuksaClient {
public:
  //============================================================================
  // Construction & Destruction
  //============================================================================

  /**
   * @brief Construct client from configuration structure
   *
   * @param config Client configuration
   * @throws std::runtime_error if initialization fails
   *
   * @note Does not connect automatically - call connect() explicitly
   */
  explicit KuksaClient(const Config &config);

  /**
   * @brief Construct client from JSON configuration file
   *
   * @param configFile Path to JSON configuration file
   * @throws std::runtime_error if file cannot be read or parsed
   *
   * Expected JSON format:
   * @code{.json}
   * {
   *   "broker": {
   *     "serverURI": "127.0.0.1:55555"
   *   },
   *   "debug": false,
   *   "signal": [
   *     { "path": "Vehicle.Speed" },
   *     { "path": "Vehicle.Gear" }
   *   ]
   * }
   * @endcode
   */
  explicit KuksaClient(const std::string &configFile);

  /**
   * @brief Destructor - automatically cleans up all resources
   *
   * Performs the following cleanup sequence:
   * 1. Signals all threads to stop
   * 2. Cancels all active gRPC operations
   * 3. Joins all subscription threads (with cancellation, no timeout needed)
   * 4. Cleans up connection resources
   *
   * @note Safe to call even with active subscriptions
   * @note Blocks until all threads have exited (but they exit quickly due to cancellation)
   */
  ~KuksaClient();

  // Non-copyable and non-movable
  KuksaClient(const KuksaClient&) = delete;
  KuksaClient& operator=(const KuksaClient&) = delete;
  KuksaClient(KuksaClient&&) = delete;
  KuksaClient& operator=(KuksaClient&&) = delete;

  //============================================================================
  // Connection Management
  //============================================================================

  /**
   * @brief Establish connection to the KUKSA Databroker
   *
   * @throws std::runtime_error if connection fails after all retries
   *
   * @note Includes built-in retry logic with exponential backoff
   * @note If auto-reconnect is enabled, connection will be restored automatically
   */
  void connect();

  /**
   * @brief Check current connection status
   *
   * @return true if connected, false otherwise
   *
   * @note Thread-safe
   */
  bool isConnected() const;

  /**
   * @brief Enable or disable automatic reconnection
   *
   * @param enabled true to enable, false to disable
   *
   * When enabled, the client will automatically attempt to reconnect
   * if the connection is lost. Subscriptions are automatically restarted
   * after successful reconnection.
   *
   * @note Auto-reconnect is enabled by default
   * @note Thread-safe
   */
  void setAutoReconnect(bool enabled);

  //============================================================================
  // Get Operations
  //============================================================================

  /**
   * @brief Get current value with automatic type conversion
   *
   * @tparam T Target type (int, float, double, bool, uint8_t, std::string, etc.)
   * @param entryPath VSS path (e.g., "Vehicle.Speed")
   * @param out Output variable to store the converted value
   * @return true if value was retrieved and converted successfully
   *
   * @note Thread-safe
   * @note Returns false if not connected or conversion fails
   *
   * Example:
   * @code
   *   float speed;
   *   if (client.getCurrentValue("Vehicle.Speed", speed)) {
   *       std::cout << "Speed: " << speed << std::endl;
   *   }
   * @endcode
   */
  template <typename T>
  bool getCurrentValue(const std::string &entryPath, T &out);

  /**
   * @brief Get target (actuator) value with automatic type conversion
   *
   * @tparam T Target type (int, float, double, bool, uint8_t, std::string, etc.)
   * @param entryPath VSS path
   * @param out Output variable to store the converted value
   * @return true if value was retrieved and converted successfully
   *
   * @note Thread-safe
   * @note Returns false if not connected or conversion fails
   */
  template <typename T>
  bool getTargetValue(const std::string &entryPath, T &out);

  //============================================================================
  // Set Operations
  //============================================================================

  /**
   * @brief Set current value with automatic type conversion
   *
   * @tparam T Value type (int, float, double, bool, uint8_t, std::string, etc.)
   * @param entryPath VSS path
   * @param newValue Value to set
   *
   * @note Thread-safe
   * @note Silent failure if not connected
   *
   * Example:
   * @code
   *   client.setCurrentValue("Vehicle.Gear", 3);
   *   client.setCurrentValue("Vehicle.IsParked", true);
   * @endcode
   */
  template <typename T>
  void setCurrentValue(const std::string &entryPath, const T &newValue);

  /**
   * @brief Set target (actuator) value with automatic type conversion
   *
   * @tparam T Value type (int, float, double, bool, uint8_t, std::string, etc.)
   * @param entryPath VSS path
   * @param newValue Value to set
   *
   * @note Thread-safe
   * @note Silent failure if not connected
   */
  template <typename T>
  void setTargetValue(const std::string &entryPath, const T &newValue);

  //============================================================================
  // Subscription Operations
  //============================================================================

  /**
   * @brief Subscribe to current value updates
   *
   * @param entryPath VSS path to subscribe to
   * @param callback Function to call on each update
   *
   * Features:
   * - Automatic reconnection: subscription survives connection loss
   * - Thread-per-subscription: each runs in its own thread
   * - Duplicate prevention: ignores duplicate subscription requests
   * - Automatic cleanup: threads cleaned up on destruction
   *
   * @note Thread-safe
   * @note Callback is invoked from subscription thread - keep it fast!
   * @note Subscription persists until object destruction
   *
   * Example:
   * @code
   *   client.subscribeCurrentValue("Vehicle.Speed",
   *       [](const std::string& path, const std::string& value, int field) {
   *           std::cout << path << " changed to " << value << std::endl;
   *       }
   *   );
   * @endcode
   */
  void subscribeCurrentValue(const std::string &entryPath, SubscribeCallback callback);

  /**
   * @brief Subscribe to target (actuator) value updates
   *
   * @param entryPath VSS path to subscribe to
   * @param callback Function to call on each update
   *
   * @note Thread-safe
   * @note See subscribeCurrentValue() for details
   */
  void subscribeTargetValue(const std::string &entryPath, SubscribeCallback callback);

  /**
   * @brief Generic subscribe with field type selector
   *
   * @param entryPath VSS path to subscribe to
   * @param field Field type (FT_VALUE or FT_ACTUATOR_TARGET)
   * @param callback Function to call on each update
   *
   * @note Thread-safe
   * @note Prefer using subscribeCurrentValue() or subscribeTargetValue() instead
   */
  void subscribe(const std::string &entryPath, FieldType field, SubscribeCallback callback);

  //============================================================================
  // Utility Methods
  //============================================================================

  /**
   * @brief Parse JSON configuration file
   *
   * @param filename Path to configuration file
   * @param config Output configuration structure
   * @return true if parsing succeeded
   *
   * @note Static method - can be called without an instance
   */
  static bool parseConfig(const std::string &filename, Config &config);

private:
  //============================================================================
  // Private Implementation
  //============================================================================

  // Forward declarations
  struct Impl;
  struct SubscriptionContext;

  // Private helper methods
  std::string getValueInternal(const std::string &entryPath, FieldType field);

  template <typename T>
  void setValueInternalImpl(const std::string &entryPath, const T &newValue, FieldType field);

  template <typename T>
  static bool convertString(const std::string &str, T &out);

  static bool convertString(const std::string &str, bool &out);
  static bool convertString(const std::string &str, uint8_t &out);
  static bool convertString(const std::string &str, uint16_t &out);
  static bool convertString(const std::string &str, uint32_t &out);

  // Reconnection helpers
  bool attemptReconnection();
  void handleConnectionFailure();
  void restartSubscriptions();

  // Subscription thread management
  void subscriptionThreadFunc(std::shared_ptr<SubscriptionContext> ctx);
  void cleanupSubscriptionThread(const std::string &subscriptionKey);

  //============================================================================
  // Private Members
  //============================================================================

  // pImpl - hides all gRPC details from header
  std::unique_ptr<Impl> pImpl_;

  // Configuration
  Config config_;
  std::string serverURI_;
  bool debug_{false};

  // Connection state
  mutable std::atomic<bool> connected_{false};
  std::atomic<bool> autoReconnect_{true};
  std::atomic<bool> shouldStop_{false};

  // Synchronization primitives
  mutable std::mutex connectionMutex_;
  std::mutex subscriptionsMutex_;
  std::mutex reconnectMutex_;
  std::condition_variable reconnectCV_;

  // Thread management
  std::thread reconnectThread_;
  std::vector<std::shared_ptr<SubscriptionContext>> subscriptionContexts_;

  // Subscription tracking
  struct SubscriptionInfo {
    std::string entryPath;
    SubscribeCallback callback;
    FieldType field;
  };
  std::vector<SubscriptionInfo> activeSubscriptions_;
  std::set<std::string> activeSubscriptionKeys_;
};

//==============================================================================
// Template Implementations
//==============================================================================

template <typename T>
bool KuksaClient::getCurrentValue(const std::string &entryPath, T &out) {
  std::string strVal = getValueInternal(entryPath, FT_VALUE);
  if (strVal.empty()) return false;
  return convertString(strVal, out);
}

template <typename T>
bool KuksaClient::getTargetValue(const std::string &entryPath, T &out) {
  std::string strVal = getValueInternal(entryPath, FT_ACTUATOR_TARGET);
  if (strVal.empty()) return false;
  return convertString(strVal, out);
}

template <typename T>
void KuksaClient::setCurrentValue(const std::string &entryPath, const T &newValue) {
  setValueInternalImpl(entryPath, newValue, FT_VALUE);
}

template <typename T>
void KuksaClient::setTargetValue(const std::string &entryPath, const T &newValue) {
  setValueInternalImpl(entryPath, newValue, FT_ACTUATOR_TARGET);
}

template <typename T>
bool KuksaClient::convertString(const std::string &str, T &out) {
  std::istringstream iss(str);
  iss >> out;
  return !iss.fail() && iss.eof();
}

} // namespace KuksaClient

#endif // KUKSA_CLIENT_HPP
