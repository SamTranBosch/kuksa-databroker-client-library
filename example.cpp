/**
 * @file example.cpp
 * @brief Comprehensive example demonstrating KuksaClient usage
 *
 * This example shows:
 * - Configuration and connection
 * - Get/Set operations with type conversion
 * - Subscription with callbacks
 * - Error handling
 * - Clean shutdown via RAII
 *
 * Build:
 *   g++ -std=c++17 example.cpp -lKuksaClient -lgrpc++ -lprotobuf -o example
 *
 * Run:
 *   ./example
 */

#include "KuksaClient.hpp"
#include <iostream>
#include <csignal>
#include <atomic>

// Global flag for graceful shutdown
std::atomic<bool> running{true};

void signalHandler(int signum) {
  std::cout << "\nInterrupt signal (" << signum << ") received. Shutting down..." << std::endl;
  running = false;
}

int main(int argc, char** argv) {
  // Register signal handler for Ctrl+C
  signal(SIGINT, signalHandler);

  std::cout << "========================================" << std::endl;
  std::cout << " KUKSA Databroker Client Example" << std::endl;
  std::cout << "========================================" << std::endl;

  try {
    // ========================================================================
    // Configuration
    // ========================================================================

    std::cout << "\n[1/5] Configuring client..." << std::endl;

    KuksaClient::Config config;

    // Option 1: Hardcoded configuration
    config.serverURI = "127.0.0.1:55555";
    config.debug = true;  // Enable debug logging
    config.signalPaths = {
      "Vehicle.Speed",
      "Vehicle.Gear",
      "Vehicle.CurrentLocation.Latitude",
      "Vehicle.CurrentLocation.Longitude"
    };

    // Option 2: Load from file (uncomment to use)
    // KuksaClient::parseConfig("config.json", config);

    std::cout << "  Server: " << config.serverURI << std::endl;
    std::cout << "  Debug: " << (config.debug ? "enabled" : "disabled") << std::endl;
    std::cout << "  Signals: " << config.signalPaths.size() << std::endl;

    // ========================================================================
    // Connection
    // ========================================================================

    std::cout << "\n[2/5] Connecting to KUKSA Databroker..." << std::endl;

    KuksaClient::KuksaClient client(config);

    try {
      client.connect();
      std::cout << "  ✓ Connected successfully!" << std::endl;
    } catch (const std::runtime_error& e) {
      std::cerr << "  ✗ Connection failed: " << e.what() << std::endl;
      std::cerr << "  Note: Auto-reconnect is enabled, will retry in background" << std::endl;
    }

    // ========================================================================
    // Get Operations
    // ========================================================================

    std::cout << "\n[3/5] Reading values..." << std::endl;

    if (client.isConnected()) {
      // Get with type conversion
      float speed = 0.0f;
      if (client.getCurrentValue("Vehicle.Speed", speed)) {
        std::cout << "  Vehicle.Speed = " << speed << " km/h" << std::endl;
      } else {
        std::cout << "  Vehicle.Speed: (not available)" << std::endl;
      }

      int gear = 0;
      if (client.getCurrentValue("Vehicle.Gear", gear)) {
        std::cout << "  Vehicle.Gear = " << gear << std::endl;
      } else {
        std::cout << "  Vehicle.Gear: (not available)" << std::endl;
      }

      double latitude = 0.0;
      if (client.getCurrentValue("Vehicle.CurrentLocation.Latitude", latitude)) {
        std::cout << "  Latitude = " << latitude << std::endl;
      }

      double longitude = 0.0;
      if (client.getCurrentValue("Vehicle.CurrentLocation.Longitude", longitude)) {
        std::cout << "  Longitude = " << longitude << std::endl;
      }
    } else {
      std::cout << "  (Skipped - not connected yet)" << std::endl;
    }

    // ========================================================================
    // Set Operations
    // ========================================================================

    std::cout << "\n[4/5] Setting values..." << std::endl;

    if (client.isConnected()) {
      client.setCurrentValue("Vehicle.Speed", 65.5f);
      std::cout << "  ✓ Set Vehicle.Speed = 65.5" << std::endl;

      client.setCurrentValue("Vehicle.Gear", 4);
      std::cout << "  ✓ Set Vehicle.Gear = 4" << std::endl;

      client.setCurrentValue("Vehicle.IsParked", false);
      std::cout << "  ✓ Set Vehicle.IsParked = false" << std::endl;
    } else {
      std::cout << "  (Skipped - not connected yet)" << std::endl;
    }

    // ========================================================================
    // Subscriptions
    // ========================================================================

    std::cout << "\n[5/5] Setting up subscriptions..." << std::endl;

    // Subscription counter
    std::atomic<int> updateCount{0};

    // Subscribe to speed updates
    client.subscribeCurrentValue("Vehicle.Speed",
      [&updateCount](const std::string& path, const std::string& value, int field) {
        updateCount++;
        std::cout << "  [Update #" << updateCount << "] "
                  << path << " = " << value
                  << " (field: " << (field == KuksaClient::FT_VALUE ? "VALUE" : "TARGET")
                  << ")" << std::endl;
      }
    );

    // Subscribe to gear updates
    client.subscribeCurrentValue("Vehicle.Gear",
      [](const std::string& path, const std::string& value, int field) {
        std::cout << "  [Gear] " << path << " changed to " << value << std::endl;
      }
    );

    // Subscribe to location updates
    client.subscribeCurrentValue("Vehicle.CurrentLocation.Latitude",
      [](const std::string& path, const std::string& value, int field) {
        std::cout << "  [Location] Latitude: " << value << std::endl;
      }
    );

    client.subscribeCurrentValue("Vehicle.CurrentLocation.Longitude",
      [](const std::string& path, const std::string& value, int field) {
        std::cout << "  [Location] Longitude: " << value << std::endl;
      }
    );

    std::cout << "  ✓ Subscriptions active!" << std::endl;

    // ========================================================================
    // Main Loop
    // ========================================================================

    std::cout << "\n========================================" << std::endl;
    std::cout << " Receiving updates..." << std::endl;
    std::cout << " Press Ctrl+C to exit" << std::endl;
    std::cout << "========================================\n" << std::endl;

    // Connection monitoring
    int reconnectCount = 0;
    bool wasConnected = client.isConnected();

    while (running) {
      std::this_thread::sleep_for(std::chrono::seconds(1));

      bool isConnected = client.isConnected();

      // Detect reconnection
      if (isConnected && !wasConnected) {
        reconnectCount++;
        std::cout << "\n[Reconnected!] (count: " << reconnectCount << ")\n" << std::endl;
      } else if (!isConnected && wasConnected) {
        std::cout << "\n[Connection lost] Auto-reconnect in progress...\n" << std::endl;
      }

      wasConnected = isConnected;
    }

    std::cout << "\n========================================" << std::endl;
    std::cout << " Shutting down..." << std::endl;
    std::cout << "========================================" << std::endl;

    std::cout << "\nStatistics:" << std::endl;
    std::cout << "  Total updates received: " << updateCount << std::endl;
    std::cout << "  Reconnections: " << reconnectCount << std::endl;

  } catch (const std::exception& e) {
    std::cerr << "Fatal error: " << e.what() << std::endl;
    return 1;
  }

  std::cout << "\nCleanup in progress..." << std::endl;
  // Destructor automatically:
  // - Cancels all gRPC operations
  // - Joins all subscription threads
  // - Cleans up all resources
  // No manual cleanup needed!

  std::cout << "✓ Cleanup complete. Goodbye!" << std::endl;

  return 0;
}
