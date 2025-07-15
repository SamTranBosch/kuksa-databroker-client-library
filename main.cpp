#include "KuksaClient.hpp"

#include <iostream>
#include <chrono>
#include <thread>

using namespace KuksaClient;

//------------------------------------------------------------------------------
// Example main() usage
//------------------------------------------------------------------------------
int main(int argc, char *argv[]) {
  try {
    // Either pass a config file or set up Config manually.
    KuksaClient::Config config;
    if (!KuksaClient::KuksaClient::parseConfig("/usr/local/bin/config.json", config)) {
      std::cerr << "Failed to load config from file. Using default configuration." << std::endl;
      config.serverURI   = "localhost:50051";
      config.debug       = true;
      config.signalPaths = {"Vehicle.Body.Lights.Beam.Low.IsOn", "Vehicle.Body.Lights.Hazard.IsSignaling"};
    }
    // Create the client and connect.
    std::cout << "Starting Kuksa Databroker Client (v1)..." << std::endl;
    KuksaClient::KuksaClient client(config);
    client.connect();

    // Get server info.
    client.getServerInfo();

    // Get converted values.
    bool currentBool;
    uint8_t targetUint8;
    
    // Example: set new values.
    client.setCurrentValue("Vehicle.Speed", 5.14f);

    // Example: set values.
    client.setCurrentValue("Vehicle.Body.Lights.Beam.Low.IsOn", true);
    client.setTargetValue("Vehicle.Body.Lights.Beam.Low.IsOn", false);
    
    // Example: get values.
    if (client.getCurrentValueAs<bool>("Vehicle.Body.Lights.Beam.Low.IsOn", currentBool))
      std::cout << "getCurrentValue(): " << currentBool << std::endl;
    else
      std::cerr << "Failed to convert current value to bool." << std::endl;
    if (client.getTargetValueAs<bool>("Vehicle.Body.Lights.Beam.Low.IsOn", currentBool))
      std::cout << "getCurrentValue(): " << currentBool << std::endl;
    else
      std::cerr << "Failed to convert current value to bool." << std::endl;

    // Example: get get values.
    if (client.getTargetValueAs<uint8_t>("Vehicle.Cabin.HVAC.Station.Row1.Driver.FanSpeed", targetUint8))
      std::cout << "Converted target uint8: " << static_cast<unsigned>(targetUint8) << std::endl;
    else
      std::cerr << "Failed to convert target value to uint8." << std::endl;


    // Start subscriptions for all configured signal paths.
    client.subscribeAll([](const std::string &path, const std::string &value, const int &field) {
      if (field == FT_ACTUATOR_TARGET) {
        // updateValue = getTargetValue(entryPath);
        std::cout << "[User Callback]: Update TargetValue received for [" << path << "] -> " << value << std::endl;
      } else { // FT_VALUE
        // updateValue = getCurrentValue(entryPath);
        std::cout << "[User Callback]: Update CurrentValue received for [" << path << "] -> " << value << std::endl;
      }
    });
    // Wait for subscriptions.
    client.joinAllSubscriptions();

    // Let subscriptions run for 30 seconds (adjust as needed).
    std::this_thread::sleep_for(std::chrono::seconds(30));

    std::cout << "Exiting client." << std::endl;

  } catch (const std::exception &ex) {
    std::cerr << "Exception: " << ex.what() << std::endl;
    return 1;
  }
  return 0;
}
