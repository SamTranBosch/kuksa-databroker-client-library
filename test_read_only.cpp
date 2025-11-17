/**
 * Simple Read-Only Test
 * This program ONLY reads values from databroker
 * Use this to verify values written by main.cpp
 */

#include "KuksaClient.hpp"
#include <iostream>
#include <iomanip>

int main(int argc, char* argv[]) {
    std::string serverURI = (argc > 1) ? argv[1] : "127.0.0.1:55555";

    std::cout << "\n╔══════════════════════════════════════════════════╗\n";
    std::cout << "║  READ-ONLY TEST - Verify Values from Terminal   ║\n";
    std::cout << "╚══════════════════════════════════════════════════╝\n\n";

    KuksaClient::Config config;
    config.serverURI = serverURI;
    config.debug = false;

    KuksaClient::KuksaClient client(config);

    try {
        client.connect();
        std::cout << "✓ Connected to databroker at " << serverURI << "\n\n";
    } catch (const std::exception& e) {
        std::cerr << "✗ Failed to connect: " << e.what() << "\n";
        return 1;
    }

    // Test signals that main.cpp writes
    struct TestSignal {
        std::string path;
        std::string type;
    };

    TestSignal signals[] = {
        {"Vehicle.Speed", "float"},
        {"Vehicle.Powertrain.FuelSystem.Range", "uint32"},
        {"Vehicle.Powertrain.CombustionEngine.Speed", "uint16"},
        {"Vehicle.Powertrain.CombustionEngine.Power", "uint16"},
        {"Vehicle.Powertrain.IsIgnitionOn", "bool"},
        {"Vehicle.Body.DriveMode", "string"},
        {"Vehicle.Chassis.Sportiness.Target", "float"},
    };

    std::cout << std::left << std::setw(50) << "SIGNAL" << " VALUE\n";
    std::cout << std::string(70, '=') << "\n";

    for (const auto& signal : signals) {
        std::cout << std::setw(50) << signal.path << " ";

        if (signal.type == "float") {
            float value;
            if (client.getCurrentValue(signal.path, value)) {
                std::cout << value << "\n";
            } else {
                std::cout << "FAILED TO READ\n";
            }
        } else if (signal.type == "uint32") {
            uint32_t value;
            if (client.getCurrentValue(signal.path, value)) {
                std::cout << value << "\n";
            } else {
                std::cout << "FAILED TO READ\n";
            }
        } else if (signal.type == "uint16") {
            uint16_t value;
            if (client.getCurrentValue(signal.path, value)) {
                std::cout << value << "\n";
            } else {
                std::cout << "FAILED TO READ\n";
            }
        } else if (signal.type == "bool") {
            bool value;
            if (client.getCurrentValue(signal.path, value)) {
                std::cout << (value ? "true" : "false") << "\n";
            } else {
                std::cout << "FAILED TO READ\n";
            }
        } else if (signal.type == "string") {
            std::string value;
            if (client.getTargetValue(signal.path, value)) {
                std::cout << "\"" << value << "\"\n";
            } else {
                std::cout << "FAILED TO READ\n";
            }
        }
    }

    std::cout << std::string(70, '=') << "\n";
    std::cout << "\n✓ Read test complete\n\n";

    return 0;
}
