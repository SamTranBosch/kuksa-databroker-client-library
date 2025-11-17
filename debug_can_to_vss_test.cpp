/**
 * Comprehensive CAN → VSS → KUKSA Debug Test
 *
 * Tests all VSS signals from vss_can_mapping.json to identify where
 * the data flow breaks (CAN reception, VSS update queue, or KUKSA delivery).
 *
 * For each signal:
 * 1. Write test value to VSS
 * 2. Query VSS at multiple delays (10ms, 50ms, 100ms, 200ms, 500ms, 1000ms)
 * 3. Track when value actually becomes visible in KUKSA
 * 4. Generate detailed timing report
 *
 * Usage:
 *   ./debug_can_to_vss_test [databroker_uri] [output_file]
 *
 * Example:
 *   ./debug_can_to_vss_test 127.0.0.1:55555 /tmp/debug_results.txt
 */

#include "debug_test.hpp"
#include "KuksaClient.hpp"
#include <iostream>
#include <vector>
#include <thread>

using namespace std::chrono_literals;

int main(int argc, char* argv[]) {
    std::cout << COLOR_BOLD << "\n╔════════════════════════════════════════════════════════════╗\n";
    std::cout << "║  CAN → VSS → KUKSA Debug Test Suite                       ║\n";
    std::cout << "║  (Comprehensive Signal Flow Verification)                 ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════╝\n" << COLOR_RESET << "\n";

    // Parse arguments
    std::string serverURI = (argc > 1) ? argv[1] : "127.0.0.1:55555";
    std::string reportFile = (argc > 2) ? argv[2] : "/tmp/debug_can_vss_results.txt";

    INFO_LOG("Test Configuration:");
    INFO_LOG("  Databroker URI: %s", serverURI.c_str());
    INFO_LOG("  Output Report: %s", reportFile.c_str());
    INFO_LOG("  Query Delays: 10ms, 50ms, 100ms, 200ms, 500ms, 1000ms");
    INFO_LOG("  Total Signals: 10 (4 CAN→VSS sensors + 6 VSS→CAN actuators)");

    // Initialize KUKSA client
    KuksaClient::Config clientConfig;
    clientConfig.serverURI = serverURI;
    clientConfig.debug = false;

    KuksaClient::KuksaClient client(clientConfig);

    INFO_LOG("\nConnecting to KUKSA Databroker at %s...", serverURI.c_str());

    try {
        client.connect();
        SUCCESS_LOG("✓ Connected successfully!\n");
    } catch (const std::exception& e) {
        ERROR_LOG("✗ Failed to connect to databroker!");
        std::cerr << "Error: " << e.what() << "\n";
        std::cerr << "\nMake sure KUKSA Databroker is running at " << serverURI << "\n";
        return 1;
    }

    // Create tester
    DebugVSSSignalTester tester(client);

    // ==================== SECTION 1: CAN → VSS (Sensor Signals) ====================
    std::cout << "\n" << COLOR_BOLD << COLOR_UNDERLINE
              << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
              << COLOR_RESET << "\n";
    std::cout << COLOR_BOLD << "Section 1: CAN → VSS (Sensor/Receiver Signals)\n"
              << COLOR_RESET;
    std::cout << COLOR_BOLD << COLOR_UNDERLINE
              << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
              << COLOR_RESET << "\n";

    INFO_LOG("These signals flow FROM CAN bus TO VSS (one-way sensor data)");
    INFO_LOG("Expected flow: CAN Frame → RXPipeline → SignalMapper → VSSManager.setCurrentValue()");
    INFO_LOG("              → Update Queue → VSSManager Worker → KUKSA gRPC\n");

    // Test 1: Vehicle.Speed
    tester.testSignalFlow<float>(
        "Vehicle.Speed",
        "Com__vWhlVehSpd",
        "Vehicle speed from wheel speed sensor (J1939 TxCCVS)",
        120.5f,
        {10, 50, 100, 200, 500, 1000}
    );

    // Test 2: Vehicle.Powertrain.CombustionEngine.Speed
    tester.testSignalFlow<uint16_t>(
        "Vehicle.Powertrain.CombustionEngine.Speed",
        "Com__nEngSpd",
        "Engine speed from ECU (J1939 EEC1)",
        2500,
        {10, 50, 100, 200, 500, 1000}
    );

    // Test 3: Vehicle.Powertrain.IsIgnitionOn
    tester.testSignalFlow<bool>(
        "Vehicle.Powertrain.IsIgnitionOn",
        "ComScl_stT15",
        "Ignition state reading (Terminal 15 - ECU2HMI Standard CAN)",
        true,
        {10, 50, 100, 200, 500, 1000}
    );

    // Test 4: Vehicle.ADAS.IDPS.EventStatus
    tester.testSignalFlow<uint8_t>(
        "Vehicle.ADAS.IDPS.EventStatus",
        "Com__stEventStatus",
        "ADAS event status from driver profile system (VCU_HMI Standard CAN)",
        static_cast<uint8_t>(3),
        {10, 50, 100, 200, 500, 1000}
    );

    // ==================== SECTION 2: VSS → CAN (Actuator Signals) ====================
    std::cout << "\n" << COLOR_BOLD << COLOR_UNDERLINE
              << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
              << COLOR_RESET << "\n";
    std::cout << COLOR_BOLD << "Section 2: VSS → CAN (Actuator/Command Signals)\n"
              << COLOR_RESET;
    std::cout << COLOR_BOLD << COLOR_UNDERLINE
              << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
              << COLOR_RESET << "\n";

    INFO_LOG("These signals flow FROM VSS TO CAN bus (one-way actuator commands)");
    INFO_LOG("Expected flow: VSS update → VSSManager.subscribeTarget() → TXPipeline");
    INFO_LOG("              → SignalMapper → CANManager → CAN Bus");
    INFO_LOG("Note: Verification requires CAN bus monitoring (candump)\n");

    // Test 5: Vehicle.Body.IgnitionState
    tester.testActuatorFlow<uint8_t>(
        "Vehicle.Body.IgnitionState",
        "Com__stIgnSCAC2Byte1",
        "Ignition state command to ECU (write)",
        1
    );

    // Test 6: Vehicle.Chassis.Sportiness.Mode
    tester.testActuatorFlow<uint8_t>(
        "Vehicle.Chassis.Sportiness.Mode",
        "ComScl_stReqMod",
        "Drive mode request (Eco/Comfort/Sport/Sport+)",
        2
    );

    // Test 7: Vehicle.Chassis.Sportiness.Target
    tester.testActuatorFlow<float>(
        "Vehicle.Chassis.Sportiness.Target",
        "ComScl_rTqLvUsrProfDmd",
        "Torque level user profile demand (-125% to 125%)",
        50.0f
    );

    // Test 8: Vehicle.Chassis.Brake.PedalPosition
    tester.testActuatorFlow<float>(
        "Vehicle.Chassis.Brake.PedalPosition",
        "Com__stBrkSwt",
        "Brake pedal command (0=released, 1=pressed)",
        0.5f
    );

    // Test 9: Vehicle.Chassis.Accelerator.PedalPosition
    tester.testActuatorFlow<float>(
        "Vehicle.Chassis.Accelerator.PedalPosition",
        "Com__rAPPCAN",
        "Accelerator pedal position command (0-100%)",
        75.0f
    );

    // Test 10: Additional test for thorough coverage
    tester.testSignalFlow<float>(
        "Vehicle.Speed",
        "Com__vWhlVehSpd",
        "Vehicle speed - repeat test (should be faster on second attempt)",
        85.5f,
        {10, 50, 100, 200, 500}
    );

    // ==================== SUMMARY AND REPORT ====================
    tester.printSummary();
    tester.saveReport(reportFile);

    std::cout << "\n" << COLOR_BOLD << COLOR_GREEN
              << "✓ Debug test complete!\n"
              << COLOR_RESET;

    std::cout << "\n" << COLOR_BOLD << "Next Steps:" << COLOR_RESET << "\n";
    std::cout << "1. Review the detailed report:\n";
    std::cout << "   " << COLOR_CYAN << "cat " << reportFile << COLOR_RESET << "\n\n";

    std::cout << "2. Monitor KUKSA in real-time to see updates:\n";
    std::cout << "   " << COLOR_CYAN << "echo 'getValue Vehicle.Speed' | kuksa-client grpc://"
              << serverURI << COLOR_RESET << "\n\n";

    std::cout << "3. Monitor CAN traffic (if hardware available):\n";
    std::cout << "   " << COLOR_CYAN << "candump vcan0 -t A -x" << COLOR_RESET << "\n\n";

    std::cout << "4. Check application logs:\n";
    std::cout << "   " << COLOR_CYAN << "docker logs -f kuksa-can-provider-cpp"
              << COLOR_RESET << "\n\n";

    std::cout << COLOR_BOLD << "Key Observations:" << COLOR_RESET << "\n";
    std::cout << "• " << COLOR_YELLOW << "If latency > 500ms:" << COLOR_RESET
              << " Check queue size and batch processing\n";
    std::cout << "• " << COLOR_YELLOW << "If signal unavailable:" << COLOR_RESET
              << " Check VSS path and KUKSA connection\n";
    std::cout << "• " << COLOR_YELLOW << "If inconsistent delays:" << COLOR_RESET
              << " Check thread scheduling and KUKSA load\n\n";

    return 0;
}
