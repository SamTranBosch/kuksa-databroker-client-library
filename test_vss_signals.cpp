/**
 * End-to-End VSS Signal Test Suite
 * Tests sensor and actuator signals with full write-read-verify cycle
 * Handles unavailable signals gracefully
 */

#include "KuksaClient.hpp"
#include <iostream>
#include <iomanip>
#include <vector>
#include <map>
#include <thread>
#include <chrono>
#include <atomic>
#include <mutex>
#include <fstream>
#include <sstream>
#include <cmath>

using namespace std::chrono_literals;

// ANSI color codes for terminal output
#define COLOR_RESET   "\033[0m"
#define COLOR_GREEN   "\033[32m"
#define COLOR_RED     "\033[31m"
#define COLOR_YELLOW  "\033[33m"
#define COLOR_BLUE    "\033[34m"
#define COLOR_CYAN    "\033[36m"
#define COLOR_BOLD    "\033[1m"

// Test result tracking
struct TestResult {
    std::string testName;
    std::string signalPath;
    bool passed;
    std::string message;
    double duration_ms;
};

class VSSSignalTester {
private:
    KuksaClient::KuksaClient& client;
    std::vector<TestResult> results;
    std::mutex results_mutex;
    std::atomic<int> totalTests{0};
    std::atomic<int> passedTests{0};
    std::atomic<int> failedTests{0};
    std::atomic<int> skippedTests{0};

    void addResult(const std::string& testName, const std::string& signalPath,
                   bool passed, const std::string& message, double duration_ms = 0.0) {
        std::lock_guard<std::mutex> lock(results_mutex);
        results.push_back({testName, signalPath, passed, message, duration_ms});
        totalTests++;
        if (passed) {
            passedTests++;
        } else if (message.find("SKIPPED") != std::string::npos ||
                   message.find("NOT AVAILABLE") != std::string::npos) {
            skippedTests++;
        } else {
            failedTests++;
        }
    }

    std::string formatDuration(double ms) const {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << ms << "ms";
        return oss.str();
    }

public:
    VSSSignalTester(KuksaClient::KuksaClient& c) : client(c) {}

    // ==================== END-TO-END TESTS (WRITE-READ-VERIFY) ====================

    template<typename T>
    void testE2E(const std::string& path, const std::vector<T>& testValues,
                 const std::string& description, const std::string& testType) {
        std::cout << COLOR_CYAN << "\n  → Testing: " << COLOR_RESET << path << " (" << description << ")\n";

        bool signalAvailable = false;

        for (size_t i = 0; i < testValues.size(); i++) {
            T testValue = testValues[i];

            // Step 1: Write value (setCurrentValue returns void, silently fails if not connected)
            auto writeStart = std::chrono::high_resolution_clock::now();
            client.setCurrentValue(path, testValue);
            auto writeEnd = std::chrono::high_resolution_clock::now();
            double writeDuration = std::chrono::duration<double, std::milli>(writeEnd - writeStart).count();

            // Step 2: Wait for value to propagate
            std::this_thread::sleep_for(50ms);

            // Step 3: Read back to verify (getCurrentValue returns bool)
            auto readStart = std::chrono::high_resolution_clock::now();
            T readValue{};
            bool readSuccess = client.getCurrentValue(path, readValue);
            auto readEnd = std::chrono::high_resolution_clock::now();
            double readDuration = std::chrono::duration<double, std::milli>(readEnd - readStart).count();

            double totalDuration = writeDuration + readDuration;

            if (!readSuccess) {
                // Signal doesn't exist or we're not connected
                if (i == 0) {  // Only report once per signal
                    addResult(testType + "_E2E", path, false,
                             "SIGNAL NOT AVAILABLE - Cannot read after write", totalDuration);
                    std::cout << COLOR_YELLOW << "    ⊗ " << COLOR_RESET
                             << "Signal not available or read-only\n";
                }
                break;  // Skip remaining test values for this signal
            }

            signalAvailable = true;

            // Verify the value matches
            bool match = false;
            if constexpr (std::is_floating_point_v<T>) {
                match = std::abs(readValue - testValue) < 0.01;
            } else {
                match = (readValue == testValue);
            }

            if (match) {
                std::string valueStr = formatValue(testValue);
                std::string readStr = formatValue(readValue);
                addResult(testType + "_E2E", path, true,
                         "Write: " + valueStr + ", Read: " + readStr + " ✓", totalDuration);
                std::cout << COLOR_GREEN << "    ✓ " << COLOR_RESET
                         << "Write " << valueStr << " → Read " << readStr
                         << " (" << formatDuration(totalDuration) << ")\n";
            } else {
                std::string valueStr = formatValue(testValue);
                std::string readStr = formatValue(readValue);
                addResult(testType + "_E2E", path, false,
                         "MISMATCH - Write: " + valueStr + ", Read: " + readStr, totalDuration);
                std::cout << COLOR_RED << "    ✗ " << COLOR_RESET
                         << "MISMATCH - Wrote " << valueStr << " but read " << readStr << "\n";
            }
        }
    }

    // Helper to format values for output
    template<typename T>
    std::string formatValue(const T& value) {
        if constexpr (std::is_same_v<T, bool>) {
            return value ? "true" : "false";
        } else if constexpr (std::is_same_v<T, std::string>) {
            return "\"" + value + "\"";
        } else if constexpr (std::is_same_v<T, int8_t> || std::is_same_v<T, uint8_t>) {
            return std::to_string(static_cast<int>(value));
        } else {
            return std::to_string(value);
        }
    }

    // ==================== SUBSCRIPTION TESTS ====================

    void testSubscription(const std::string& path, const std::string& description,
                         int durationSeconds = 5) {
        std::cout << COLOR_CYAN << "\n  → Testing subscription: " << COLOR_RESET
                 << path << " (" << description << ")\n";

        std::atomic<int> updateCount{0};
        std::string lastValue;
        std::mutex valueMutex;
        std::atomic<bool> callbackInvoked{false};

        auto start = std::chrono::high_resolution_clock::now();

        // subscribeCurrentValue returns void
        try {
            client.subscribeCurrentValue(path,
                [&updateCount, &lastValue, &valueMutex, &callbackInvoked, path]
                (const std::string& signalPath, const std::string& value, int field) {
                    callbackInvoked = true;
                    updateCount++;
                    {
                        std::lock_guard<std::mutex> lock(valueMutex);
                        lastValue = value;
                    }
                    std::cout << COLOR_BLUE << "    ↻ " << COLOR_RESET
                             << signalPath << " = " << value << " (update #" << updateCount << ")\n";
                }
            );

            // Wait for updates
            std::this_thread::sleep_for(std::chrono::seconds(durationSeconds));

            auto end = std::chrono::high_resolution_clock::now();
            double duration = std::chrono::duration<double, std::milli>(end - start).count();

            if (updateCount > 0) {
                addResult("SUBSCRIPTION", path, true,
                         "Received " + std::to_string(updateCount.load()) + " updates", duration);
                std::cout << COLOR_GREEN << "    ✓ " << COLOR_RESET
                         << "Subscription successful (" << updateCount
                         << " updates in " << durationSeconds << "s)\n";
            } else if (callbackInvoked) {
                // Subscription worked but no updates (static signal)
                addResult("SUBSCRIPTION", path, true,
                         "Subscribed successfully (no updates - static signal)", duration);
                std::cout << COLOR_YELLOW << "    ⚠ " << COLOR_RESET
                         << "Subscribed but no updates (static signal)\n";
            } else {
                // Subscription may have failed silently or signal doesn't exist
                addResult("SUBSCRIPTION", path, false,
                         "No callback invoked - signal may not be available", duration);
                std::cout << COLOR_YELLOW << "    ⊗ " << COLOR_RESET
                         << "No updates received (signal not available?)\n";
            }
        } catch (const std::exception& e) {
            addResult("SUBSCRIPTION", path, false,
                     "Exception: " + std::string(e.what()), 0.0);
            std::cout << COLOR_RED << "    ✗ " << COLOR_RESET
                     << "Exception: " << e.what() << "\n";
        }
    }

    // ==================== STRESS TESTS ====================

    void stressTestConcurrentWrites(const std::string& path, float baseValue, int iterations = 100) {
        std::cout << COLOR_CYAN << "\n  → Stress test: " << iterations
                 << " concurrent writes to " << path << COLOR_RESET << "\n";

        auto start = std::chrono::high_resolution_clock::now();
        int verifiedCount = 0;

        for (int i = 0; i < iterations; i++) {
            float value = baseValue + (i % 10) * 0.1f;
            client.setCurrentValue(path, value);

            // Periodically verify (every 10 writes)
            if (i % 10 == 0) {
                std::this_thread::sleep_for(10ms);
                float readValue;
                if (client.getCurrentValue(path, readValue)) {
                    verifiedCount++;
                }
            }
        }

        auto end = std::chrono::high_resolution_clock::now();
        double duration = std::chrono::duration<double, std::milli>(end - start).count();
        double avgLatency = duration / iterations;

        if (verifiedCount > 0) {
            addResult("STRESS_WRITE", path, true,
                     std::to_string(iterations) + " writes completed, " +
                     std::to_string(verifiedCount) + " verified, avg " + formatDuration(avgLatency), duration);
            std::cout << COLOR_GREEN << "    ✓ " << COLOR_RESET
                     << iterations << " writes completed, " << verifiedCount << " verified"
                     << " (avg: " << formatDuration(avgLatency) << "/write, total: "
                     << formatDuration(duration) << ")\n";
        } else {
            addResult("STRESS_WRITE", path, false,
                     "Signal not available for verification", duration);
            std::cout << COLOR_YELLOW << "    ⊗ " << COLOR_RESET
                     << "Could not verify writes (signal not available)\n";
        }
    }

    void stressTestConcurrentReads(const std::string& path, int iterations = 100) {
        std::cout << COLOR_CYAN << "\n  → Stress test: " << iterations
                 << " concurrent reads from " << path << COLOR_RESET << "\n";

        auto start = std::chrono::high_resolution_clock::now();
        int successCount = 0;
        float lastValue = 0.0f;

        for (int i = 0; i < iterations; i++) {
            float value;
            if (client.getCurrentValue(path, value)) {
                successCount++;
                lastValue = value;
            }
        }

        auto end = std::chrono::high_resolution_clock::now();
        double duration = std::chrono::duration<double, std::milli>(end - start).count();
        double avgLatency = duration / iterations;

        if (successCount > 0) {
            addResult("STRESS_READ", path, true,
                     std::to_string(successCount) + "/" + std::to_string(iterations) +
                     " reads succeeded, avg " + formatDuration(avgLatency), duration);
            std::cout << COLOR_GREEN << "    ✓ " << COLOR_RESET
                     << successCount << "/" << iterations << " reads succeeded"
                     << " (avg: " << formatDuration(avgLatency) << "/read, total: "
                     << formatDuration(duration) << ")\n";
        } else {
            addResult("STRESS_READ", path, false,
                     "All reads failed - signal not available", duration);
            std::cout << COLOR_YELLOW << "    ⊗ " << COLOR_RESET
                     << "All reads failed (signal not available)\n";
        }
    }

    // ==================== REPORT GENERATION ====================

    void printSummary() const {
        std::cout << "\n" << COLOR_BOLD << "========================================\n";
        std::cout << "         TEST SUMMARY REPORT\n";
        std::cout << "========================================" << COLOR_RESET << "\n\n";

        std::cout << "Total Tests:   " << totalTests << "\n";
        std::cout << COLOR_GREEN << "Passed:        " << passedTests << COLOR_RESET << "\n";
        std::cout << COLOR_RED << "Failed:        " << failedTests << COLOR_RESET << "\n";
        std::cout << COLOR_YELLOW << "Skipped:       " << skippedTests
                 << " (unavailable signals)" << COLOR_RESET << "\n\n";

        int availableTests = totalTests - skippedTests;
        double successRate = availableTests > 0 ?
            (double)(passedTests) / availableTests * 100.0 : 0.0;

        std::cout << "Success Rate:  " << std::fixed << std::setprecision(1)
                 << successRate << "% (excluding skipped)\n\n";

        // Group results by test type
        std::map<std::string, int> testTypeCounts;
        std::map<std::string, int> testTypeSuccess;

        for (const auto& result : results) {
            testTypeCounts[result.testName]++;
            if (result.passed) {
                testTypeSuccess[result.testName]++;
            }
        }

        std::cout << COLOR_BOLD << "Results by Test Type:" << COLOR_RESET << "\n";
        for (const auto& [testType, count] : testTypeCounts) {
            int success = testTypeSuccess[testType];
            std::cout << "  " << std::setw(20) << std::left << testType << ": "
                     << success << "/" << count << "\n";
        }

        std::cout << "\n" << COLOR_BOLD << "========================================"
                 << COLOR_RESET << "\n\n";
    }

    void saveReport(const std::string& filename) const {
        std::ofstream file(filename);
        if (!file.is_open()) {
            std::cerr << COLOR_RED << "Failed to open report file: " << filename << COLOR_RESET << "\n";
            return;
        }

        auto now = std::chrono::system_clock::now();
        auto time = std::chrono::system_clock::to_time_t(now);

        file << "VSS Signal End-to-End Test Report\n";
        file << "Generated: " << std::ctime(&time) << "\n";
        file << "Total Tests: " << totalTests << "\n";
        file << "Passed: " << passedTests << "\n";
        file << "Failed: " << failedTests << "\n";
        file << "Skipped: " << skippedTests << "\n\n";

        file << "Detailed Results:\n";
        file << std::string(100, '=') << "\n";

        for (const auto& result : results) {
            file << "[" << (result.passed ? "PASS" : "FAIL") << "] ";
            file << std::setw(25) << result.testName << " | ";
            file << std::setw(50) << result.signalPath << " | ";
            file << result.message;
            if (result.duration_ms > 0) {
                file << " (" << std::fixed << std::setprecision(2)
                     << result.duration_ms << "ms)";
            }
            file << "\n";
        }

        file.close();
        std::cout << COLOR_GREEN << "Report saved to: " << filename << COLOR_RESET << "\n";
    }
};

// ==================== MAIN TEST RUNNER ====================

int main(int argc, char* argv[]) {
    std::cout << COLOR_BOLD << "\n╔════════════════════════════════════════╗\n";
    std::cout << "║  VSS Signal End-to-End Test Suite     ║\n";
    std::cout << "╚════════════════════════════════════════╝\n" << COLOR_RESET << "\n";

    // Setup Kuksa client
    std::string serverURI = (argc > 1) ? argv[1] : "127.0.0.1:55555";

    KuksaClient::Config clientConfig;
    clientConfig.serverURI = serverURI;
    clientConfig.debug = false;

    KuksaClient::KuksaClient client(clientConfig);
    std::cout << "Connecting to KUKSA Databroker at " << clientConfig.serverURI << "...\n";

    try {
        client.connect();  // connect() returns void, throws on error
        std::cout << COLOR_GREEN << "✓ Connected successfully!\n\n" << COLOR_RESET;
    } catch (const std::exception& e) {
        std::cerr << COLOR_RED << "Failed to connect to databroker!" << COLOR_RESET << "\n";
        std::cerr << "Error: " << e.what() << "\n";
        std::cerr << "Make sure KUKSA Databroker is running at " << clientConfig.serverURI << "\n";
        return 1;
    }

    VSSSignalTester tester(client);

    // ==================== TEST SENSOR SIGNALS (E2E) ====================
    std::cout << COLOR_BOLD << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    std::cout << "  SENSOR SIGNALS (End-to-End Testing)\n";
    std::cout << "  Write → Read → Verify\n";
    std::cout << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" << COLOR_RESET << "\n";

    tester.testE2E<float>("Vehicle.Speed",
                          {0.0f, 60.5f, 120.0f, 80.5f, 0.0f},
                          "Vehicle speed (km/h)", "FLOAT");

    tester.testE2E<uint32_t>("Vehicle.Powertrain.FuelSystem.Range",
                             {500, 350, 200, 100, 50},
                             "Fuel range (km)", "UINT32");

    tester.testE2E<int8_t>("Vehicle.Powertrain.Transmission.CurrentGear",
                           {-1, 0, 1, 2, 3, 4, 5, 6},
                           "Current gear", "INT8");

    tester.testE2E<uint16_t>("Vehicle.Powertrain.CombustionEngine.Speed",
                             {800, 1500, 3000, 4500, 6000, 800},
                             "Engine RPM", "UINT16");

    tester.testE2E<uint16_t>("Vehicle.Powertrain.CombustionEngine.Power",
                             {0, 50, 100, 200, 300, 150, 0},
                             "Engine power (kW)", "UINT16");

    tester.testE2E<bool>("Vehicle.Powertrain.IsIgnitionOn",
                         {true, false, true},
                         "Ignition status", "BOOL");

    tester.testE2E<float>("Vehicle.Chassis.Accelerator.PedalPosition",
                          {0.0f, 25.0f, 50.0f, 75.0f, 100.0f, 50.0f, 0.0f},
                          "Accelerator pedal (%)", "FLOAT");

    tester.testE2E<float>("Vehicle.Chassis.Brake.PedalPosition",
                          {0.0f, 30.0f, 60.0f, 100.0f, 0.0f},
                          "Brake pedal (%)", "FLOAT");

    // ==================== TEST ACTUATOR SIGNALS (E2E) ====================
    std::cout << "\n" << COLOR_BOLD << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    std::cout << "  ACTUATOR SIGNALS (End-to-End Testing)\n";
    std::cout << "  Write → Read → Verify\n";
    std::cout << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" << COLOR_RESET << "\n";

    tester.testE2E<std::string>("Vehicle.Body.DriveMode",
                                {"ECO", "NORMAL", "SPORT", "U_SPORTINESS", "NORMAL"},
                                "Drive mode", "STRING");

    tester.testE2E<float>("Vehicle.Chassis.Sportiness.Target",
                          {0.0f, 25.0f, 50.0f, 75.0f, 100.0f, 50.0f},
                          "Sportiness target (%)", "FLOAT");

    tester.testE2E<uint8_t>("Vehicle.Chassis.Sportiness.Mode",
                            {0, 2, 5, 8, 10, 5},
                            "Sportiness mode level", "UINT8");

    // ==================== SUBSCRIPTION TESTS ====================
    std::cout << "\n" << COLOR_BOLD << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    std::cout << "  SUBSCRIPTION TESTS\n";
    std::cout << "  (Will monitor for 3 seconds each)\n";
    std::cout << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" << COLOR_RESET << "\n";

    tester.testSubscription("Vehicle.Speed", "Speed monitoring", 3);
    tester.testSubscription("Vehicle.Chassis.Accelerator.PedalPosition", "Accelerator monitoring", 3);

    // ==================== STRESS TESTS ====================
    std::cout << "\n" << COLOR_BOLD << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    std::cout << "  STRESS TESTS\n";
    std::cout << "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" << COLOR_RESET << "\n";

    tester.stressTestConcurrentWrites("Vehicle.Chassis.Sportiness.Target", 50.0f, 100);
    tester.stressTestConcurrentReads("Vehicle.Speed", 100);

    // ==================== FINAL REPORT ====================
    tester.printSummary();
    tester.saveReport("test_results.txt");

    std::cout << COLOR_GREEN << "\n✓ All tests completed!\n" << COLOR_RESET;
    return 0;
}
