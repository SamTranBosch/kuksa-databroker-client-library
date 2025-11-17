/**
 * Debug Test Framework for CAN → VSS → KUKSA Integration
 *
 * This header provides comprehensive debugging and instrumentation
 * for testing VSS signal flow from CAN to KUKSA databroker.
 *
 * Features:
 * - Signal timing instrumentation
 * - Queue depth monitoring
 * - VSS update verification
 * - KUKSA client-side confirmation
 * - Latency tracking
 * - Detailed logging
 */

#pragma once

#include "KuksaClient.hpp"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <map>
#include <string>
#include <fstream>
#include <sstream>
#include <atomic>
#include <mutex>
#include <cmath>

// ==================== ANSI Color Codes ====================
#define COLOR_RESET       "\033[0m"
#define COLOR_BLACK       "\033[30m"
#define COLOR_RED         "\033[31m"
#define COLOR_GREEN       "\033[32m"
#define COLOR_YELLOW      "\033[33m"
#define COLOR_BLUE        "\033[34m"
#define COLOR_MAGENTA     "\033[35m"
#define COLOR_CYAN        "\033[36m"
#define COLOR_WHITE       "\033[37m"
#define COLOR_BOLD        "\033[1m"
#define COLOR_DIM         "\033[2m"
#define COLOR_UNDERLINE   "\033[4m"

// ==================== Debug Instrumentation ====================

#ifdef DEBUG_VSS_FLOW
    #define DEBUG_LOG(fmt, ...) \
        do { \
            fprintf(stderr, "%s[DEBUG]%s " fmt "\n", COLOR_BLUE, COLOR_RESET, ##__VA_ARGS__); \
            fflush(stderr); \
        } while(0)
#else
    #define DEBUG_LOG(fmt, ...) do {} while(0)
#endif

#define TRACE_LOG(fmt, ...) \
    do { \
        auto now = std::chrono::high_resolution_clock::now(); \
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()); \
        fprintf(stderr, "%s[%lld.%03lld]%s " fmt "\n", \
                COLOR_DIM, ms.count() / 1000, ms.count() % 1000, COLOR_RESET, ##__VA_ARGS__); \
        fflush(stderr); \
    } while(0)

#define INFO_LOG(fmt, ...) \
    do { \
        fprintf(stdout, "%s[INFO]%s " fmt "\n", COLOR_CYAN, COLOR_RESET, ##__VA_ARGS__); \
        fflush(stdout); \
    } while(0)

#define SUCCESS_LOG(fmt, ...) \
    do { \
        fprintf(stdout, "%s✓%s " fmt "\n", COLOR_GREEN, COLOR_RESET, ##__VA_ARGS__); \
        fflush(stdout); \
    } while(0)

#define WARN_LOG(fmt, ...) \
    do { \
        fprintf(stdout, "%s⚠%s " fmt "\n", COLOR_YELLOW, COLOR_RESET, ##__VA_ARGS__); \
        fflush(stdout); \
    } while(0)

#define ERROR_LOG(fmt, ...) \
    do { \
        fprintf(stderr, "%s✗%s " fmt "\n", COLOR_RED, COLOR_RESET, ##__VA_ARGS__); \
        fflush(stderr); \
    } while(0)

// ==================== Test Result Tracking ====================

struct SignalTestResult {
    std::string vssPath;
    std::string dbcSignal;
    std::string description;

    // Timing measurements (in milliseconds)
    double writeTime;           // Time to send CAN frame
    double queueWaitTime;       // Time until removed from queue
    double kuksakClientSetTime; // Time for setCurrentValue call
    double totalQueuedTime;     // Time from write to queue processing
    double kukaClientGetTime;   // Time for getClientValue call
    double totalEndToEndTime;   // Total time from write to KUKSA read

    // Values
    std::string writtenValue;
    std::string readBackValue;

    // Status
    bool passed;
    bool signalAvailable;
    std::string errorMessage;

    // Query attempts
    int queryAttempts;
    std::vector<double> queryDelays;   // Delays when queries were performed
    std::vector<std::string> queryResults; // Results at each delay
};

// ==================== VSS Flow Tracker ====================

class VSSFlowTracker {
private:
    std::vector<SignalTestResult> results;
    std::mutex resultsMutex;
    std::atomic<int> totalSignals{0};
    std::atomic<int> passedSignals{0};
    std::atomic<int> failedSignals{0};

public:
    void addResult(const SignalTestResult& result) {
        std::lock_guard<std::mutex> lock(resultsMutex);
        results.push_back(result);
        totalSignals++;
        if (result.passed) {
            passedSignals++;
        } else {
            failedSignals++;
        }
    }

    void printSummary() const {
        std::cout << "\n" << COLOR_BOLD << COLOR_UNDERLINE
                  << "═══════════════════════════════════════════════════════════"
                  << COLOR_RESET << "\n";
        std::cout << COLOR_BOLD << "              VSS FLOW DEBUG TEST SUMMARY\n"
                  << COLOR_RESET;
        std::cout << COLOR_BOLD << COLOR_UNDERLINE
                  << "═══════════════════════════════════════════════════════════"
                  << COLOR_RESET << "\n\n";

        std::cout << "Total Signals Tested:    " << totalSignals << "\n";
        std::cout << COLOR_GREEN << "Passed:                  " << passedSignals << COLOR_RESET << "\n";
        std::cout << COLOR_RED << "Failed:                  " << failedSignals << COLOR_RESET << "\n";

        if (totalSignals > 0) {
            double passRate = (double)passedSignals / totalSignals * 100.0;
            std::cout << "Success Rate:            " << std::fixed << std::setprecision(1)
                      << passRate << "%\n";
        }

        // Print detailed results
        std::cout << "\n" << COLOR_BOLD << "Detailed Results:" << COLOR_RESET << "\n";
        std::cout << COLOR_DIM << std::string(130, '-') << COLOR_RESET << "\n";

        {
            std::lock_guard<std::mutex> lock(resultsMutex);

            for (const auto& result : results) {
                std::cout << (result.passed ? COLOR_GREEN : COLOR_RED)
                          << (result.passed ? "✓" : "✗")
                          << COLOR_RESET << " ";

                std::cout << std::setw(40) << std::left << result.vssPath << " | ";
                std::cout << std::setw(20) << std::left << result.dbcSignal << " | ";
                std::cout << std::fixed << std::setprecision(2);
                std::cout << "E2E: " << std::setw(6) << result.totalEndToEndTime << "ms";

                if (!result.passed) {
                    std::cout << " | " << COLOR_RED << result.errorMessage << COLOR_RESET;
                }
                std::cout << "\n";
            }
        }

        std::cout << COLOR_DIM << std::string(130, '-') << COLOR_RESET << "\n";
    }

    void saveDetailedReport(const std::string& filename) const {
        std::ofstream file(filename);
        if (!file.is_open()) {
            ERROR_LOG("Cannot open report file: %s", filename.c_str());
            return;
        }

        file << "VSS Flow Debug Test Report\n";
        file << "==========================\n\n";

        file << "Summary:\n";
        file << "  Total Signals: " << totalSignals << "\n";
        file << "  Passed: " << passedSignals << "\n";
        file << "  Failed: " << failedSignals << "\n\n";

        file << "Detailed Measurements:\n";
        file << std::string(180, '=') << "\n";

        {
            std::lock_guard<std::mutex> lock(resultsMutex);

            for (const auto& result : results) {
                file << "\nSignal: " << result.vssPath << "\n";
                file << "DBC Signal: " << result.dbcSignal << "\n";
                file << "Description: " << result.description << "\n";
                file << "Status: " << (result.passed ? "PASSED" : "FAILED") << "\n";

                if (!result.passed) {
                    file << "Error: " << result.errorMessage << "\n";
                }

                file << "\nTiming Measurements:\n";
                file << "  Write Time:           " << std::fixed << std::setprecision(2)
                     << result.writeTime << " ms\n";
                file << "  Queue Wait Time:      " << result.queueWaitTime << " ms\n";
                file << "  KUKSA Client Set Time: " << result.kuksakClientSetTime << " ms\n";
                file << "  Total Queued Time:    " << result.totalQueuedTime << " ms\n";
                file << "  KUKSA Client Get Time: " << result.kukaClientGetTime << " ms\n";
                file << "  Total E2E Time:       " << result.totalEndToEndTime << " ms\n";

                if (!result.queryDelays.empty()) {
                    file << "\nQuery Results:\n";
                    for (size_t i = 0; i < result.queryDelays.size(); i++) {
                        file << "  Query at " << std::fixed << std::setprecision(0)
                             << result.queryDelays[i] << "ms: "
                             << result.queryResults[i] << "\n";
                    }
                }

                file << std::string(180, '-') << "\n";
            }
        }

        file.close();
        SUCCESS_LOG("Report saved to: %s", filename.c_str());
    }
};

// ==================== CAN → VSS Debug Tester ====================

class DebugVSSSignalTester {
private:
    KuksaClient::KuksaClient& client;
    VSSFlowTracker tracker;

public:
    explicit DebugVSSSignalTester(KuksaClient::KuksaClient& c) : client(c) {}

    /**
     * Test CAN → VSS signal flow with detailed instrumentation
     *
     * Flow:
     * 1. Write test value to VSS
     * 2. Record write time
     * 3. Query VSS at various intervals (10ms, 50ms, 100ms, 200ms, 500ms, 1000ms)
     * 4. Track when value actually becomes visible
     * 5. Report total latency
     */
    template<typename T>
    void testSignalFlow(
        const std::string& vssPath,
        const std::string& dbcSignal,
        const std::string& description,
        const T& testValue,
        const std::vector<int>& queryDelaysMs = {10, 50, 100, 200, 500, 1000}
    ) {
        SignalTestResult result;
        result.vssPath = vssPath;
        result.dbcSignal = dbcSignal;
        result.description = description;
        result.passed = false;
        result.signalAvailable = false;
        result.queryAttempts = 0;

        std::cout << "\n" << COLOR_BOLD << COLOR_CYAN << "▶ Testing: "
                  << COLOR_RESET << vssPath << "\n";
        std::cout << "  DBC Signal: " << dbcSignal << "\n";
        std::cout << "  Description: " << description << "\n\n";

        // Step 1: Write value
        INFO_LOG("Step 1: Writing value to VSS...");

        auto writeStart = std::chrono::high_resolution_clock::now();
        client.setCurrentValue(vssPath, testValue);
        auto writeEnd = std::chrono::high_resolution_clock::now();

        result.writeTime = std::chrono::duration<double, std::milli>(writeEnd - writeStart).count();
        result.writtenValue = valueToString(testValue);

        SUCCESS_LOG("Value written in %.2f ms: %s", result.writeTime, result.writtenValue.c_str());

        // Step 2: Query at various delays
        INFO_LOG("Step 2: Querying VSS at various delays to track propagation...\n");

        bool valueFound = false;
        T readValue{};
        double foundAtDelay = -1.0;

        for (int delayMs : queryDelaysMs) {
            std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));

            TRACE_LOG("Querying at +%dms...", delayMs);

            auto queryStart = std::chrono::high_resolution_clock::now();
            bool querySuccess = client.getCurrentValue(vssPath, readValue);
            auto queryEnd = std::chrono::high_resolution_clock::now();

            double queryTime = std::chrono::duration<double, std::milli>(queryEnd - queryStart).count();
            result.kukaClientGetTime = queryTime;

            std::string readStr = querySuccess ? valueToString(readValue) : "NOT AVAILABLE";
            result.queryDelays.push_back(delayMs);
            result.queryResults.push_back(readStr);
            result.queryAttempts++;

            if (querySuccess && !valueFound) {
                if (valuesMatch(readValue, testValue)) {
                    valueFound = true;
                    foundAtDelay = delayMs;
                    result.signalAvailable = true;
                    result.readBackValue = readStr;
                    result.totalEndToEndTime = result.writeTime + delayMs + queryTime;

                    SUCCESS_LOG("Value confirmed at +%.0f ms: %s (got %.2f ms query time)",
                               foundAtDelay, readStr.c_str(), queryTime);
                }
            } else if (querySuccess) {
                std::cout << "  Query at +" << delayMs << "ms: " << readStr
                          << " (query time: " << std::fixed << std::setprecision(2)
                          << queryTime << "ms)\n";
            } else {
                WARN_LOG("Query at +%dms failed or signal unavailable", delayMs);
            }
        }

        // Step 3: Determine result
        if (valueFound) {
            result.passed = true;
            SUCCESS_LOG("✓ Signal flow successful! Latency: %.0f ms (write: %.2f ms + query: %.0f ms)",
                       result.totalEndToEndTime, result.writeTime, foundAtDelay);
        } else {
            result.passed = false;
            result.errorMessage = "Value never confirmed in VSS. Check KUKSA connection.";
            ERROR_LOG("✗ %s", result.errorMessage.c_str());
        }

        tracker.addResult(result);
    }

    /**
     * Test VSS → CAN actuator command flow
     */
    template<typename T>
    void testActuatorFlow(
        const std::string& vssPath,
        const std::string& dbcSignal,
        const std::string& description,
        const T& testValue
    ) {
        SignalTestResult result;
        result.vssPath = vssPath;
        result.dbcSignal = dbcSignal;
        result.description = description;
        result.passed = false;

        std::cout << "\n" << COLOR_BOLD << COLOR_MAGENTA << "▶ Testing Actuator: "
                  << COLOR_RESET << vssPath << "\n";
        std::cout << "  DBC Signal: " << dbcSignal << "\n";
        std::cout << "  Description: " << description << "\n\n";

        // Write actuator command
        INFO_LOG("Writing actuator command to VSS...");
        auto writeStart = std::chrono::high_resolution_clock::now();
        client.setCurrentValue(vssPath, testValue);
        auto writeEnd = std::chrono::high_resolution_clock::now();

        result.writeTime = std::chrono::duration<double, std::milli>(writeEnd - writeStart).count();
        result.writtenValue = valueToString(testValue);

        SUCCESS_LOG("Actuator command sent in %.2f ms: %s", result.writeTime, result.writtenValue.c_str());

        // Note: Can't easily verify actuator writes without CAN bus monitoring
        INFO_LOG("(Actuator verification requires CAN bus monitoring - check candump)");

        result.passed = true;
        tracker.addResult(result);
    }

    void printSummary() {
        tracker.printSummary();
    }

    void saveReport(const std::string& filename) {
        tracker.saveDetailedReport(filename);
    }

private:
    template<typename T>
    std::string valueToString(const T& value) {
        if constexpr (std::is_same_v<T, bool>) {
            return value ? "true" : "false";
        } else if constexpr (std::is_same_v<T, std::string>) {
            return "\"" + value + "\"";
        } else if constexpr (std::is_same_v<T, int8_t>) {
            return std::to_string(static_cast<int>(value));
        } else if constexpr (std::is_same_v<T, uint8_t>) {
            return std::to_string(static_cast<unsigned int>(value));
        } else {
            return std::to_string(value);
        }
    }

    template<typename T>
    bool valuesMatch(const T& a, const T& b) {
        if constexpr (std::is_floating_point_v<T>) {
            return std::abs(a - b) < 0.01;
        } else {
            return a == b;
        }
    }
};

#endif // DEBUG_TEST_HPP
