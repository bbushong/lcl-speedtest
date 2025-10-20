//
// This source file is part of the LCL open source project
//
// Copyright (c) 2021-2024 Local Connectivity Lab and the project authors
// Licensed under Apache License v2.0
//
// See LICENSE for license information
// See CONTRIBUTORS for the list of project authors
//
// SPDX-License-Identifier: Apache-2.0
//

import Foundation

/// The speed test client that will perform the upload, download, or both upload and download speed test
/// using the [M-Lab NDT7 protocol](https://www.measurementlab.net/tests/ndt/ndt7/).
public struct SpeedTestClient {

    /// Callback function that will be invoked during the upload test
    /// when an intermediate test progress is available.
    public var onUploadProgress: ((MeasurementProgress) -> Void)?

    /// Callback function that will be invoked during the upload test
    /// when a measurement result is available.
    public var onUploadMeasurement: ((SpeedTestMeasurement) -> Void)?

    /// Callback function that will be invoked during the donwload test
    /// when an intermediate test progress is available.
    public var onDownloadProgress: ((MeasurementProgress) -> Void)?

    /// Callback function that will be invoked during the download test
    /// when a measurement result is available.
    public var onDownloadMeasurement: ((SpeedTestMeasurement) -> Void)?

    /// Callback function that will be invoked when a test server is selected
    public var onServerSelected: ((TestServer) -> Void)?

    /// The selected test server for the current/last test
    public private(set) var selectedServer: TestServer?

    /// The download test client
    private var downloader: DownloadClient?

    /// The upload client
    private var uploader: UploadClient?

    public init() {}

    /// Start the speed test according to the test type asynchronously.
    ///
    /// - Parameters:
    ///   - type: The type of test to run (download, upload, or both)
    ///   - connectionMode: The connection security mode (secure wss:// or insecure ws://). Defaults to .secure
    ///   - testDuration: The maximum duration in seconds for each phase (download/upload). Defaults to 10 seconds
    ///   - deviceName: Optional device name to include in test metadata
    public mutating func start(with type: TestType, connectionMode: ConnectionMode = .secure, testDuration: Int64 = 10, deviceName: String? = nil) async throws {
        do {
            let testServers = try await TestServer.discover()

            // Store and notify about selected server
            if let firstServer = testServers.first {
                self.selectedServer = firstServer
                self.onServerSelected?(firstServer)
            }

            switch type {
            case .download:
                try await runDownloadTest(using: testServers, connectionMode: connectionMode, testDuration: testDuration, deviceName: deviceName)
            case .upload:
                try await runUploadTest(using: testServers, connectionMode: connectionMode, testDuration: testDuration, deviceName: deviceName)
            case .downloadAndUpload:
                try await runDownloadTest(using: testServers, connectionMode: connectionMode, testDuration: testDuration, deviceName: deviceName)
                try await runUploadTest(using: testServers, connectionMode: connectionMode, testDuration: testDuration, deviceName: deviceName)
            }
        } catch {
            throw error
        }
    }

    /// Stop and cancel the remaining test.
    /// Cancellation will be cooperative, but the system will try its best to stop the best.
    public func cancel() throws {
        try downloader?.stop()
        try uploader?.stop()
    }

    /// Run the download test using the available test servers
    private mutating func runDownloadTest(
        using testServers: [TestServer],
        connectionMode: ConnectionMode,
        testDuration: Int64,
        deviceName: String? = nil
    )
        async throws
    {
        guard !testServers.isEmpty else {
            throw SpeedTestError.invalidTestURL("No test servers available")
        }

        let maxRetriesPerServer = 3
        var lastError: Error?

        // Try each server in sequence
        for (serverIndex, server) in testServers.enumerated() {
            let downloadPath: String
            switch connectionMode {
            case .secure:
                downloadPath = server.urls.downloadPath
            case .insecure:
                downloadPath = server.urls.insecureDownloadPath
            }

            guard let downloadURL = URL(string: downloadPath) else {
                print("Server \(serverIndex + 1)/\(testServers.count) (\(server.machine)): Invalid URL, skipping")
                lastError = SpeedTestError.invalidTestURL("Invalid URL for server \(server.machine)")
                continue
            }

            print("Trying server \(serverIndex + 1)/\(testServers.count): \(server.machine) (\(server.location.city ?? "unknown"), \(server.location.country ?? "unknown"))")

            // Retry current server up to 3 times
            var shouldTryNextServer = false

            for attempt in 1...maxRetriesPerServer {
                do {
                    print("  Download attempt \(attempt) of \(maxRetriesPerServer)")

                    // Use continuation to track completion with data check
                    let bytesReceived: Int = try await withCheckedThrowingContinuation { continuation in
                        let client = DownloadClient(url: downloadURL, deviceName: deviceName, measurementDuration: testDuration)
                        client.onProgress = self.onDownloadProgress
                        client.onMeasurement = self.onDownloadMeasurement
                        self.downloader = client

                        var hasResumed = false
                        client.onFinish = { progress, error in
                            guard !hasResumed else { return }
                            hasResumed = true

                            if let error = error {
                                continuation.resume(throwing: error)
                            } else {
                                continuation.resume(returning: Int(progress.appInfo.numBytes))
                            }
                        }

                        Task {
                            do {
                                try await client.start().get()
                            } catch {
                                if !hasResumed {
                                    hasResumed = true
                                    continuation.resume(throwing: error)
                                }
                            }
                        }
                    }

                    // Check if we actually received data
                    if bytesReceived > 0 {
                        print("  ✓ Download succeeded with \(bytesReceived) bytes from \(server.machine)")
                        return // Success!
                    } else {
                        lastError = SpeedTestError.testFailed("Download completed but no data received")
                        print("  Download attempt \(attempt) failed: no data received")
                    }
                } catch {
                    lastError = error
                    let errorMessage = "\(error)"
                    print("  Download attempt \(attempt) failed: \(errorMessage)")

                    // Check if this is a protocol error that indicates we should try a different server
                    if errorMessage.contains("unknownControl") ||
                       errorMessage.contains("Invalid reserved bits") ||
                       errorMessage.contains("fragmentedControlFrame") {
                        print("  Protocol error detected - will try next server")
                        shouldTryNextServer = true
                        break
                    }
                }

                // Wait before retry (unless it's the last attempt or we're moving to next server)
                if attempt < maxRetriesPerServer && !shouldTryNextServer {
                    try await Task.sleep(nanoseconds: 2_000_000_000) // 2 seconds
                }
            }

            // If we should try next server, continue to next iteration
            if shouldTryNextServer {
                continue
            }

            // If we got here without shouldTryNextServer being true, all retries for this server failed
            // Continue to next server
            print("  All attempts failed for \(server.machine), trying next server...")
        }

        // If all servers and all attempts failed, throw the last error
        throw lastError ?? SpeedTestError.testFailed("Download failed after trying \(testServers.count) server(s)")
    }

    /// Run the upload test using the available test servers
    private mutating func runUploadTest(
        using testServers: [TestServer],
        connectionMode: ConnectionMode,
        testDuration: Int64,
        deviceName: String? = nil
    )
        async throws
    {
        guard !testServers.isEmpty else {
            throw SpeedTestError.invalidTestURL("No test servers available")
        }

        let maxRetriesPerServer = 3
        var lastError: Error?

        // Try each server in sequence
        for (serverIndex, server) in testServers.enumerated() {
            let uploadPath: String
            switch connectionMode {
            case .secure:
                uploadPath = server.urls.uploadPath
            case .insecure:
                uploadPath = server.urls.insecureUploadPath
            }

            guard let uploadURL = URL(string: uploadPath) else {
                print("Server \(serverIndex + 1)/\(testServers.count) (\(server.machine)): Invalid URL, skipping")
                lastError = SpeedTestError.invalidTestURL("Invalid URL for server \(server.machine)")
                continue
            }

            print("Trying server \(serverIndex + 1)/\(testServers.count): \(server.machine) (\(server.location.city ?? "unknown"), \(server.location.country ?? "unknown"))")

            // Retry current server up to 3 times
            var shouldTryNextServer = false

            for attempt in 1...maxRetriesPerServer {
                do {
                    print("  Upload attempt \(attempt) of \(maxRetriesPerServer)")

                    // Use continuation to track completion with data check
                    let bytesReceived: Int = try await withCheckedThrowingContinuation { continuation in
                        let client = UploadClient(url: uploadURL, deviceName: deviceName, measurementDuration: testDuration)
                        client.onProgress = self.onUploadProgress
                        client.onMeasurement = self.onUploadMeasurement
                        self.uploader = client

                        var hasResumed = false
                        client.onFinish = { progress, error in
                            guard !hasResumed else { return }
                            hasResumed = true

                            if let error = error {
                                continuation.resume(throwing: error)
                            } else {
                                continuation.resume(returning: Int(progress.appInfo.numBytes))
                            }
                        }

                        Task {
                            do {
                                try await client.start().get()
                            } catch {
                                if !hasResumed {
                                    hasResumed = true
                                    continuation.resume(throwing: error)
                                }
                            }
                        }
                    }

                    // Check if we actually sent data
                    if bytesReceived > 0 {
                        print("  ✓ Upload succeeded with \(bytesReceived) bytes to \(server.machine)")
                        return // Success!
                    } else {
                        lastError = SpeedTestError.testFailed("Upload completed but no data sent")
                        print("  Upload attempt \(attempt) failed: no data sent")
                    }
                } catch {
                    lastError = error
                    let errorMessage = "\(error)"
                    print("  Upload attempt \(attempt) failed: \(errorMessage)")

                    // Check if this is a protocol error that indicates we should try a different server
                    if errorMessage.contains("unknownControl") ||
                       errorMessage.contains("Invalid reserved bits") ||
                       errorMessage.contains("fragmentedControlFrame") {
                        print("  Protocol error detected - will try next server")
                        shouldTryNextServer = true
                        break
                    }
                }

                // Wait before retry (unless it's the last attempt or we're moving to next server)
                if attempt < maxRetriesPerServer && !shouldTryNextServer {
                    try await Task.sleep(nanoseconds: 2_000_000_000) // 2 seconds
                }
            }

            // If we should try next server, continue to next iteration
            if shouldTryNextServer {
                continue
            }

            // If we got here without shouldTryNextServer being true, all retries for this server failed
            // Continue to next server
            print("  All attempts failed for \(server.machine), trying next server...")
        }

        // If all servers and all attempts failed, throw the last error
        throw lastError ?? SpeedTestError.testFailed("Upload failed after trying \(testServers.count) server(s)")
    }
}
