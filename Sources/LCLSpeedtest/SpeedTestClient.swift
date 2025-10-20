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
        let downloadPath: String?
        switch connectionMode {
        case .secure:
            downloadPath = testServers.first?.urls.downloadPath
        case .insecure:
            downloadPath = testServers.first?.urls.insecureDownloadPath
        }

        guard let path = downloadPath,
            let downloadURL = URL(string: path)
        else {
            throw SpeedTestError.invalidTestURL("Cannot locate URL for download test")
        }

        downloader = DownloadClient(url: downloadURL, deviceName: deviceName, measurementDuration: testDuration)
        downloader?.onProgress = self.onDownloadProgress
        downloader?.onMeasurement = self.onDownloadMeasurement
        try await downloader?.start().get()
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
        let uploadPath: String?
        switch connectionMode {
        case .secure:
            uploadPath = testServers.first?.urls.uploadPath
        case .insecure:
            uploadPath = testServers.first?.urls.insecureUploadPath
        }

        guard let path = uploadPath,
            let uploadURL = URL(string: path)
        else {
            throw SpeedTestError.invalidTestURL("Cannot locate URL for upload test")
        }

        uploader = UploadClient(url: uploadURL, deviceName: deviceName, measurementDuration: testDuration)
        uploader?.onProgress = self.onUploadProgress
        uploader?.onMeasurement = self.onUploadMeasurement
        try await uploader?.start().get()
    }
}
