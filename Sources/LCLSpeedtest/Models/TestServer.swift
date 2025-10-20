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

#if os(Linux)
import FoundationNetworking
#endif

/// The M-Lab test server location
public struct TestServerLocation: Codable {
    /// The contry in which the test server is located.
    public let country: String?

    /// The city in which the test server is located.
    public let city: String?
}

/// The M-Lab test server URL
public struct TestServerURLs: Codable {

    /// The download test server URL (secure, start with wss).
    public let downloadPath: String

    /// The upload test server URL (secure, start with wss).
    public let uploadPath: String

    /// The download test server URL (insecure, start with ws)
    public let insecureDownloadPath: String

    /// The upload test server URL (insecure, start with ws)
    public let insecureUploadPath: String

    enum CodingKeys: String, CodingKey {
        case downloadPath = "wss:///ndt/v7/download"
        case uploadPath = "wss:///ndt/v7/upload"
        case insecureDownloadPath = "ws:///ndt/v7/download"
        case insecureUploadPath = "ws:///ndt/v7/upload"
    }
}

/// The M-Lab test server
public struct TestServer: Codable {
    /// The name of the machine.
    public let machine: String

    /// The location of the test server. See `TestServerLocation`.
    public let location: TestServerLocation

    /// The URLs of the test servers. See `TestServerURL`.
    public let urls: TestServerURLs
}

extension TestServer {

    /// Discover available test servers from M-Lab asynchronously.
    ///
    /// - Returns: an array of `TestServer`
    /// - Throws: `SpeedTestError.testServersOutOfCapacity` is test server is out of capacity and there is no test server available.
    /// - Throws: `SpeedTestError.rateLimited` if too many requests have been made.
    internal static func discover() async throws -> [TestServer] {
        let result = try await Networking.fetch(from: discoverServerURL)
        if result.isEmpty {
            throw SpeedTestError.testServersOutOfCapacity
        }

        let response = try JSONDecoder().decode(TestServerResponse.self, from: result)
        return response.results
    }
}

/// Response object from M-lab server regarding the test server information.
internal struct TestServerResponse: Codable {

    /// An array of `TestServer` for available test servers.
    let results: [TestServer]
}
