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
import LCLWebSocket
import NIOCore
import NIOPosix
import NIOWebSocket

internal final class DownloadClient: SpeedTestable {
    private let url: URL
    private let eventloopGroup: MultiThreadedEventLoopGroup

    private var startTime: NIODeadline
    private var totalBytes: Int
    private var previousTimeMark: NIODeadline
    private var deviceName: String?
    private let jsonDecoder: JSONDecoder
    private let emitter = DispatchQueue(label: "downloader", qos: .userInteractive)
    private let measurementDuration: Int64
    private var timeoutTriggered: Bool = false
    private var connectionClosed: Bool = false
    private var onFinishCalled: Bool = false

    required init(url: URL) {
        self.url = url
        self.eventloopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 4)
        self.startTime = .now()
        self.previousTimeMark = .now()
        self.totalBytes = 0
        self.jsonDecoder = JSONDecoder()
        self.deviceName = nil
        self.measurementDuration = 10  // Default 10 seconds (NDT7 spec)
    }

    convenience init(url: URL, deviceName: String?) {
        self.init(url: url)
        self.deviceName = deviceName
    }

    init(url: URL, deviceName: String?, measurementDuration: Int64) {
        self.url = url
        self.eventloopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 4)
        self.startTime = .now()
        self.previousTimeMark = .now()
        self.totalBytes = 0
        self.jsonDecoder = JSONDecoder()
        self.deviceName = deviceName
        self.measurementDuration = measurementDuration
    }

    var websocketConfiguration: LCLWebSocket.Configuration {
        .init(
            maxFrameSize: maxMessageSize,
            minNonFinalFragmentSize: minMessageSize,
            deviceName: self.deviceName
        )
    }

    var onMeasurement: ((SpeedTestMeasurement) -> Void)?
    var onProgress: ((MeasurementProgress) -> Void)?
    var onFinish: ((MeasurementProgress, Error?) -> Void)?

    func start() throws -> EventLoopFuture<Void> {
        let promise = self.eventloopGroup.next().makePromise(of: Void.self)
        self.startTime = .now()

        var client = LCLWebSocket.client(on: self.eventloopGroup)
        var websocket: WebSocket?
        var timeoutTask: Scheduled<Void>?

        client.onOpen { ws in
            print("websocket connected")
            websocket = ws

            // Schedule early failure detection - if no data in 2s, server is broken
            let earlyCheckEl = self.eventloopGroup.next()
            let earlyCheckTask = earlyCheckEl.scheduleTask(in: TimeAmount.seconds(2)) {
                // If no data received and connection is closed, fail immediately
                if self.totalBytes == 0 && self.connectionClosed {
                    print("Early failure detection: connection closed with no data after 2s")
                    if !self.onFinishCalled, let onFinish = self.onFinish {
                        self.onFinishCalled = true
                        self.emitter.async {
                            onFinish(
                                DownloadClient.generateMeasurementProgress(
                                    startTime: self.startTime,
                                    numBytes: self.totalBytes,
                                    direction: .download
                                ),
                                SpeedTestError.testFailed("Connection closed with no data")
                            )
                        }
                    }
                }
            }

            // Schedule timeout to force close if download takes too long
            let el = self.eventloopGroup.next()
            timeoutTask = el.scheduleTask(in: TimeAmount.seconds(self.measurementDuration)) {
                // Cancel early check when full timeout fires
                earlyCheckTask.cancel()

                guard NIODeadline.now() - self.startTime >= TimeAmount.seconds(self.measurementDuration) else {
                    return
                }

                print("Download timeout reached")
                self.timeoutTriggered = true

                // Try to close if connection is still open
                if !self.connectionClosed {
                    print("Closing connection due to timeout")
                    _ = ws.close(code: .normalClosure)
                } else {
                    print("Connection already closed when timeout fired")
                }

                // ALWAYS call onFinish to ensure continuation resumes
                // (in case onClosing never fired due to protocol errors)
                if !self.onFinishCalled, let onFinish = self.onFinish {
                    self.onFinishCalled = true
                    print("Calling onFinish from timeout")
                    self.emitter.async {
                        onFinish(
                            DownloadClient.generateMeasurementProgress(
                                startTime: self.startTime,
                                numBytes: self.totalBytes,
                                direction: .download
                            ),
                            nil
                        )
                    }
                }
            }
        }
        client.onText(self.onText(ws:text:))
        client.onBinary(self.onBinary(ws:bytes:))
        client.onClosing { closeCode, _ in
            // Mark connection as closed to prevent timeout from trying to close it
            self.connectionClosed = true

            // Cancel timeout task when connection is closing
            timeoutTask?.cancel()

            let result = self.onClose(closeCode: closeCode)
            switch result {
            case .success:
                print("Connection closing normally")
                promise.succeed()
                if !self.onFinishCalled, let onFinish = self.onFinish {
                    self.onFinishCalled = true
                    self.emitter.async {
                        onFinish(
                            DownloadClient.generateMeasurementProgress(
                                startTime: self.startTime,
                                numBytes: self.totalBytes,
                                direction: .download
                            ),
                            nil
                        )
                    }
                }
            case .failure(let error):
                print("Connection closing with error: \(error)")
                promise.fail(error)
                if !self.onFinishCalled, let onFinish = self.onFinish {
                    self.onFinishCalled = true
                    self.emitter.async {
                        onFinish(
                            DownloadClient.generateMeasurementProgress(
                                startTime: self.startTime,
                                numBytes: self.totalBytes,
                                direction: .download
                            ),
                            error
                        )
                    }
                }
            }
        }

        // Add error handler to catch WebSocket failures gracefully
        client.onError { error in
            print("DownloadClient WebSocket error: \(error)")
            // Cancel timeout task on error
            timeoutTask?.cancel()

            // If timeout triggered the close, treat it as successful completion
            // (the actual data will be evaluated in onFinish)
            if self.timeoutTriggered {
                print("Error occurred after timeout-triggered close - completing normally")
                // Trigger onFinish to complete the test (if not already called from timeout)
                if !self.onFinishCalled, let onFinish = self.onFinish {
                    self.onFinishCalled = true
                    self.emitter.async {
                        onFinish(
                            DownloadClient.generateMeasurementProgress(
                                startTime: self.startTime,
                                numBytes: self.totalBytes,
                                direction: .download
                            ),
                            nil
                        )
                    }
                }
            } else {
                // Genuine error - fail the promise
                print("Failing promise with error: \(error)")
                promise.fail(error)

                // Also trigger onFinish with the error to ensure continuation doesn't hang
                if !self.onFinishCalled, let onFinish = self.onFinish {
                    self.onFinishCalled = true
                    self.emitter.async {
                        onFinish(
                            DownloadClient.generateMeasurementProgress(
                                startTime: self.startTime,
                                numBytes: self.totalBytes,
                                direction: .download
                            ),
                            error
                        )
                    }
                }
            }
        }

        // Don't cascade connect promise - let onClosing/onError handle promise completion
        // Otherwise promise succeeds on connection, before protocol errors can fail it
        _ = client.connect(
            to: self.url,
            headers: self.httpHeaders,
            configuration: self.websocketConfiguration
        )
        return promise.futureResult
    }

    func stop() throws {
        var itr = self.eventloopGroup.makeIterator()
        while let next = itr.next() {
            try next.close()
        }
    }

    @Sendable
    func onText(ws: WebSocket, text: String) {
        let buffer = ByteBuffer(string: text)
        do {
            let measurement: SpeedTestMeasurement = try jsonDecoder.decode(
                SpeedTestMeasurement.self,
                from: buffer
            )
            self.totalBytes += buffer.readableBytes
            if let onMeasurement = self.onMeasurement {
                self.emitter.async {
                    onMeasurement(measurement)
                }
            }
        } catch {
            print("onText Error: \(error)")
        }
    }

    @Sendable
    func onBinary(ws: WebSocket, bytes: ByteBuffer) {
        self.totalBytes += bytes.readableBytes
        if let onProgress = self.onProgress {
            let current = NIODeadline.now()
            if (current - self.previousTimeMark) > TimeAmount.milliseconds(measurementReportInterval) {
                self.emitter.async {
                    onProgress(
                        DownloadClient.generateMeasurementProgress(
                            startTime: self.startTime,
                            numBytes: self.totalBytes,
                            direction: .download
                        )
                    )
                }
                self.previousTimeMark = current
            }
        }
    }
}
