import Foundation
import Testing
@testable import ConnectionPool

/// The core promise of the pool: no more than `poolSize` callers hold a connection at once,
/// whichever path they arrived through.
@Suite("Pool size enforcement", .timeLimit(.minutes(1)))
struct PoolLimitTests {

    private static let hold = Duration.milliseconds(25)

    @Test("blocking callers never exceed the pool size")
    func syncRespectsPoolSize() async {
        let poolSize = 3
        let workers = 12
        let (pool, state) = makePool(size: poolSize)
        let tracker = ConcurrencyTracker()
        let exclusivity = ExclusivityChecker()

        await runOnThreads(count: workers) { _ in
            _ = capture {
                try pool.withConnectionSync { connection in
                    exclusivity.use(connection) {
                        tracker.track { blockingSleep(Self.hold) }
                    }
                }
            }
        }

        #expect(tracker.total == workers, "every worker should eventually get a connection")
        #expect(tracker.peak <= poolSize, "peak concurrency was \(tracker.peak), pool size is \(poolSize)")
        #expect(exclusivity.violations.isEmpty, "\(exclusivity.violations)")
        #expect(state.createdCount <= poolSize)
        #expect(pool.currentConnection == nil)
    }

    @Test("async callers never exceed the pool size")
    func asyncRespectsPoolSize() async {
        let poolSize = 3
        let workers = 20
        let (pool, state) = makePool(size: poolSize)
        let tracker = ConcurrencyTracker()
        let exclusivity = ExclusivityChecker()

        await withTaskGroup(of: Void.self) { group in
            for _ in 0..<workers {
                group.addTask {
                    _ = await capture {
                        try await pool.withConnection { connection in
                            await exclusivity.use(connection) {
                                await tracker.track { try? await Task.sleep(for: Self.hold) }
                            }
                        }
                    }
                }
            }
        }

        #expect(tracker.total == workers, "only \(tracker.total) of \(workers) async callers ever got a connection")
        #expect(tracker.peak <= poolSize, "peak concurrency was \(tracker.peak), pool size is \(poolSize)")
        #expect(exclusivity.violations.isEmpty, "\(exclusivity.violations)")
        #expect(state.createdCount <= poolSize)
    }

    @Test("sync and async callers share one budget of permits")
    func mixedRespectsPoolSize() async {
        let poolSize = 2
        let workersPerKind = 6
        let (pool, state) = makePool(size: poolSize)
        let tracker = ConcurrencyTracker()
        let exclusivity = ExclusivityChecker()

        await withTaskGroup(of: Void.self) { group in
            group.addTask {
                await runOnThreads(count: workersPerKind) { _ in
                    _ = capture {
                        try pool.withConnectionSync { connection in
                            exclusivity.use(connection) {
                                tracker.track { blockingSleep(Self.hold) }
                            }
                        }
                    }
                }
            }
            for _ in 0..<workersPerKind {
                group.addTask {
                    _ = await capture {
                        try await pool.withConnection { connection in
                            await exclusivity.use(connection) {
                                await tracker.track { try? await Task.sleep(for: Self.hold) }
                            }
                        }
                    }
                }
            }
        }

        #expect(tracker.total == workersPerKind * 2, "a waiter of either kind must eventually be signalled")
        #expect(tracker.peak <= poolSize, "peak concurrency was \(tracker.peak), pool size is \(poolSize)")
        #expect(exclusivity.violations.isEmpty, "\(exclusivity.violations)")
        #expect(state.createdCount <= poolSize)
    }

    @Test("a pool of one serialises everything")
    func poolOfOneSerialises() async {
        let (pool, state) = makePool(size: 1)
        let tracker = ConcurrencyTracker()

        await withTaskGroup(of: Void.self) { group in
            group.addTask {
                await runOnThreads(count: 4) { _ in
                    _ = capture {
                        try pool.withConnectionSync { _ in
                            tracker.track { blockingSleep(.milliseconds(20)) }
                        }
                    }
                }
            }
            for _ in 0..<4 {
                group.addTask {
                    _ = await capture {
                        try await pool.withConnection { _ in
                            await tracker.track { try? await Task.sleep(for: .milliseconds(20)) }
                        }
                    }
                }
            }
        }

        #expect(tracker.total == 8)
        #expect(tracker.peak == 1)
        #expect(state.createdCount == 1, "one permit can only ever need one connection")
    }

    @Test("a permit taken by a failing handler is returned to the pool")
    func permitsSurviveHandlerFailures() async {
        let (pool, _) = makePool(size: 1)

        await runOnThreads(count: 8) { _ in
            _ = capture {
                try pool.withConnectionSync { _ in throw MockError.handlerFailed }
            }
        }
        await withTaskGroup(of: Void.self) { group in
            for _ in 0..<8 {
                group.addTask {
                    _ = await capture {
                        try await pool.withConnection { _ in throw MockError.handlerFailed }
                    }
                }
            }
        }

        let outcome = await expectCompletes(within: .seconds(3)) {
            await capture { try await pool.withConnection { $0.serial } }
        }
        #expect(outcome?.value == 1, "the pool stopped handing out connections after handler failures")
    }

    @Test("connections are created lazily, never more than the pool size")
    func connectionsAreCreatedLazily() async throws {
        let poolSize = 5
        let (pool, state) = makePool(size: poolSize)

        #expect(state.createdCount == 0, "constructing a pool must not open any connection")

        try await pool.withConnection { _ in }
        #expect(state.createdCount == 1, "a single caller only needs a single connection")

        await runOnThreads(count: 20) { _ in
            _ = capture {
                try pool.withConnectionSync { _ in blockingSleep(.milliseconds(20)) }
            }
        }

        #expect(state.createdCount <= poolSize, "created \(state.createdCount) connections for a pool of \(poolSize)")
    }

    @Test("queued waiters are served in arrival order")
    func waitersAreServedFirstInFirstOut() async {
        let (pool, _) = makePool(size: 1)
        let served = Collector<Int>()
        let arrivals = 5

        await withTaskGroup(of: Void.self) { group in
            // Occupies the only permit for long enough that every waiter has queued behind it.
            group.addTask {
                _ = await capture {
                    try await pool.withConnection { _ in
                        try? await Task.sleep(for: .milliseconds(400))
                    }
                }
            }
            try? await Task.sleep(for: .milliseconds(50))

            for index in 0..<arrivals {
                group.addTask {
                    _ = await capture {
                        try await pool.withConnection { _ in
                            served.append(index)
                            try? await Task.sleep(for: .milliseconds(5))
                        }
                    }
                }
                // Wide enough that each waiter is queued before the next one arrives.
                try? await Task.sleep(for: .milliseconds(30))
            }
        }

        #expect(served.values == Array(0..<arrivals), "waiters were served out of order: \(served.values)")
    }

    /// Drives timeouts, cancellations and signals into each other on purpose. The permit handoff
    /// has to arbitrate all three, and the tell for getting it wrong is a permit that quietly
    /// disappears — so the real assertion is the recovery check at the end.
    @Test("a storm of timeouts and cancellations leaves every permit accounted for")
    func randomisedStressLeavesNoPermitBehind() async {
        let poolSize = 4
        let (pool, _) = makePool(size: poolSize, timeout: .milliseconds(50))
        let tracker = ConcurrencyTracker()
        let exclusivity = ExclusivityChecker()

        await withTaskGroup(of: Void.self) { group in
            for index in 0..<40 {
                group.addTask {
                    let work = Task {
                        _ = await capture {
                            try await pool.withConnection(timeout: .milliseconds(.random(in: 5...60))) { connection in
                                await exclusivity.use(connection) {
                                    await tracker.track {
                                        try? await Task.sleep(for: .milliseconds(.random(in: 1...20)))
                                    }
                                }
                                // occasionally the peer drops the connection mid-use
                                if index % 11 == 0 { connection.invalidate() }
                            }
                        }
                    }
                    if index % 7 == 0 {
                        try? await Task.sleep(for: .milliseconds(.random(in: 1...30)))
                        work.cancel()
                    }
                    await work.value
                }
            }
            group.addTask {
                await runOnThreads(count: 12) { _ in
                    _ = capture {
                        try pool.withConnectionSync(timeout: .milliseconds(.random(in: 5...60))) { connection in
                            exclusivity.use(connection) {
                                tracker.track { blockingSleep(.milliseconds(.random(in: 1...20))) }
                            }
                        }
                    }
                }
            }
        }

        #expect(tracker.peak <= poolSize, "peak concurrency was \(tracker.peak), pool size is \(poolSize)")
        #expect(exclusivity.violations.isEmpty, "\(exclusivity.violations)")

        // Every permit must still be there: `poolSize` callers should all get in at once.
        let recovered = ConcurrencyTracker()
        let finished = await completes(within: .seconds(5)) {
            await withTaskGroup(of: Void.self) { group in
                for _ in 0..<poolSize {
                    group.addTask {
                        _ = await capture {
                            try await pool.withConnection(timeout: .seconds(3)) { _ in
                                await recovered.track { try? await Task.sleep(for: .milliseconds(150)) }
                            }
                        }
                    }
                }
            }
        }

        #expect(finished, "the pool stopped serving callers after the storm")
        #expect(recovered.peak == poolSize, "only \(recovered.peak) of \(poolSize) permits survived")
    }

    @Test("a connection is checked out of the idle pool exactly once at a time")
    func idleConnectionsAreHandedOutExclusively() async {
        let (pool, _) = makePool(size: 4, creationDelay: .milliseconds(5))
        let exclusivity = ExclusivityChecker()

        await runOnThreads(count: 24) { _ in
            _ = capture {
                try pool.withConnectionSync { connection in
                    exclusivity.use(connection) { blockingSleep(.milliseconds(5)) }
                }
            }
        }

        #expect(exclusivity.violations.isEmpty, "\(exclusivity.violations)")
    }
}
