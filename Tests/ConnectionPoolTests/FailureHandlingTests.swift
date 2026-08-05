import Foundation
import Testing
@testable import ConnectionPool

/// What the pool does when things go wrong: timeouts, cancellation, providers that fail, and
/// connections that die while nobody is looking.
///
/// Every test here is watchdogged. These paths fail by hanging rather than by returning something
/// wrong, and a hang would otherwise wedge the whole run instead of reporting a failure.
@Suite("Failure handling", .timeLimit(.minutes(1)))
struct FailureHandlingTests {

    /// Occupies the pool's only permit until the returned task is cancelled.
    private func holdOnlyConnection(of pool: MockPool) async -> Task<Void, Never> {
        let ready = Box<Bool>(false)
        let holder = Task {
            _ = await capture {
                try await pool.withConnection { _ in
                    ready.value = true
                    try? await Task.sleep(for: .seconds(60))
                }
            }
        }
        while !ready.value {
            try? await Task.sleep(for: .milliseconds(10))
        }
        return holder
    }

    // MARK: - Waiting

    @Test("a queued async caller resumes as soon as a permit is free")
    func signalledAsyncWaiterResumesPromptly() async {
        let (pool, _) = makePool(size: 1)

        // Holds the only permit for 100 ms, then releases it.
        let holder = Task {
            _ = await capture {
                try await pool.withConnection { _ in
                    try? await Task.sleep(for: .milliseconds(100))
                }
            }
        }
        try? await Task.sleep(for: .milliseconds(20))

        let outcome = await expectCompletes(within: .seconds(3)) {
            await capture { try await pool.withConnection { $0.serial } }
        }

        #expect(outcome?.value == 1, "a queued async caller was not resumed when the permit was released")
        await holder.value
    }

    // MARK: - Timeouts

    @Test("a blocking caller's timeout is honoured")
    func syncTimeoutIsHonoured() async {
        let (pool, _) = makePool(size: 1)
        let holder = await holdOnlyConnection(of: pool)
        defer { holder.cancel() }

        let started = ContinuousClock.now
        let outcome = await expectCompletesOnThread(within: .seconds(3)) {
            capture { try pool.withConnectionSync(timeout: .milliseconds(200)) { $0.serial } }
        }
        let elapsed = ContinuousClock.now - started

        #expect(outcome?.isTimedOutError == true, "expected ConnectionPoolError.timedOut, got \(String(describing: outcome))")
        #expect(elapsed < .seconds(1), "the timeout fired after \(elapsed), not the requested 200 ms")
    }

    @Test("an async caller's timeout is honoured")
    func asyncTimeoutIsHonoured() async {
        let (pool, _) = makePool(size: 1)
        let holder = await holdOnlyConnection(of: pool)
        defer { holder.cancel() }

        let started = ContinuousClock.now
        let outcome = await expectCompletes(within: .seconds(3)) {
            await capture { try await pool.withConnection(timeout: .milliseconds(200)) { $0.serial } }
        }
        let elapsed = ContinuousClock.now - started

        #expect(outcome?.isTimedOutError == true, "expected ConnectionPoolError.timedOut, got \(String(describing: outcome))")
        #expect(elapsed < .seconds(1), "the timeout fired after \(elapsed), not the requested 200 ms")
    }

    @Test("a timed-out waiter does not take a permit with it")
    func timedOutWaiterDoesNotLeakAPermit() async {
        let (pool, _) = makePool(size: 1)
        let holder = await holdOnlyConnection(of: pool)

        // Several callers queue and give up.
        for _ in 0..<3 {
            let outcome = await expectCompletes(within: .seconds(3)) {
                await capture { try await pool.withConnection(timeout: .milliseconds(100)) { $0.serial } }
            }
            #expect(outcome?.isTimedOutError == true)
        }

        // Releasing the holder must leave the pool exactly as capable as it started.
        holder.cancel()
        await holder.value

        let outcome = await expectCompletes(within: .seconds(3)) {
            await capture { try await pool.withConnection { $0.serial } }
        }
        #expect(outcome?.value == 1, "the pool lost a permit to a waiter that had already timed out")
    }

    @Test("the pool's default timeout applies when the caller does not give one")
    func defaultTimeoutApplies() async {
        let (pool, _) = makePool(size: 1, timeout: .milliseconds(200))
        let holder = await holdOnlyConnection(of: pool)
        defer { holder.cancel() }

        let outcome = await expectCompletes(within: .seconds(3)) {
            await capture { try await pool.withConnection { $0.serial } }
        }
        #expect(outcome?.isTimedOutError == true)
    }

    // MARK: - Cancellation

    @Test("cancelling a waiting task unblocks it with a CancellationError")
    func cancellingAWaiterUnblocksIt() async {
        let (pool, _) = makePool(size: 1)
        let holder = await holdOnlyConnection(of: pool)
        defer { holder.cancel() }

        let waiter = Task {
            await capture { try await pool.withConnection { $0.serial } }
        }
        try? await Task.sleep(for: .milliseconds(200))
        waiter.cancel()

        let outcome = await expectCompletes(within: .seconds(3)) { await waiter.value }
        #expect(outcome != nil, "a cancelled waiter never resumed")
        #expect(outcome?.isCancellationError == true, "expected CancellationError, got \(String(describing: outcome))")
    }

    @Test("a cancelled waiter does not take a permit with it")
    func cancelledWaiterDoesNotLeakAPermit() async {
        let (pool, _) = makePool(size: 1)
        let holder = await holdOnlyConnection(of: pool)

        for _ in 0..<3 {
            let waiter = Task {
                await capture { try await pool.withConnection { $0.serial } }
            }
            try? await Task.sleep(for: .milliseconds(50))
            waiter.cancel()
            _ = await expectCompletes(within: .seconds(3)) { await waiter.value }
        }

        holder.cancel()
        await holder.value

        let outcome = await expectCompletes(within: .seconds(3)) {
            await capture { try await pool.withConnection { $0.serial } }
        }
        #expect(outcome?.value == 1, "the pool lost a permit to a cancelled waiter")
    }

    // MARK: - Provider failures

    @Test("a failed blocking connection attempt returns its permit")
    func failedSyncCreationReturnsPermit() async {
        let (pool, state) = makePool(size: 1)
        state.failNextCreations(1)

        let failure = await expectCompletesOnThread(within: .seconds(3)) {
            capture { try pool.withConnectionSync { $0.serial } }
        }
        #expect(failure?.failureDescription == String(describing: MockError.creationFailed))

        let outcome = await expectCompletesOnThread(within: .seconds(3)) {
            capture { try pool.withConnectionSync { $0.serial } }
        }
        #expect(outcome?.value == 1, "the pool never recovered from a failed connection attempt")
    }

    @Test("a failed async connection attempt returns its permit")
    func failedAsyncCreationReturnsPermit() async {
        let (pool, state) = makePool(size: 1)
        state.failNextCreations(1)

        let failure = await expectCompletes(within: .seconds(3)) {
            await capture { try await pool.withConnection { $0.serial } }
        }
        #expect(failure?.failureDescription == String(describing: MockError.creationFailed))

        let outcome = await expectCompletes(within: .seconds(3)) {
            await capture { try await pool.withConnection { $0.serial } }
        }
        #expect(outcome?.value == 1, "the pool never recovered from a failed connection attempt")
    }

    @Test("a pool survives losing every permit to failed connection attempts")
    func poolSurvivesExhaustingEveryPermit() async {
        let poolSize = 3
        let (pool, state) = makePool(size: poolSize)
        state.failNextCreations(poolSize)

        await runOnThreads(count: poolSize) { _ in
            _ = capture { try pool.withConnectionSync { $0.serial } }
        }

        let outcome = await expectCompletesOnThread(within: .seconds(3)) {
            capture { try pool.withConnectionSync { $0.serial } }
        }
        #expect(outcome?.value != nil, "a pool of \(poolSize) died after \(poolSize) failed connection attempts")
    }

    // MARK: - Connection health

    @Test("an idle connection is validated before being handed out again")
    func idleConnectionIsValidatedOnCheckout() async throws {
        let (pool, state) = makePool(size: 2)

        let first = try await pool.withConnection { $0 }
        #expect(state.createdCount == 1)

        // The peer drops the connection while it is sitting in the idle list — the common case
        // for an idle-timed-out database or HTTP connection.
        first.invalidate()

        let second = try await pool.withConnection { $0 }
        #expect(second !== first)
        #expect(second.isValid)
        #expect(first.isClosed, "a dead idle connection must be closed when discarded")
        #expect(state.createdCount == 2)
    }

    @Test("several dead idle connections are skipped in one checkout")
    func severalDeadIdleConnectionsAreSkipped() async throws {
        let poolSize = 4
        let (pool, state) = makePool(size: poolSize)

        // Fill the idle list by running the whole pool concurrently.
        let tracker = ConcurrencyTracker()
        await withTaskGroup(of: Void.self) { group in
            for _ in 0..<poolSize {
                group.addTask {
                    _ = await capture {
                        try await pool.withConnection { _ in
                            await tracker.track { try? await Task.sleep(for: .milliseconds(50)) }
                        }
                    }
                }
            }
        }
        #expect(state.createdCount == poolSize)

        // Every pooled connection dies while idle.
        for connection in state.created {
            connection.invalidate()
        }

        let fresh = try await pool.withConnection { $0 }
        #expect(fresh.isValid, "a dead connection was handed out")
        #expect(state.createdCount == poolSize + 1, "the dead idle connections should have been discarded, not reused")
        #expect(state.created.prefix(poolSize).allSatisfy { $0.isClosed }, "every dead idle connection must be closed when discarded")
    }

    // MARK: - Pin lifetime

    @Test("an unstructured task cannot use a pinned connection after the scope ends")
    func pinDoesNotEscapeItsScope() async {
        let (pool, _) = makePool(size: 1)
        let tracker = ConcurrencyTracker()
        let exclusivity = ExclusivityChecker()
        let escapee = Box<Task<Void, Never>?>(nil)

        // The handler starts an unstructured task, which copies the enclosing task-local values
        // — including the pin — and then outlives this scope.
        _ = await capture {
            try await pool.withConnection { _ in
                escapee.value = Task {
                    try? await Task.sleep(for: .milliseconds(300))
                    _ = await capture {
                        try await pool.withConnection { connection in
                            await exclusivity.use(connection) {
                                await tracker.track { try? await Task.sleep(for: .milliseconds(400)) }
                            }
                        }
                    }
                }
            }
        }

        // Meanwhile a well-behaved caller takes the pool's only permit.
        let competitor = Task {
            try? await Task.sleep(for: .milliseconds(400))
            _ = await capture {
                try await pool.withConnection { connection in
                    await exclusivity.use(connection) {
                        await tracker.track { try? await Task.sleep(for: .milliseconds(400)) }
                    }
                }
            }
        }

        await escapee.value?.value
        await competitor.value

        #expect(tracker.total == 2, "both callers should have run")
        #expect(tracker.peak <= 1, "peak concurrency was \(tracker.peak) on a pool of size 1")
        #expect(exclusivity.violations.isEmpty, "\(exclusivity.violations)")
    }
}
