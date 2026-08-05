import Foundation
import Testing
@testable import ConnectionPool

/// Closing the pool: reject new checkouts, wake queued waiters with `ConnectionPoolError.closed`,
/// and close idle connections — without touching connections currently in use.
@Suite("Pool close", .timeLimit(.minutes(1)))
struct CloseTests {

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

    @Test("a closed pool rejects async checkouts immediately")
    func closedPoolRejectsAsyncCheckouts() async {
        let (pool, state) = makePool(size: 2)
        pool.close()

        let outcome = await expectCompletes(within: .seconds(3)) {
            await capture { try await pool.withConnection { $0.serial } }
        }
        #expect(outcome?.isClosedError == true, "expected ConnectionPoolError.closed, got \(String(describing: outcome))")
        #expect(state.createdCount == 0, "a closed pool must not open connections")
    }

    @Test("a closed pool rejects blocking checkouts immediately")
    func closedPoolRejectsSyncCheckouts() async {
        let (pool, state) = makePool(size: 2)
        pool.close()

        let outcome = await expectCompletesOnThread(within: .seconds(3)) {
            capture { try pool.withConnectionSync { $0.serial } }
        }
        #expect(outcome?.isClosedError == true, "expected ConnectionPoolError.closed, got \(String(describing: outcome))")
        #expect(state.createdCount == 0, "a closed pool must not open connections")
    }

    @Test("close wakes queued sync and async waiters with ConnectionPoolError.closed")
    func closeWakesQueuedWaiters() async {
        let (pool, _) = makePool(size: 1)
        let holder = await holdOnlyConnection(of: pool)

        let asyncWaiter = Task {
            await capture { try await pool.withConnection { $0.serial } }
        }
        let syncWaiter = startOnThread {
            capture { try pool.withConnectionSync { $0.serial } }
        }
        try? await Task.sleep(for: .milliseconds(100))    // let both queue behind the holder

        pool.close()

        let asyncOutcome = await expectCompletes(within: .seconds(3)) { await asyncWaiter.value }
        let syncOutcome = await expectCompletes(within: .seconds(3)) { await syncWaiter() }
        #expect(asyncOutcome?.isClosedError == true, "a queued async waiter was not woken by close: \(String(describing: asyncOutcome))")
        #expect(syncOutcome?.isClosedError == true, "a queued blocking waiter was not woken by close: \(String(describing: syncOutcome))")

        holder.cancel()
        await holder.value
    }

    @Test("close closes idle connections")
    func closeClosesIdleConnections() async {
        let (pool, state) = makePool(size: 2)

        // fill the idle list by running the whole pool concurrently
        await withTaskGroup(of: Void.self) { group in
            for _ in 0..<2 {
                group.addTask {
                    _ = await capture {
                        try await pool.withConnection { _ in
                            try? await Task.sleep(for: .milliseconds(50))
                        }
                    }
                }
            }
        }
        #expect(state.createdCount == 2)
        #expect(state.created.allSatisfy { !$0.isClosed }, "pooled connections must not be closed while the pool is open")

        pool.close()

        #expect(state.created.allSatisfy { $0.isClosed }, "idle connections must be closed when the pool closes")
    }

    @Test("a connection in use survives close and is closed when checked back in")
    func inUseConnectionIsClosedOnCheckin() async {
        let (pool, state) = makePool(size: 1)

        let outcome = await expectCompletes(within: .seconds(3)) {
            await capture {
                try await pool.withConnection { connection in
                    pool.close()

                    // close() must not touch a connection that is currently checked out
                    #expect(!connection.isClosed, "close() touched a connection in use")
                    #expect(connection.isValid)

                    // the scope's pin keeps working after close
                    let nested = try await pool.withConnection { $0 }
                    return nested === connection
                }
            }
        }

        #expect(outcome?.value == true, "a held pin must keep working after close")
        #expect(state.created.first?.isClosed == true, "the connection must be closed once its scope returns it")
    }

    @Test("deinit closes the pool")
    func deinitClosesThePool() async throws {
        let state = ProviderState()
        var pool: MockPool? = ConnectionPool(provider: MockProvider(state: state), poolSize: 2)
        _ = try await pool?.withConnection { $0.serial }    // leaves one idle connection
        #expect(state.created.first?.isClosed == false)

        pool = nil

        #expect(state.created.first?.isClosed == true, "dropping the pool must close its idle connections")
    }

    @Test("closing twice is harmless and closes each connection once")
    func doubleCloseIsHarmless() async throws {
        let (pool, state) = makePool(size: 1)
        _ = try await pool.withConnection { $0.serial }    // leaves one idle connection

        pool.close()
        pool.close()

        #expect(state.created.first?.timesClosed == 1, "an idle connection was closed more than once")

        let outcome = await capture { try await pool.withConnection { $0.serial } }
        #expect(outcome.isClosedError == true)
    }
}
