import Foundation
import Testing
@testable import ConnectionPool

/// Single-caller behaviour: hand out a connection, run the handler, put the connection back.
@Suite("Checkout and reuse", .timeLimit(.minutes(1)))
struct CheckoutTests {

    @Test("sync withConnection supplies a connection and returns the handler's result")
    func syncSuppliesConnection() throws {
        let (pool, state) = makePool(size: 2)

        let serial = try pool.withConnectionSync { connection in
            #expect(connection.isValid)
            return connection.serial
        }

        #expect(serial == 1)
        #expect(state.createdCount == 1)
        #expect(state.syncCalls == 1)
        #expect(state.asyncCalls == 0)
    }

    @Test("async withConnection supplies a connection and returns the handler's result")
    func asyncSuppliesConnection() async throws {
        let (pool, state) = makePool(size: 2)

        let serial = try await pool.withConnection { connection in
            #expect(connection.isValid)
            return connection.serial
        }

        #expect(serial == 1)
        #expect(state.createdCount == 1)
        #expect(state.syncCalls == 0)
        #expect(state.asyncCalls == 1)
    }

    @Test("sequential sync calls reuse the idle connection")
    func syncReusesIdleConnection() throws {
        let (pool, state) = makePool(size: 4)

        let first = try pool.withConnectionSync { $0 }
        let second = try pool.withConnectionSync { $0 }
        let third = try pool.withConnectionSync { $0 }

        #expect(first === second)
        #expect(second === third)
        #expect(state.createdCount == 1)
    }

    @Test("sequential async calls reuse the idle connection")
    func asyncReusesIdleConnection() async throws {
        let (pool, state) = makePool(size: 4)

        let first = try await pool.withConnection { $0 }
        let second = try await pool.withConnection { $0 }

        #expect(first === second)
        #expect(state.createdCount == 1)
    }

    @Test("the sync and async paths share one idle pool")
    func syncAndAsyncShareIdleConnections() async throws {
        let (pool, state) = makePool(size: 4)

        let fromSync = try inSyncContext { try pool.withConnectionSync { $0 } }
        let fromAsync = try await pool.withConnection { $0 }
        let backToSync = try inSyncContext { try pool.withConnectionSync { $0 } }

        #expect(fromSync === fromAsync)
        #expect(fromAsync === backToSync)
        #expect(state.createdCount == 1, "an idle connection should be reused regardless of which path returned it")
    }

    @Test("an invalidated connection is not returned to the idle pool (sync)")
    func syncDiscardsInvalidatedConnection() throws {
        let (pool, state) = makePool(size: 2)

        let first = try pool.withConnectionSync { connection -> MockConnection in
            connection.invalidate()
            return connection
        }
        let second = try pool.withConnectionSync { $0 }

        #expect(first !== second)
        #expect(first.isClosed, "a discarded connection must be closed")
        #expect(state.createdCount == 2)
    }

    @Test("an invalidated connection is not returned to the idle pool (async)")
    func asyncDiscardsInvalidatedConnection() async throws {
        let (pool, state) = makePool(size: 2)

        let first = try await pool.withConnection { connection -> MockConnection in
            connection.invalidate()
            return connection
        }
        let second = try await pool.withConnection { $0 }

        #expect(first !== second)
        #expect(first.isClosed, "a discarded connection must be closed")
        #expect(state.createdCount == 2)
    }

    @Test("a throwing sync handler propagates its error and still returns the connection")
    func syncHandlerErrorReleasesConnection() throws {
        let (pool, state) = makePool(size: 1)

        #expect(throws: MockError.handlerFailed) {
            try pool.withConnectionSync { _ in throw MockError.handlerFailed }
        }

        // If the permit or the connection had leaked, this call would block for the
        // hard-coded 180 s floor instead of returning immediately.
        let connection = try pool.withConnectionSync { $0 }
        #expect(connection.serial == 1)
        #expect(state.createdCount == 1)
    }

    @Test("a throwing async handler propagates its error and still returns the connection")
    func asyncHandlerErrorReleasesConnection() async throws {
        let (pool, state) = makePool(size: 1)

        await #expect(throws: MockError.handlerFailed) {
            try await pool.withConnection { _ in throw MockError.handlerFailed }
        }

        let connection = try await pool.withConnection { $0 }
        #expect(connection.serial == 1)
        #expect(state.createdCount == 1)
    }

    @Test("no connection is pinned outside of a withConnection scope")
    func connectionIsUnpinnedAfterCall() async throws {
        let (pool, _) = makePool(size: 2)

        #expect(pool.currentConnection == nil)

        try inSyncContext {
            try pool.withConnectionSync { _ in
                #expect(pool.currentConnection != nil)
            }
        }
        #expect(pool.currentConnection == nil, "the thread-local pin must be cleared when the sync call unwinds")

        try await pool.withConnection { _ in
            #expect(pool.currentConnection != nil)
        }
        #expect(pool.currentConnection == nil, "the task-local pin must be cleared when the async call unwinds")
    }

    @Test("a pin is cleared even when the handler throws")
    func connectionIsUnpinnedAfterThrowingHandler() async throws {
        let (pool, _) = makePool(size: 2)

        _ = capture { try pool.withConnectionSync { _ in throw MockError.handlerFailed } }
        #expect(pool.currentConnection == nil)

        _ = await capture { try await pool.withConnection { _ in throw MockError.handlerFailed } }
        #expect(pool.currentConnection == nil)
    }

    @Test("each pool instance pins independently")
    func pinsAreScopedToTheirPool() throws {
        let (poolA, _) = makePool(size: 1)
        let (poolB, _) = makePool(size: 1)

        try poolA.withConnectionSync { connectionA in
            try poolB.withConnectionSync { connectionB in
                #expect(connectionA !== connectionB)
                #expect(poolA.currentConnection === connectionA)
                #expect(poolB.currentConnection === connectionB)
            }
            #expect(poolB.currentConnection == nil)
            #expect(poolA.currentConnection === connectionA, "the inner pool must not clear the outer pool's pin")
        }
    }
}
