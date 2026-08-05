import Foundation
import Testing
@testable import ConnectionPool

/// The pool pins a checked-out connection to its execution context — task-local storage for
/// `async` callers, thread-local storage for blocking callers — so re-entrant work reuses the
/// connection it already holds instead of deadlocking against the pool's own limit.
@Suite("Connection pinning", .timeLimit(.minutes(1)))
struct PinningTests {

    @Test("nested blocking calls reuse the caller's connection")
    func nestedSyncReusesConnection() async {
        let (pool, state) = makePool(size: 1)

        let outcome = await expectCompletesOnThread("re-entrant sync call deadlocked against the pool") {
            capture {
                try pool.withConnectionSync { outer in
                    try pool.withConnectionSync { middle in
                        try pool.withConnectionSync { inner in
                            outer === middle && middle === inner
                        }
                    }
                }
            }
        }

        #expect(outcome?.value == true)
        #expect(state.createdCount == 1, "a re-entrant call must not open a second connection")
    }

    @Test("nested async calls reuse the caller's connection")
    func nestedAsyncReusesConnection() async {
        let (pool, state) = makePool(size: 1)

        let outcome = await expectCompletes("re-entrant async call deadlocked against the pool") {
            await capture {
                try await pool.withConnection { outer in
                    try await pool.withConnection { inner in
                        outer === inner
                    }
                }
            }
        }

        #expect(outcome?.value == true)
        #expect(state.createdCount == 1)
    }

    @Test("blocking code nested inside an async scope sees the task-local pin")
    func syncInsideAsyncReusesConnection() async {
        let (pool, state) = makePool(size: 1)

        let outcome = await expectCompletes("sync call inside an async scope did not see the task-local pin") {
            await capture {
                try await pool.withConnection { outer in
                    try inSyncContext {
                        try pool.withConnectionSync { inner in
                            outer === inner
                        }
                    }
                }
            }
        }

        #expect(outcome?.value == true)
        #expect(state.createdCount == 1)
    }

    @Test("a structured child task inherits the pinned connection")
    func childTaskInheritsPin() async {
        let (pool, state) = makePool(size: 1)

        let outcome = await expectCompletes("a child task could not reuse its parent's connection") {
            await capture {
                try await pool.withConnection { outer in
                    await withTaskGroup(of: Bool.self) { group in
                        group.addTask { pool.currentConnection === outer }
                        group.addTask {
                            let nested = try? await pool.withConnection { $0 }
                            return nested === outer
                        }
                        return await group.allSatisfy { $0 }
                    }
                }
            }
        }

        #expect(outcome?.value == true)
        #expect(state.createdCount == 1)
    }

    @Test("a detached task does not inherit the pin and checks out its own connection")
    func detachedTaskDoesNotInheritPin() async {
        let (pool, state) = makePool(size: 2)

        let outcome = await expectCompletes {
            await capture {
                try await pool.withConnection { outer -> Bool in
                    let detached = Task.detached { try await pool.withConnection { $0 } }
                    return try await detached.value !== outer
                }
            }
        }

        #expect(outcome?.value == true)
        #expect(state.createdCount == 2, "a detached task must take its own permit")
    }

    @Test("a task-local pin is invisible to unrelated tasks")
    func pinIsInvisibleToUnrelatedTasks() async {
        let (pool, _) = makePool(size: 2)

        let outcome = await expectCompletes {
            await capture {
                try await pool.withConnection { _ in
                    await Task.detached { pool.currentConnection == nil }.value
                }
            }
        }

        #expect(outcome?.value == true)
    }

    @Test("a thread-local pin is invisible to other threads")
    func pinIsInvisibleToOtherThreads() async {
        let (pool, _) = makePool(size: 2)
        let unpinnedElsewhere = Box<Bool?>(nil)

        await expectCompletesOnThread {
            capture {
                try pool.withConnectionSync { _ in
                    let done = DispatchSemaphore(value: 0)
                    let observer = Thread {
                        unpinnedElsewhere.value = pool.currentConnection == nil
                        done.signal()
                    }
                    observer.start()
                    done.wait()
                }
            }
        }

        #expect(unpinnedElsewhere.value == true)
    }

    /// `TaskLocal` is normally required to be a `static`/global `@TaskLocal`; this pool holds one
    /// per instance. That works because `TaskLocal` is a class and the runtime keys storage on
    /// the object's identity — but it is worth pinning down, since two pools alive at once must
    /// not share or clobber each other's slot.
    @Test("each pool instance pins independently in async contexts")
    func asyncPinsAreScopedToTheirPool() async {
        let (poolA, _) = makePool(size: 1)
        let (poolB, _) = makePool(size: 1)

        let outcome = await expectCompletes {
            await capture {
                try await poolA.withConnection { connectionA in
                    let distinct = try await poolB.withConnection { connectionB in
                        connectionA !== connectionB
                            && poolA.currentConnection === connectionA
                            && poolB.currentConnection === connectionB
                    }
                    // Leaving pool B's scope must restore, not erase, the surrounding state.
                    return distinct
                        && poolB.currentConnection == nil
                        && poolA.currentConnection === connectionA
                }
            }
        }

        #expect(outcome?.value == true)
    }

    @Test("overlapping async callers get different connections")
    func overlappingTasksGetDistinctConnections() async {
        let (pool, _) = makePool(size: 2)
        let serials = Collector<Int>()

        await withTaskGroup(of: Void.self) { group in
            for _ in 0..<2 {
                group.addTask {
                    _ = await capture {
                        try await pool.withConnection { connection in
                            serials.append(connection.serial)
                            try? await Task.sleep(for: .milliseconds(200))
                        }
                    }
                }
            }
        }

        #expect(Set(serials.values).count == 2, "overlapping callers shared a connection: \(serials.values)")
    }

    @Test("overlapping blocking callers get different connections")
    func overlappingThreadsGetDistinctConnections() async {
        let (pool, _) = makePool(size: 2)
        let serials = Collector<Int>()

        await runOnThreads(count: 2) { _ in
            _ = capture {
                try pool.withConnectionSync { connection in
                    serials.append(connection.serial)
                    blockingSleep(.milliseconds(200))
                }
            }
        }

        #expect(Set(serials.values).count == 2, "overlapping callers shared a connection: \(serials.values)")
    }

    @Test("a nested scope does not release the connection when it unwinds")
    func nestedScopeDoesNotReleaseEarly() async {
        let (pool, _) = makePool(size: 1)
        let tracker = ConcurrencyTracker()

        await withTaskGroup(of: Void.self) { group in
            // Holds the pool's only connection for ~300 ms, with a nested checkout in the middle.
            group.addTask {
                _ = await capture {
                    try await pool.withConnection { _ in
                        await tracker.track {
                            try? await Task.sleep(for: .milliseconds(100))
                            _ = try? await pool.withConnection { $0.serial }
                            try? await Task.sleep(for: .milliseconds(200))
                        }
                    }
                }
            }
            // Arrives while the nested scope is unwinding.
            group.addTask {
                try? await Task.sleep(for: .milliseconds(150))
                _ = await capture {
                    try await pool.withConnection { _ in
                        await tracker.track { try? await Task.sleep(for: .milliseconds(50)) }
                    }
                }
            }
        }

        #expect(tracker.total == 2)
        #expect(tracker.peak == 1, "the nested scope released the connection while the outer scope still held it")
    }
}
