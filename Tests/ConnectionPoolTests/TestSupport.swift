import Foundation
import Synchronization
import Testing
@testable import ConnectionPool

// MARK: - Mock connection

final class MockConnection: ConnectionPoolConnection, CustomStringConvertible {
    let serial: Int
    private let valid = Mutex<Bool>(true)
    private let closeCount = Mutex<Int>(0)

    init(serial: Int) {
        self.serial = serial
    }

    var isValid: Bool {
        valid.withLock { $0 }
    }

    var isClosed: Bool {
        closeCount.withLock { $0 > 0 }
    }

    var timesClosed: Int {
        closeCount.withLock { $0 }
    }

    func invalidate() {
        valid.withLock { $0 = false }
    }

    func close() {
        closeCount.withLock { $0 += 1 }
    }

    var description: String { "MockConnection(#\(serial))" }
}

// MARK: - Mock provider

enum MockError: Error, Equatable {
    case creationFailed
    case handlerFailed
}

/// Shared, thread safe bookkeeping for `MockProvider`. `MockProvider` is a value type
/// (the pool stores it by value) so all mutable state lives here.
final class ProviderState: Sendable {
    private struct Storage {
        var nextSerial = 1
        var created: [MockConnection] = []
        var pendingFailures = 0
        var syncCalls = 0
        var asyncCalls = 0
    }

    private let storage = Mutex(Storage())

    /// Delay applied inside `newConnection()` to widen races in tests.
    let creationDelay: Duration?

    init(creationDelay: Duration? = nil) {
        self.creationDelay = creationDelay
    }

    var created: [MockConnection] { storage.withLock { $0.created } }
    var createdCount: Int { storage.withLock { $0.created.count } }
    var syncCalls: Int { storage.withLock { $0.syncCalls } }
    var asyncCalls: Int { storage.withLock { $0.asyncCalls } }
    var totalCalls: Int { storage.withLock { $0.syncCalls + $0.asyncCalls } }

    /// Makes the next `count` connection attempts throw `MockError.creationFailed`.
    func failNextCreations(_ count: Int) {
        storage.withLock { $0.pendingFailures += count }
    }

    func make(isAsync: Bool) throws -> MockConnection {
        if let creationDelay {
            Thread.sleep(forTimeInterval: creationDelay.seconds)
        }
        return try storage.withLock { state in
            if isAsync { state.asyncCalls += 1 } else { state.syncCalls += 1 }
            if state.pendingFailures > 0 {
                state.pendingFailures -= 1
                throw MockError.creationFailed
            }
            let connection = MockConnection(serial: state.nextSerial)
            state.nextSerial += 1
            state.created.append(connection)
            return connection
        }
    }
}

struct MockProvider: ConnectionPoolProvider, Sendable {
    let state: ProviderState

    func newConnectionSync() throws -> MockConnection {
        try state.make(isAsync: false)
    }

    func newConnection() async throws -> MockConnection {
        try state.make(isAsync: true)
    }
}

typealias MockPool = ConnectionPool<MockProvider>

func makePool(
    size: Int,
    timeout: Duration = .seconds(180),
    creationDelay: Duration? = nil
) -> (pool: MockPool, state: ProviderState) {
    let state = ProviderState(creationDelay: creationDelay)
    let pool = ConnectionPool(provider: MockProvider(state: state), poolSize: size, timeout: timeout)
    return (pool, state)
}

// MARK: - Concurrency tracking

/// Records how many callers are inside a critical section at once.
final class ConcurrencyTracker: Sendable {
    private struct Storage {
        var current = 0
        var peak = 0
        var total = 0
    }

    private let storage = Mutex(Storage())

    var peak: Int { storage.withLock { $0.peak } }
    var current: Int { storage.withLock { $0.current } }
    var total: Int { storage.withLock { $0.total } }

    func enter() {
        storage.withLock { state in
            state.current += 1
            state.total += 1
            state.peak = max(state.peak, state.current)
        }
    }

    func exit() {
        storage.withLock { $0.current -= 1 }
    }

    func track<T>(_ body: () throws -> T) rethrows -> T {
        enter()
        defer { exit() }
        return try body()
    }

    func track<T>(_ body: () async throws -> T) async rethrows -> T {
        enter()
        defer { exit() }
        return try await body()
    }
}

/// Detects the same connection being handed to two callers at the same time.
final class ExclusivityChecker: Sendable {
    private struct Storage {
        var inUse: Set<Int> = []
        var violations: [String] = []
    }

    private let storage = Mutex(Storage())

    var violations: [String] { storage.withLock { $0.violations } }

    private func acquire(_ serial: Int) {
        storage.withLock { state in
            if !state.inUse.insert(serial).inserted {
                state.violations.append("connection #\(serial) was held by two callers at once")
            }
        }
    }

    private func release(_ serial: Int) {
        storage.withLock { _ = $0.inUse.remove(serial) }
    }

    func use<T>(_ connection: MockConnection, _ body: () throws -> T) rethrows -> T {
        acquire(connection.serial)
        defer { release(connection.serial) }
        return try body()
    }

    func use<T>(_ connection: MockConnection, _ body: () async throws -> T) async rethrows -> T {
        acquire(connection.serial)
        defer { release(connection.serial) }
        return try await body()
    }
}

// MARK: - Collecting results across tasks/threads

final class Collector<Element: Sendable>: Sendable {
    private let storage = Mutex<[Element]>([])

    var values: [Element] { storage.withLock { $0 } }
    var count: Int { storage.withLock { $0.count } }

    func append(_ element: Element) {
        storage.withLock { $0.append(element) }
    }
}

final class Box<Value: Sendable>: Sendable {
    private let storage: Mutex<Value>

    init(_ value: Value) {
        storage = Mutex(value)
    }

    var value: Value {
        get { storage.withLock { $0 } }
        set { storage.withLock { $0 = newValue } }
    }
}

// MARK: - Outcomes

/// A `Sendable` stand-in for `Result` (`any Error` is not `Sendable`).
enum Outcome<Value: Sendable>: Sendable {
    case value(Value)
    case failure(String)

    var value: Value? {
        if case .value(let value) = self { return value }
        return nil
    }

    var failureDescription: String? {
        if case .failure(let description) = self { return description }
        return nil
    }

    var isTimedOutError: Bool {
        failureDescription == String(describing: ConnectionPoolError.timedOut)
    }

    var isClosedError: Bool {
        failureDescription == String(describing: ConnectionPoolError.closed)
    }

    var isCancellationError: Bool {
        failureDescription == String(describing: CancellationError())
    }
}

func capture<Value: Sendable>(_ body: () throws -> Value) -> Outcome<Value> {
    do { return .value(try body()) } catch { return .failure(String(describing: error)) }
}

func capture<Value: Sendable>(_ body: () async throws -> Value) async -> Outcome<Value> {
    do { return .value(try await body()) } catch { return .failure(String(describing: error)) }
}

// MARK: - Watchdogs
//
// Several of the behaviours under test can hang forever when they regress. These helpers
// convert "never returns" into "returns nil after `timeout`" so a defect shows up as a test
// failure instead of wedging the whole suite. The stuck work is deliberately abandoned rather
// than awaited — that is the entire point.

/// Runs `operation` in a detached task and gives up after `timeout`. Returns `nil` on timeout.
func withWatchdog<Value: Sendable>(
    _ timeout: Duration = .seconds(3),
    operation: @escaping @Sendable () async -> Value
) async -> Value? {
    let (stream, continuation) = AsyncStream<Value?>.makeStream()
    let work = Task.detached { continuation.yield(await operation()) }
    let timer = Task.detached {
        try? await Task.sleep(for: timeout)
        continuation.yield(nil)
    }
    defer { timer.cancel() }

    var iterator = stream.makeAsyncIterator()
    let result = await iterator.next() ?? nil
    if result == nil {
        // `work` is stuck; abandon it. Cancelling would not unblock a non-cancellable wait.
        _ = work
    }
    return result
}

/// Runs blocking `operation` on a dedicated `Thread` and gives up after `timeout`.
/// A dedicated thread keeps blocking waits off the cooperative pool.
func withWatchdogOnThread<Value: Sendable>(
    _ timeout: Duration = .seconds(3),
    operation: @escaping @Sendable () -> Value
) async -> Value? {
    let (stream, continuation) = AsyncStream<Value?>.makeStream()
    let thread = Thread { continuation.yield(operation()) }
    thread.name = "watchdog-worker"
    thread.start()
    let timer = Task.detached {
        try? await Task.sleep(for: timeout)
        continuation.yield(nil)
    }
    defer { timer.cancel() }

    var iterator = stream.makeAsyncIterator()
    return await iterator.next() ?? nil
}

// MARK: - Running blocking work off the cooperative pool

/// Runs `body` on `count` dedicated threads and suspends (without blocking) until all finish.
func runOnThreads(count: Int, _ body: @escaping @Sendable (Int) -> Void) async {
    let (stream, continuation) = AsyncStream<Void>.makeStream()
    let remaining = Mutex<Int>(count)

    for index in 0..<count {
        let thread = Thread {
            body(index)
            let done = remaining.withLock { value -> Bool in
                value -= 1
                return value == 0
            }
            if done { continuation.finish() }
        }
        thread.name = "pool-test-worker-\(index)"
        thread.start()
    }

    for await _ in stream {}
}

/// Runs `operation` under a watchdog, recording a test failure instead of hanging forever.
@discardableResult
func expectCompletes<Value: Sendable>(
    within timeout: Duration = .seconds(5),
    _ comment: Comment? = nil,
    sourceLocation: SourceLocation = #_sourceLocation,
    operation: @escaping @Sendable () async -> Value
) async -> Value? {
    let result = await withWatchdog(timeout, operation: operation)
    if result == nil {
        Issue.record(comment ?? "did not complete within \(timeout)", sourceLocation: sourceLocation)
    }
    return result
}

/// Runs `workload` with a deadline. Returns `false` if it did not finish in time; the stuck
/// work is abandoned rather than awaited.
func completes(
    within timeout: Duration = .seconds(10),
    _ workload: @escaping @Sendable () async -> Void
) async -> Bool {
    await withWatchdog(timeout) {
        await workload()
        return true
    } ?? false
}

/// Blocking variant of `expectCompletes`, run on a dedicated thread.
@discardableResult
func expectCompletesOnThread<Value: Sendable>(
    within timeout: Duration = .seconds(5),
    _ comment: Comment? = nil,
    sourceLocation: SourceLocation = #_sourceLocation,
    operation: @escaping @Sendable () -> Value
) async -> Value? {
    let result = await withWatchdogOnThread(timeout, operation: operation)
    if result == nil {
        Issue.record(comment ?? "did not complete within \(timeout)", sourceLocation: sourceLocation)
    }
    return result
}

/// Starts blocking `body` on a dedicated thread *without* waiting for it, so the caller can go on
/// to race something against it. Await the returned closure to collect the result.
func startOnThread<Value: Sendable>(_ body: @escaping @Sendable () -> Value) -> @Sendable () async -> Value {
    let (stream, continuation) = AsyncStream<Value>.makeStream()
    let thread = Thread {
        continuation.yield(body())
        continuation.finish()
    }
    thread.name = "pool-test-racer"
    thread.start()

    return {
        var iterator = stream.makeAsyncIterator()
        return await iterator.next()!
    }
}

/// Runs blocking `body` on one dedicated thread, suspending until it completes.
func onThread<Value: Sendable>(_ body: @escaping @Sendable () -> Value) async -> Value {
    let (stream, continuation) = AsyncStream<Value>.makeStream()
    let thread = Thread {
        continuation.yield(body())
        continuation.finish()
    }
    thread.name = "pool-test-worker"
    thread.start()

    var iterator = stream.makeAsyncIterator()
    return await iterator.next()!
}

// MARK: - Misc

extension Duration {
    var seconds: TimeInterval {
        let (secs, attos) = components
        return TimeInterval(secs) + TimeInterval(attos) / 1e18
    }
}

func blockingSleep(_ duration: Duration) {
    Thread.sleep(forTimeInterval: duration.seconds)
}

/// Runs `body` in a synchronous frame on the current thread, within the current task.
///
/// `withConnectionSync` is marked `noasync`, which rejects *direct* calls from async contexts.
/// Production blocking code reaches it through ordinary synchronous functions; this helper
/// stands in for that pattern in tests. Unlike `onThread` it does not leave the current task,
/// so task-local pins remain visible — which is exactly what the pinning tests exercise.
func inSyncContext<T>(_ body: () throws -> T) rethrows -> T {
    try body()
}
