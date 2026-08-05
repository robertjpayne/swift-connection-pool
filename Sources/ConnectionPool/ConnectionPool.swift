import Foundation
import Dispatch
import Synchronization


/// A connection that can be managed by a ``ConnectionPool``.
///
/// Conforming types are typically wrappers around a database or network connection. The pool
/// hands each connection to at most one caller at a time, so conformances only need to be
/// `Sendable` in the sense of being safe to *transfer* between threads and tasks — not safe
/// for concurrent use.
public protocol ConnectionPoolConnection : Sendable {

    /// Whether the connection is still usable.
    ///
    /// The pool consults this at two points: before handing an idle connection to the next
    /// caller, and when a connection is returned at the end of a
    /// ``ConnectionPool/withConnection(timeout:_:)`` or
    /// ``ConnectionPool/withConnectionSync(timeout:_:)`` scope. A connection that reports
    /// `false` is removed from the pool, closed, and replaced lazily on a later checkout.
    ///
    /// - Note: This property is read while the pool holds an internal lock. Keep it
    ///   cheap — return cached liveness state rather than performing I/O such as a ping.
    var isValid: Bool { get }

    /// Releases the connection's underlying resources.
    ///
    /// The pool calls this at most once per connection: when a dead connection is discarded,
    /// when an idle connection is closed by ``ConnectionPool/close()``, or when a connection
    /// is returned to a pool that has been closed. Connections must not be usable again after
    /// this returns.
    ///
    /// - Note: This is a synchronous requirement invoked from arbitrary threads; if teardown
    ///   involves slow I/O, perform it asynchronously inside your implementation.
    func close()
}

/// A factory that opens new connections on behalf of a ``ConnectionPool``.
///
/// The pool creates connections lazily — only when a caller holds a pool slot and no idle
/// connection is available — and never retries a failed attempt itself; errors thrown here
/// surface directly to the caller that triggered the creation.
///
/// Providers must be `Sendable`: a pool may invoke them from several threads and tasks at
/// once, up to its size.
public protocol ConnectionPoolProvider : Sendable {

    /// The concrete connection type this provider opens.
    associatedtype Connection: ConnectionPoolConnection

    /// Opens a new connection, blocking the calling thread until it is established.
    ///
    /// Called by ``ConnectionPool/withConnectionSync(timeout:_:)``. Both requirements must
    /// produce interchangeable connections — a connection opened here may later be handed to
    /// an `async` caller, and vice versa.
    ///
    /// - Returns: A newly established, valid connection.
    /// - Throws: Any error describing why the connection could not be established.
    func newConnectionSync() throws -> Connection

    /// Opens a new connection without blocking the calling thread.
    ///
    /// Called by ``ConnectionPool/withConnection(timeout:_:)``. Both requirements must
    /// produce interchangeable connections — a connection opened here may later be handed to
    /// a blocking caller, and vice versa.
    ///
    /// - Returns: A newly established, valid connection.
    /// - Throws: Any error describing why the connection could not be established.
    func newConnection() async throws -> Connection
}

/// Errors thrown by ``ConnectionPool`` itself.
///
/// Errors thrown by the ``ConnectionPoolProvider`` or by a caller's handler are rethrown
/// unchanged and are not wrapped in this type. Cancelled `async` callers receive
/// `CancellationError`, not a case of this enum.
public enum ConnectionPoolError : Swift.Error {

    /// The pool was closed — either before the call, or while the caller was waiting for a
    /// connection to become available.
    case closed

    /// No connection became available within the caller's timeout (or the pool's
    /// ``ConnectionPool/defaultTimeout`` when none was given).
    case timedOut
}

/// A fixed-size connection pool that serves blocking and Swift-concurrency callers from one
/// shared set of connections.
///
/// The pool opens at most `poolSize` connections, creating them lazily and reusing idle ones
/// in first-in, first-out order. Callers borrow a connection for the duration of a closure —
/// `async` code uses ``withConnection(timeout:_:)``, blocking code uses
/// ``withConnectionSync(timeout:_:)`` — and both kinds of caller draw from the same pool and
/// wait in a single first-come, first-served queue.
///
/// ```swift
/// let pool = ConnectionPool(provider: MyProvider(), poolSize: 8)
///
/// // From async code:
/// let value = try await pool.withConnection { connection in
///     try await connection.query(...)
/// }
///
/// // From blocking code:
/// let value = try pool.withConnectionSync { connection in
///     try connection.query(...)
/// }
/// ```
///
/// ### Re-entrancy
///
/// While a closure runs, its connection is *pinned* to the surrounding execution context — the
/// current task for `async` callers (inherited by structured child tasks), the current thread
/// for blocking callers. A nested `withConnection`/`withConnectionSync` call in that context
/// reuses the pinned connection instead of waiting for a second one, so re-entrant code cannot
/// deadlock against the pool's own limit.
///
/// ### Connection health
///
/// Idle connections whose ``ConnectionPoolConnection/isValid`` has become `false` are closed
/// and skipped at checkout, and connections that are invalid when their scope ends are closed
/// instead of returned to the pool. The pool never proactively probes health — validity is
/// only consulted at these handoff points.
public final class ConnectionPool<Provider: ConnectionPoolProvider> : @unchecked Sendable {

    /// The connection type managed by this pool, as opened by its `Provider`.
    public typealias Connection = Provider.Connection

    private final class PinnedConnection: Sendable {
        let connection: Connection
        private let active = Atomic<Bool>(true)

        init(_ connection: Connection) {
            self.connection = connection
        }

        var isActive: Bool {
            active.load(ordering: .acquiring)
        }

        func deactivate() {
            active.store(false, ordering: .releasing)
        }
    }

    /// A unique identifier for this pool instance.
    public let id: String

    /// The time a caller waits for a connection when no explicit timeout is passed to
    /// ``withConnection(timeout:_:)`` or ``withConnectionSync(timeout:_:)``.
    public let defaultTimeout: Duration

    /// The factory this pool uses to open new connections.
    public let provider: Provider
    
    private let currentTaskPinnedConnection = TaskLocal<PinnedConnection?>(wrappedValue: nil)
    private let currentThreadPinnedConnectionKey: String
    private let lock = NSRecursiveLock()
    private let semaphore: ConnectionPoolSemaphore
    private var idleConnections: [Connection] = []
    private var closed = false
    
    /// Creates a pool.
    ///
    /// No connections are opened up front; each is created on first demand, up to `poolSize`.
    ///
    /// - Parameters:
    ///   - provider: The factory used to open connections.
    ///   - poolSize: The maximum number of connections that may exist — and therefore the
    ///     maximum number of concurrent borrowers. Must be greater than zero.
    ///   - timeout: The default wait applied when a caller does not pass its own timeout.
    ///     Defaults to 180 seconds.
    /// - Precondition: `poolSize > 0`.
    public init(provider: Provider, poolSize: Int, timeout: Duration = .seconds(180)) {
        precondition(poolSize > 0, "Pool size must be greater than zero")
        let id = UUID().uuidString
        
        self.id = id
        self.currentThreadPinnedConnectionKey = "connection_pool_\(id)_connection"
        self.provider = provider
        self.defaultTimeout = timeout
        self.semaphore = .init(value: poolSize)
    }

    deinit {
        close()
    }

    /// Closes the pool.
    ///
    /// Closing takes effect immediately:
    /// - Callers queued waiting for a connection are woken and thrown
    ///   ``ConnectionPoolError/closed``.
    /// - Subsequent ``withConnection(timeout:_:)`` and ``withConnectionSync(timeout:_:)``
    ///   calls throw ``ConnectionPoolError/closed`` without waiting.
    /// - Idle connections are closed via ``ConnectionPoolConnection/close()``.
    ///
    /// Connections currently in use are not tracked and are left untouched: their scopes keep
    /// running (including nested, pinned reuse) and each connection is closed when its scope
    /// returns it. Calling `close()` more than once has no further effect, and the pool closes
    /// itself automatically when it is deinitialized.
    public func close() {
        let connectionsToClose: [Connection]? = lock.withLock {
            guard !closed else {
                return nil
            }
            closed = true
            let idle = idleConnections
            idleConnections.removeAll()
            return idle
        }

        // already closed
        guard let connectionsToClose else {
            return
        }

        // wake every waiter with a closed result
        semaphore.close()

        // close idle connections outside the lock
        for connection in connectionsToClose {
            connection.close()
        }
    }
    
    /// The connection pinned to the current execution context, if any.
    ///
    /// Inside a ``withConnection(timeout:_:)`` scope this returns that scope's connection for
    /// the current task and its structured child tasks; inside a
    /// ``withConnectionSync(timeout:_:)`` scope it returns the connection for the current
    /// thread. Anywhere else — including detached tasks, unrelated threads, and unstructured
    /// tasks that outlive the scope they were started in — it returns `nil`.
    public var currentConnection: Connection? {
        if let pinnedConnection = currentTaskPinnedConnection.get(), pinnedConnection.isActive {
            return pinnedConnection.connection
        }
        if let result = Thread.current.threadDictionary[currentThreadPinnedConnectionKey] as? Connection {
            return result
        }
        return nil
    }
    
    /// Borrows a connection for the duration of a closure, blocking the calling thread while
    /// it waits.
    ///
    /// If the current context already holds one of this pool's connections — because the call
    /// is nested inside another `withConnectionSync` scope on this thread, or inside a
    /// ``withConnection(timeout:_:)`` scope on the current task — that connection is reused
    /// directly and `timeout` is ignored. Otherwise the calling thread blocks until a
    /// connection is available, taking an idle one or opening a new one via
    /// ``ConnectionPoolProvider/newConnectionSync()``.
    ///
    /// The connection is only valid inside `handler`; do not store or use it after the closure
    /// returns. When the outermost scope ends the connection returns to the pool, or is closed
    /// if it is no longer valid or the pool has been closed.
    ///
    /// - Warning: This method blocks the calling thread and is marked `noasync`: calling it
    ///   directly from an `async` context is a compile-time error — use
    ///   ``withConnection(timeout:_:)`` there instead. The check does not follow calls into
    ///   synchronous functions, so blocking helpers invoked from async code still compile;
    ///   inside such helpers, pinned-connection reuse keeps nested checkouts deadlock-free.
    ///   Avoid running a nested `RunLoop` from `handler` (for example via a modal panel):
    ///   work scheduled onto the blocked thread would observe this scope's pinned connection.
    ///
    /// - Parameters:
    ///   - timeout: The maximum time to wait for a connection. When `nil`, the pool's
    ///     ``defaultTimeout`` applies. The timeout covers only the wait for a connection —
    ///     not connection creation, and not the handler's own runtime.
    ///   - handler: The work to perform with the borrowed connection.
    /// - Returns: Whatever `handler` returns.
    /// - Throws: ``ConnectionPoolError/timedOut`` if no connection became available in time;
    ///   ``ConnectionPoolError/closed`` if the pool is closed before or while waiting; any
    ///   error thrown by ``ConnectionPoolProvider/newConnectionSync()``; and rethrows
    ///   whatever `handler` throws.
    @available(*, noasync, renamed: "withConnection(timeout:_:)", message: "withConnectionSync blocks the calling thread; use withConnection from async contexts")
    public func withConnectionSync<T>(timeout: Duration? = nil, _ handler: (Connection) throws -> T) throws -> T {
        // use any current connection
        if let connection = currentConnection {
            return try handler(connection)
        }
        
        let connection = try checkoutSync(timeout: timeout)
        defer {
            checkin(connection: connection)
        }
        
        Thread.current.threadDictionary[currentThreadPinnedConnectionKey] = connection
        defer {
            Thread.current.threadDictionary[currentThreadPinnedConnectionKey] = nil
        }
        
        return try handler(connection)
    }
    
    /// Borrows a connection for the duration of an `async` closure, suspending — never
    /// blocking a thread — while it waits.
    ///
    /// If the current task already holds one of this pool's connections (including via a
    /// structured parent task), that connection is reused directly and `timeout` is ignored.
    /// Otherwise the call suspends until a connection is available, taking an idle one or
    /// opening a new one via ``ConnectionPoolProvider/newConnection()``.
    ///
    /// While `handler` runs, the connection is pinned to the current task: structured child
    /// tasks inherit it and nested calls reuse it. Detached tasks do not inherit the pin, and
    /// an unstructured `Task` that outlives this scope loses access to it — a later call from
    /// such a task checks out its own connection. The connection is only valid inside
    /// `handler`; do not store or use it after the closure returns. When the outermost scope
    /// ends the connection returns to the pool, or is closed if it is no longer valid or the
    /// pool has been closed.
    ///
    /// - Parameters:
    ///   - timeout: The maximum time to wait for a connection. When `nil`, the pool's
    ///     ``defaultTimeout`` applies. The timeout covers only the wait for a connection —
    ///     not connection creation, and not the handler's own runtime.
    ///   - handler: The work to perform with the borrowed connection.
    /// - Returns: Whatever `handler` returns.
    /// - Throws: `CancellationError` if the task is cancelled before a connection is acquired
    ///   (once `handler` is running, cancellation is the handler's to observe);
    ///   ``ConnectionPoolError/timedOut`` if no connection became available in time;
    ///   ``ConnectionPoolError/closed`` if the pool is closed before or while waiting; any
    ///   error thrown by ``ConnectionPoolProvider/newConnection()``; and rethrows whatever
    ///   `handler` throws.
    public func withConnection<T: Sendable>(timeout: Duration? = nil, _ handler: @escaping @Sendable (Connection) async throws -> T) async throws -> T {
        // use any current connection
        if let connection = currentConnection {
            return try await handler(connection)
        }
        
        let connection = try await checkout(timeout: timeout)
        defer {
            checkin(connection: connection)
        }
        
        let pinnedConnection = PinnedConnection(connection)
        defer {
            pinnedConnection.deactivate()
        }
        
        return try await currentTaskPinnedConnection.withValue(pinnedConnection) {
            try await handler(connection)
        }
    }
    
    private func checkoutSync(timeout: Duration? = nil) throws -> Connection {
        // wait on semaphore
        switch semaphore.wait(timeout: timeout ?? defaultTimeout) {
        case .cancelled:
            throw CancellationError()
        case .timedOut:
            throw ConnectionPoolError.timedOut
        case .closed:
            throw ConnectionPoolError.closed
        case .success:
            break
        }
        
        // attempt to take an idle connection
        if let connection = nextIdleConnection() {
            return connection
        }
        
        // otherwise create a new connection, if this creation fails ensure to free the pool slot
        // before rethrowing the error
        do {
            return try provider.newConnectionSync()
        } catch {
            semaphore.signal()
            throw error
        }
    }

    private func checkout(timeout: Duration? = nil) async throws -> Connection {
        // ensure not cancelled
        guard !Task.isCancelled else {
            throw CancellationError()
        }
        
        // wait on semaphore
        switch await semaphore.wait(timeout: timeout ?? defaultTimeout) {
        case .cancelled:
            throw CancellationError()
        case .timedOut:
            throw ConnectionPoolError.timedOut
        case .closed:
            throw ConnectionPoolError.closed
        case .success:
            break
        }
        
        // attempt to take an idle connection
        if let connection = nextIdleConnection() {
            return connection
        }
        
        // otherwise create a new connection, if this creation fails ensure to free the pool slot
        // before rethrowing the error
        do {
            return try await provider.newConnection()
        } catch {
            semaphore.signal()
            throw error
        }
    }
    
    private func nextIdleConnection() -> Connection? {
        var discarded: [Connection] = []
        let connection: Connection? = lock.withLock {
            while !idleConnections.isEmpty {
                let candidate = idleConnections.removeFirst()
                if candidate.isValid {
                    return candidate
                }
                discarded.append(candidate)
            }
            return nil
        }

        // close discarded connections outside the lock
        for deadConnection in discarded {
            deadConnection.close()
        }
        return connection
    }

    private func checkin(connection: Connection) {
        let shouldCloseConnection: Bool = lock.withLock {
            let pooled = !closed && connection.isValid
            if pooled {
                idleConnections.append(connection)
            }
            semaphore.signal()
            return !pooled
        }

        // dead connections and connections returned after close() are closed, outside the lock
        if shouldCloseConnection {
            connection.close()
        }
    }
}

final class ConnectionPoolSemaphore : @unchecked Sendable {
    enum WaitResult : Equatable, Sendable {
        case success
        case timedOut
        case cancelled
        case closed
    }
    
    private final class Waiter : @unchecked Sendable {
        enum State {
            case pending
            case waitingSync(DispatchSemaphore)
            case waitingAsync(CheckedContinuation<WaitResult, Never>)
            case resolved(WaitResult)
        }
        
        let id: UUID
        var state: State
        
        init() {
            self.id = UUID()
            self.state = .pending
        }
    }
    
    private let lock = NSRecursiveLock()
    private var permits: Int
    private var waiters: [Waiter] = []
    private var closed = false
    
    init(value: Int) {
        precondition(value >= 0, "Semaphore value must be non-negative")
        self.permits = value
    }
    
    // MARK: Signal
    
    func signal() {
        lock.withLock {
            // resolve the first pending waiter and then return
            for (idx, waiter) in waiters.enumerated() {
                guard case .resolved = resolve(waiter, result: .success) else {
                    continue
                }
                waiters.remove(at: idx)
                return
            }
            
            // if no pending waiters increase the permits
            permits += 1
        }
    }

    // MARK: Close

    func close() {
        lock.withLock {
            guard !closed else { return }
            closed = true
            for waiter in waiters {
                resolve(waiter, result: .closed)
            }
            waiters.removeAll()
        }
    }

    // MARK: Wait
    
    func wait(timeout: Duration? = nil) -> WaitResult {
        // setup waiter, unless the semaphore is already closed
        let waiter: Waiter? = lock.withLock {
            guard !closed else { return nil }
            let waiter = Waiter()
            waiters.append(waiter)
            return waiter
        }
        guard let waiter else {
            return .closed
        }
        
        // defer a cleanup
        defer {
            cleanup(waiter)
        }
        
        
        // setup semaphore
        let semaphore = DispatchSemaphore(value: 0)
        
        // attempt to park the waiter for waking
        if case let .resolved(result) = park(waiter, at: .waitingSync(semaphore)) {
            return result
        }
        
        // wait on the semaphore
        guard case .success = semaphore.wait(wallTimeout: timeout?.wallTime ?? .distantFuture) else {
            switch resolve(waiter, result: .timedOut) {
            case .resolved:
                return .timedOut
            case let .alreadyResolved(existingResult):
                return existingResult
            }
        }

        return lock.withLock {
            guard case let .resolved(result) = waiter.state else {
                return .success
            }
            return result
        }
    }
    
    func wait(timeout: Duration? = nil) async -> WaitResult {
        // setup waiter, unless the semaphore is already closed
        let waiter: Waiter? = lock.withLock {
            guard !closed else { return nil }
            let waiter = Waiter()
            waiters.append(waiter)
            return waiter
        }
        guard let waiter else {
            return .closed
        }
        
        // defer a cleanup
        defer {
            cleanup(waiter)
        }
        
        // check if a fast acquire is possible
        if acquire(waiter) == .acquired {
            return .success
        }

        // start the timeout clock, if any
        let timeoutTask: Task<Void, Never>? = timeout.map { timeout in
            Task {
                try? await Task.sleep(for: timeout)
                guard !Task.isCancelled else {
                    return
                }
                self.resolve(waiter, result: .timedOut)
            }
        }
        defer {
            timeoutTask?.cancel()
        }

        // park the waiter and suspend; a signal, the timeout, close(), or cancellation of the
        // surrounding task resolves it, and the resolve state machine guarantees the
        // continuation is resumed exactly once
        return await withTaskCancellationHandler {
            await withCheckedContinuation { (continuation: CheckedContinuation<WaitResult, Never>) in
                if case let .resolved(result) = self.park(waiter, at: .waitingAsync(continuation)) {
                    continuation.resume(returning: result)
                }
            }
        } onCancel: {
            self.resolve(waiter, result: .cancelled)
        }
    }
    
    // MARK: Park
    
    private enum ParkResult : Sendable {
        case parked
        case resolved(WaitResult)
    }
    
    /// attempts to park a waiter to await ensuring that it's not already resolved and no permit is available
    private func park(_ waiter: Waiter, at state: Waiter.State) -> ParkResult {
        lock.withLock {
            if case .resolved(let result) = waiter.state {
                return .resolved(result)
            }

            // check if a fast acquire is possible
            if acquire(waiter) == .acquired {
                return .resolved(.success)
            }
            
            waiter.state = state
            return .parked
        }
    }
    
    // MARK: Resolve
    
    private enum ResolveResult : Sendable {
        case resolved
        case alreadyResolved(existingResult: WaitResult)
    }
    
    @discardableResult
    private func resolve(_ waiter: Waiter, result: WaitResult) -> ResolveResult {
        lock.withLock {
            switch waiter.state {
            case .pending:
                waiter.state = .resolved(result)
                return .resolved
            case .waitingSync(let semaphore):
                waiter.state = .resolved(result)
                semaphore.signal()
                return .resolved
            case .waitingAsync(let continuation):
                waiter.state = .resolved(result)
                continuation.resume(returning: result)
                return .resolved
            case .resolved(let existingResult):
                return .alreadyResolved(existingResult: existingResult)
            }
        }
    }
    
    // MARK: Acquire
    
    private enum AcquireResult : Sendable {
        case acquired
        case waitRequired
    }
    
    private func acquire(_ waiter: Waiter) -> AcquireResult {
        lock.withLock {
            // ensure there is at least one permit to acquire
            guard permits > 0 else {
                return .waitRequired
            }
            
            // only consume the permit if this call is the one that resolved the waiter
            guard case .resolved = resolve(waiter, result: .success) else {
                return .waitRequired
            }

            // consume the permit
            permits -= 1
            return .acquired
        }
    }
    
    // MARK: Cleanup
    
    private func cleanup(_ waiter: Waiter) {
        lock.withLock {
            waiters.removeAll(where: { $0.id == waiter.id })
        }
    }
}

private extension Duration {
    /// Clamped conversion for `DispatchWallTime` arithmetic.
    ///
    /// Both components matter: reading only `attoseconds` discards every whole second, which turns
    /// a `.seconds(180)` timeout into an immediate one.
    var wallTime: DispatchWallTime {
        let (seconds, attoseconds) = components
        guard seconds >= 0 else { return .now() }
        guard seconds < Int64(Int.max) / 1_000_000_000 else { return .distantFuture }
        return .now() + .nanoseconds(Int(seconds) * 1_000_000_000 + Int(attoseconds / 1_000_000_000))
    }
}
