# ConnectionPool

A fixed-size Swift connection pool that serves **blocking and `async` callers from one shared set of connections** — at the same time.

Most pools force a choice: block threads, or require Swift concurrency everywhere. `ConnectionPool` maintains a single budget of connections and a single first-come, first-served wait queue that both kinds of caller share, so legacy synchronous code and modern `async` code can borrow from the same pool without stepping on each other.

## Features

- **Two front doors, one pool** — `withConnection` suspends without blocking a thread; `withConnectionSync` blocks the calling thread. Both draw from the same connections, the same size limit, and one FIFO queue.
- **Re-entrant by design** — a borrowed connection is *pinned* to its execution context (the current task for `async` callers, inherited by structured child tasks; the current thread for blocking callers). Nested calls reuse the pinned connection instead of deadlocking against the pool's own limit.
- **Lazy and self-healing** — connections are opened on demand, never more than `poolSize`. Idle connections are validity-checked at every handoff; dead ones are closed and replaced transparently.
- **Timeouts and cancellation** — waits are bounded by a per-call or pool-default timeout, and cancelling a waiting task wakes it immediately with `CancellationError`.
- **Deterministic shutdown** — `close()` wakes every queued waiter, rejects new checkouts, and closes idle connections. In-use connections finish their scope and are closed on return. The pool also closes itself on `deinit`.
- **Swift 6 strict concurrency**, exercised by a race-focused, ThreadSanitizer-clean test suite.

## Requirements

- Swift 6 (tools 6.3)
- macOS 26 / iOS 26

## Installation

Add the package to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://example.com/your/connection-pool.git", from: "0.1.0"),
],
targets: [
    .target(
        name: "MyApp",
        dependencies: [
            .product(name: "ConnectionPool", package: "connection-pool"),
        ]
    ),
]
```

## Usage

### 1. Describe your connection and how to open one

```swift
import ConnectionPool

final class DatabaseConnection: ConnectionPoolConnection {
    // Keep this cheap — the pool reads it while holding an internal lock.
    var isValid: Bool { !socket.isClosed }

    // Called at most once, when the pool discards or shuts down the connection.
    func close() { socket.close() }
}

struct DatabaseProvider: ConnectionPoolProvider {
    func newConnection() async throws -> DatabaseConnection {
        try await DatabaseConnection.connect(to: host)
    }

    func newConnectionSync() throws -> DatabaseConnection {
        try DatabaseConnection.connectSync(to: host)
    }
}
```

Both provider requirements must produce interchangeable connections — a connection opened by one may later be handed to a caller of the other. Providers are called concurrently (up to the pool size) and must be `Sendable`.

### 2. Create a pool

```swift
let pool = ConnectionPool(
    provider: DatabaseProvider(),
    poolSize: 8,                    // maximum connections and concurrent borrowers
    timeout: .seconds(30)           // default wait when a caller doesn't pass one
)
```

No connections are opened until the first checkout.

### 3. Borrow connections

From `async` code:

```swift
let user = try await pool.withConnection { connection in
    try await connection.query("SELECT * FROM users WHERE id = $1", [id])
}
```

From blocking code:

```swift
let user = try pool.withConnectionSync { connection in
    try connection.querySync("SELECT * FROM users WHERE id = $1", [id])
}
```

The connection is only valid inside the closure — don't store it or let it escape. When the closure returns, the connection goes back to the pool (or is closed if it's no longer valid).

> **Warning:** `withConnectionSync` blocks the calling thread. It is marked `noasync`, so calling it directly from a Swift concurrency context is a compile-time error — use `withConnection` there instead. Synchronous helper functions invoked from async code can still reach it; inside those, pinned-connection reuse keeps nested checkouts deadlock-free.

### Re-entrancy

Nested calls in the same context reuse the connection the context already holds, so this cannot deadlock even on a pool of one:

```swift
try await pool.withConnection { connection in
    try await pool.withConnection { same in
        // same === connection — no second pool slot is taken
    }
}
```

This also works across styles — blocking calls nested inside an `async` scope see that scope's connection — and structured child tasks inherit their parent's pinned connection. Detached tasks and unstructured tasks that outlive their scope do not; they check out their own connection.

### Timeouts, errors, and cancellation

```swift
do {
    try await pool.withConnection(timeout: .seconds(5)) { connection in
        try await connection.query(...)
    }
} catch ConnectionPoolError.timedOut {
    // no connection became available within 5 seconds
} catch ConnectionPoolError.closed {
    // the pool was closed before or while waiting
} catch is CancellationError {
    // the surrounding task was cancelled while waiting
}
```

The timeout bounds only the wait for a connection — not connection creation, and not your closure's runtime. Errors thrown by your provider or your closure are rethrown unchanged.

### Shutting down

```swift
pool.close()
```

Closing is immediate and idempotent: queued waiters are woken with `ConnectionPoolError.closed`, subsequent checkouts fail fast, and idle connections are closed. Connections currently in use are untouched — their scopes finish normally and each connection is closed when returned. Dropping the last reference to a pool closes it automatically.

## Documentation

Every public symbol carries DocC documentation. Open the package in Xcode and run **Product → Build Documentation**, or add [`swift-docc-plugin`](https://github.com/swiftlang/swift-docc-plugin) and run:

```bash
swift package generate-documentation
```

## Testing

```bash
swift test
```

The suite covers pool-size enforcement under mixed sync/async load, FIFO fairness, connection pinning and pin lifetime, timeout/cancellation/signal races against the internal semaphore, provider failure recovery, and close semantics. It runs clean under ThreadSanitizer (`swift test --sanitize=thread`).
