import Foundation
import Testing
@testable import ConnectionPool

private typealias Semaphore = ConnectionPoolSemaphore

private extension ConnectionPoolSemaphore {
    /// Explicitly selects the blocking overload; inside an `async` function the compiler always
    /// prefers the `async` one.
    func waitSync(timeout: Duration?) -> WaitResult {
        wait(timeout: timeout)
    }

    /// Explicitly selects the `async` overload.
    func waitAsync(timeout: Duration?) async -> WaitResult {
        await wait(timeout: timeout)
    }
}

/// Direct tests for the permit counter underneath the pool.
///
/// `signal()`, a firing timeout and a cancellation can all try to settle the same waiter at the
/// same instant, and only one of them may win — the loser must not walk off with the permit. Those
/// windows are microseconds wide, so they are driven here against the primitive rather than
/// through `ConnectionPool`, where they are effectively unreachable.
@Suite("Permit accounting", .timeLimit(.minutes(1)))
struct SemaphoreTests {

    @Test("permits are handed out up to the limit and no further")
    func permitsAreBounded() {
        let semaphore = Semaphore(value: 2)

        #expect(semaphore.waitSync(timeout: nil) == .success)
        #expect(semaphore.waitSync(timeout: nil) == .success)
        #expect(semaphore.waitSync(timeout: .milliseconds(50)) == .timedOut)

        semaphore.signal()
        #expect(semaphore.waitSync(timeout: .milliseconds(50)) == .success)
    }

    @Test("a signal with no waiters is banked for the next caller")
    func signalWithoutWaitersIsBanked() {
        let semaphore = Semaphore(value: 1)

        #expect(semaphore.waitSync(timeout: nil) == .success)
        semaphore.signal()
        semaphore.signal()

        #expect(semaphore.waitSync(timeout: nil) == .success)
        #expect(semaphore.waitSync(timeout: nil) == .success)
        #expect(semaphore.waitSync(timeout: .milliseconds(50)) == .timedOut)
    }

    /// A blocking waiter whose deadline expires at the same moment `signal()` hands it the permit
    /// must not report `.timedOut` *and* drop the permit — that loses it forever.
    @Test("a blocking waiter racing its own deadline against a signal never loses the permit")
    func blockingWaiterRacingSignalNeverLosesPermit() async {
        var timedOut = 0
        var acquired = 0

        for _ in 0..<400 {
            let semaphore = Semaphore(value: 1)
            #expect(semaphore.waitSync(timeout: nil) == .success, "setup: the only permit should be free")

            // The waiter is left running while the signal is timed to land right around its
            // deadline, so some iterations are signalled and some expire first.
            let pending = startOnThread { semaphore.waitSync(timeout: .milliseconds(2)) }
            try? await Task.sleep(for: .microseconds(.random(in: 1_400...2_600)))
            semaphore.signal()
            let result = await pending()

            switch result {
            case .success:
                acquired += 1
            case .timedOut:
                timedOut += 1
                // The waiter declined the permit, so it must have gone back to the pool.
                let recovered = semaphore.waitSync(timeout: .milliseconds(500))
                #expect(recovered == .success, "a permit vanished when a waiter timed out alongside a signal")
            case .cancelled:
                Issue.record("a blocking waiter cannot be cancelled")
            case .closed:
                Issue.record("the semaphore was never closed")
            }
        }

        // Guard against the test silently ceasing to exercise the race.
        #expect(timedOut > 0, "no iteration timed out; the race window was never entered")
        #expect(acquired > 0, "no iteration was signalled; the race window was never entered")
    }

    /// The same race on the async side: a timeout firing as the permit arrives.
    @Test("an async waiter racing its own deadline against a signal never loses the permit")
    func asyncWaiterRacingSignalNeverLosesPermit() async {
        var timedOut = 0
        var acquired = 0

        for _ in 0..<400 {
            let semaphore = Semaphore(value: 1)
            #expect(semaphore.waitSync(timeout: nil) == .success, "setup: the only permit should be free")

            let waiter = Task { await semaphore.waitAsync(timeout: .milliseconds(2)) }
            try? await Task.sleep(for: .microseconds(.random(in: 1_400...2_600)))
            semaphore.signal()

            let result = await expectCompletes(within: .seconds(3)) { await waiter.value }
            switch result {
            case .success:
                acquired += 1
            case .timedOut, .cancelled:
                timedOut += 1
                let recovered = await semaphore.waitAsync(timeout: .milliseconds(500))
                #expect(recovered == .success, "a permit vanished when a waiter timed out alongside a signal")
            case .closed:
                Issue.record("the semaphore was never closed")
            case nil:
                Issue.record("the waiter never resumed")
            }
        }

        #expect(timedOut > 0, "no iteration timed out; the race window was never entered")
        #expect(acquired > 0, "no iteration was signalled; the race window was never entered")
    }

    /// Cancellation is the third contender for settling a waiter. Both orderings are driven
    /// explicitly rather than left to chance, so each is exercised on every run.
    @Test("cancellation and a signal arriving together never lose the permit")
    func cancellationRacingSignalNeverLosesPermit() async {
        for iteration in 0..<100 {
            let semaphore = Semaphore(value: 1)
            #expect(semaphore.waitSync(timeout: nil) == .success, "setup: the only permit should be free")

            let waiter = Task { await semaphore.waitAsync(timeout: .seconds(5)) }
            try? await Task.sleep(for: .milliseconds(1))    // let the waiter queue

            let signalFirst = iteration.isMultiple(of: 2)
            if signalFirst {
                semaphore.signal()
                waiter.cancel()
            } else {
                waiter.cancel()
                semaphore.signal()
            }

            let result = await expectCompletes(within: .seconds(3)) { await waiter.value }
            switch result {
            case .success:
                // The waiter holds the permit; hand it back so the pool is whole again.
                semaphore.signal()
            case .cancelled, .timedOut:
                break
            case .closed:
                Issue.record("the semaphore was never closed (signal first: \(signalFirst))")
            case nil:
                Issue.record("the waiter never resumed (signal first: \(signalFirst))")
            }

            // Exactly one permit must exist either way.
            let recovered = await semaphore.waitAsync(timeout: .milliseconds(500))
            #expect(recovered == .success, "a permit vanished (signal first: \(signalFirst))")
            let extra = await semaphore.waitAsync(timeout: .milliseconds(5))
            #expect(extra == .timedOut, "a permit was duplicated (signal first: \(signalFirst))")
        }
    }

    /// Before the cancellation-handler refactor, cancellation was only observed by the timeout
    /// task's sleep — a waiter with no timeout could never be woken by cancellation at all.
    @Test("a cancelled waiter with no timeout is still woken")
    func cancelledWaiterWithoutTimeoutResolves() async {
        let semaphore = Semaphore(value: 1)
        #expect(semaphore.waitSync(timeout: nil) == .success, "setup: the only permit should be free")

        let waiter = Task { await semaphore.waitAsync(timeout: nil) }
        try? await Task.sleep(for: .milliseconds(50))    // let it park
        waiter.cancel()

        let result = await expectCompletes(within: .seconds(3)) { await waiter.value }
        #expect(result == .cancelled, "a waiter with no timeout hung through cancellation")

        // the permit the waiter declined must still be whole
        semaphore.signal()
        let recovered = await semaphore.waitAsync(timeout: .milliseconds(500))
        #expect(recovered == .success)
    }

    @Test("a waiter cancelled before it ever suspends still resolves")
    func waiterCancelledBeforeSuspendingResolves() async {
        let semaphore = Semaphore(value: 1)
        #expect(semaphore.waitSync(timeout: nil) == .success)

        // Cancelled up front, so `onCancel` can fire before the continuation exists.
        let waiter = Task { await semaphore.waitAsync(timeout: .seconds(5)) }
        waiter.cancel()

        let result = await expectCompletes(within: .seconds(3)) { await waiter.value }
        #expect(result == .cancelled)

        semaphore.signal()
        let recovered = await semaphore.waitAsync(timeout: .milliseconds(500))
        #expect(recovered == .success)
    }

    @Test("a closed semaphore rejects new waiters immediately")
    func closedSemaphoreRejectsNewWaiters() async {
        let semaphore = Semaphore(value: 1)
        semaphore.close()

        // even with a permit available, a closed semaphore refuses to hand it out
        #expect(semaphore.waitSync(timeout: .milliseconds(50)) == .closed)
        let asyncResult = await semaphore.waitAsync(timeout: .milliseconds(50))
        #expect(asyncResult == .closed)
    }

    @Test("closing wakes parked sync and async waiters with .closed")
    func closeWakesParkedWaiters() async {
        let semaphore = Semaphore(value: 0)

        let syncWaiter = startOnThread { semaphore.waitSync(timeout: .seconds(5)) }
        let asyncWaiter = Task { await semaphore.waitAsync(timeout: .seconds(5)) }
        try? await Task.sleep(for: .milliseconds(50))    // let both park

        semaphore.close()

        let syncResult = await expectCompletes(within: .seconds(3)) { await syncWaiter() }
        let asyncResult = await expectCompletes(within: .seconds(3)) { await asyncWaiter.value }
        #expect(syncResult == .closed, "a parked blocking waiter was not woken by close")
        #expect(asyncResult == .closed, "a parked async waiter was not woken by close")
    }

    @Test("closing twice and signalling after close are harmless")
    func closeIsIdempotentAndSignalSafe() {
        let semaphore = Semaphore(value: 1)
        semaphore.close()
        semaphore.close()
        semaphore.signal()

        #expect(semaphore.waitSync(timeout: .milliseconds(50)) == .closed)
    }

    @Test("many concurrent waiters each get exactly one permit")
    func concurrentWaitersEachGetOnePermit() async {
        let permits = 3
        let semaphore = Semaphore(value: permits)
        let tracker = ConcurrencyTracker()

        await withTaskGroup(of: Void.self) { group in
            for _ in 0..<30 {
                group.addTask {
                    guard await semaphore.waitAsync(timeout: .seconds(5)) == .success else {
                        Issue.record("an async waiter never acquired a permit")
                        return
                    }
                    await tracker.track { try? await Task.sleep(for: .milliseconds(5)) }
                    semaphore.signal()
                }
            }
            group.addTask {
                await runOnThreads(count: 10) { _ in
                    guard semaphore.waitSync(timeout: .seconds(5)) == .success else {
                        Issue.record("a blocking waiter never acquired a permit")
                        return
                    }
                    tracker.track { blockingSleep(.milliseconds(5)) }
                    semaphore.signal()
                }
            }
        }

        #expect(tracker.total == 40)
        #expect(tracker.peak <= permits, "peak concurrency was \(tracker.peak) against \(permits) permits")
    }
}
