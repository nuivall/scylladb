# Seastar Coroutine Tutorial for AI Agents

Distilled from the Seastar tutorial. Focuses on the **coroutine style** (preferred for new code).
Skip this if you already know Seastar well; use as reference when writing async C++ in ScyllaDB.

## Core Concepts

Seastar is a C++ framework for asynchronous, share-nothing server applications.

- **One thread per core.** No OS thread context switches. Each core runs an event loop (the "reactor").
- **Share-nothing.** Memory is partitioned across cores. No locks, no atomics, no `std::mutex`.
  Inter-core communication uses explicit message passing (`submit_to()`).
- **Futures.** A `future<T>` represents a value of type `T` that may not be available yet.
  `future<>` represents a completion with no value.
- **Never block.** No `std::sleep`, `std::mutex`, blocking I/O. Use Seastar async equivalents.

## Coroutines (Preferred Style)

A coroutine is a function that returns `seastar::future<T>` and uses `co_await`/`co_return`.

```cpp
#include <seastar/core/coroutine.hh>

seastar::future<int> slow_fetch_and_increment() {
    auto n = co_await read();           // suspend until read() completes
    co_await seastar::sleep(1s);        // local variables survive across co_await
    co_await write(n + 1);              // co_await void futures too
    co_return n;                        // resolves the returned future<int>
}
```

- `co_await` on a ready future extracts the value immediately.
- `co_await` on a not-ready future suspends the coroutine; it resumes when the future resolves.
- Local variables are automatically preserved across `co_await` points -- no `do_with()` needed.
- Each `co_await` is a **preemption point** (checks task quota, may yield to the reactor).

## Exception Handling in Coroutines

Exceptions translate naturally between futures and coroutines:

```cpp
seastar::future<> exception_handling() {
    try {
        co_await function_returning_an_exceptional_future();
    } catch (...) {
        // exception from the failed future is caught here
    }
    throw std::runtime_error("oops");
    // ^ automatically becomes an exceptional future for our caller
}
```

### Avoiding throw/catch overhead

Use `coroutine::as_future` to inspect without throwing, and `coroutine::exception` to propagate:

```cpp
seastar::future<int> efficient_error_handling() {
    // Automatically propagate exception without throw (like Rust's ? operator)
    auto result = co_await seastar::coroutine::try_future(maybe_failing_call());

    // Or inspect manually without throwing
    auto fut = co_await seastar::coroutine::as_future(maybe_failing_call());
    if (fut.failed()) {
        co_return seastar::coroutine::exception(fut.get_exception());
    }
    co_return fut.get();
}
```

## Concurrency in Coroutines

### Parallel execution with `coroutine::all`

```cpp
#include <seastar/coroutine/all.hh>

seastar::future<int> parallel_sum(int key1, int key2) {
    auto [a, b] = co_await seastar::coroutine::all(
        [&] { return read(key1); },
        [&] { return read(key2); }
    );
    co_return a + b;  // both reads run concurrently
}
```

### Parallel iteration with `coroutine::parallel_for_each`

```cpp
#include <seastar/coroutine/parallel_for_each.hh>

seastar::future<bool> all_exist(std::vector<seastar::sstring> filenames) {
    bool res = true;
    co_await seastar::coroutine::parallel_for_each(filenames,
        [&res] (const seastar::sstring& name) -> seastar::future<> {
            res &= co_await seastar::file_exists(name);
        });
    co_return res;
}
```

## Breaking Up Long Computations

Long CPU-bound work without I/O can stall the reactor. Use `coroutine::maybe_yield`:

```cpp
#include <seastar/coroutine/maybe_yield.hh>

seastar::future<float> long_loop(int n) {
    float acc = 0;
    for (int i = 0; i < n; ++i) {
        acc += std::sin(float(i));
        co_await seastar::coroutine::maybe_yield();  // give reactor a chance to run
    }
    co_return acc;
}
```

The default **task quota** is 500us. Exceeding it without yielding causes a **reactor stall**.

## Lambda Coroutines (C++23)

ScyllaDB uses C++23. Lambda coroutines work safely:

```cpp
// Awaited from outer coroutine -- safe as-is
seastar::future<> outer() {
    co_await [captures...] () -> seastar::future<> {
        co_await some_operation();
        co_return;
    }();
}

// Called asynchronously -- use `this auto` to copy captures into coroutine frame
seastar::future<> outer2() {
    launch_async([captures...] (this auto) -> seastar::future<> {
        co_await some_operation();
    });
    co_return;
}
```

## Lifetime Management (Coroutine vs Continuation Style)

With coroutines, lifetime is natural -- local variables live across `co_await`:

```cpp
// Coroutine style: simple, safe
seastar::future<> process() {
    auto obj = create_object();        // lives on coroutine frame
    co_await slow_op(obj);             // obj survives
    co_await another_op(obj);          // still alive
    // obj destroyed when coroutine ends
}
```

Contrast with continuation style (avoid in new code):

```cpp
// Continuation style: verbose, error-prone
seastar::future<> process() {
    return seastar::do_with(create_object(), [] (auto& obj) {
        return slow_op(obj).then([&obj] {
            return another_op(obj);
        });
    });
}
```

## Shared Ownership

- `seastar::lw_shared_ptr<T>` -- lightweight, single-shard, no polymorphism. **Preferred.**
- `seastar::shared_ptr<T>` -- supports polymorphism, slightly heavier.
- `std::shared_ptr<T>` -- **never use** in Seastar (uses atomic refcounts, unnecessary overhead).
- `seastar::foreign_ptr<P>` -- wraps a pointer for cross-shard transfer. Destructor runs on home shard.

## Semaphores (Limiting Parallelism)

```cpp
seastar::future<> g() {
    static thread_local seastar::semaphore limit(100);
    // with_semaphore: exception-safe acquire + release
    return seastar::with_semaphore(limit, 1, [] {
        return slow();  // at most 100 concurrent slow() calls
    });
}
```

For advanced cases (e.g., starting work without waiting for completion), use `get_units()`:

```cpp
auto units = co_await seastar::get_units(limit, 1);
// units are held until destroyed -- RAII-based
```

## Gates (Shutdown Coordination)

A gate tracks in-progress operations and blocks new ones during shutdown:

```cpp
seastar::gate g;

// Start an operation (throws gate_closed_exception after close())
co_await seastar::with_gate(g, [] { return slow(); });

// Shutdown: close gate and wait for all operations to finish
co_await g.close();
```

Inside long operations, call `g.check()` to detect shutdown and stop early.

## Sharded Services

`seastar::sharded<T>` creates one instance of `T` per core:

```cpp
seastar::sharded<my_service> service;

co_await service.start(constructor_args...);     // creates T on each core
co_await service.invoke_on_all(&my_service::run);  // call run() on each core's copy
co_await service.invoke_on(0, &my_service::do_something); // call on specific core
co_await service.stop();                          // calls T::stop() on each core
```

The class must have a `future<> stop()` method. Always call `service.stop()` before destruction.

## Scheduling Groups

Isolate CPU time between components:

```cpp
auto sg = co_await seastar::create_scheduling_group("background", 100);  // 100 shares
co_await seastar::with_scheduling_group(sg, [] {
    return background_work();  // runs with controlled CPU share
});
```

Two groups with 100 shares each get equal CPU. A group with 200 shares gets 2x the CPU of a 100-share group.

## Key Anti-Patterns

| Don't | Do Instead |
|-------|-----------|
| `std::mutex`, `std::atomic` | `seastar::semaphore`, single-thread design |
| `std::sleep`, blocking I/O | `seastar::sleep`, `seastar::file` |
| `std::shared_ptr` | `seastar::lw_shared_ptr` |
| `new`/`delete` | `std::unique_ptr`, `std::make_unique` |
| Ignoring a failed future | Handle or explicitly `.ignore_ready_future()` |
| `.get()` outside `seastar::thread` | `co_await` |
| Long CPU loop without yield | Insert `co_await coroutine::maybe_yield()` |
| `do_with` + `.then()` chains | Coroutines with local variables |

## Quick Reference: Headers

| Feature | Header |
|---------|--------|
| `co_await`, `co_return` | `<seastar/core/coroutine.hh>` |
| `coroutine::all` | `<seastar/coroutine/all.hh>` |
| `coroutine::parallel_for_each` | `<seastar/coroutine/parallel_for_each.hh>` |
| `coroutine::maybe_yield` | `<seastar/coroutine/maybe_yield.hh>` |
| `coroutine::as_future` | `<seastar/core/coroutine.hh>` |
| `semaphore`, `with_semaphore` | `<seastar/core/semaphore.hh>` |
| `gate`, `with_gate` | `<seastar/core/gate.hh>` |
| `sharded<T>` | `<seastar/core/sharded.hh>` |
| `sleep` | `<seastar/core/sleep.hh>` |
| `scheduling_group` | `<seastar/core/scheduling.hh>` |
| `foreign_ptr` | `<seastar/core/shared_ptr.hh>` |
| `smp::submit_to` | `<seastar/core/smp.hh>` |

