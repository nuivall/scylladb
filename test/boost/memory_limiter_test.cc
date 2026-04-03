/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/coroutine.hh>
#include <seastar/core/scheduling.hh>

#include "test/lib/scylla_test_case.hh"
#include "service/memory_limiter.hh"

using namespace seastar;

SEASTAR_TEST_CASE(test_shared_memory_pool_basic) {
    shared_memory_pool pool(1000);
    BOOST_REQUIRE_EQUAL(pool.total_memory(), 1000);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 1000);

    BOOST_REQUIRE(pool.try_consume(400));
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 600);

    BOOST_REQUIRE(!pool.try_consume(700));
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 600);

    pool.signal(400);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 1000);

    // Adjust total memory
    pool.set_total_memory(2000);
    BOOST_REQUIRE_EQUAL(pool.total_memory(), 2000);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 2000);

    // Shrink: available should drop accordingly
    pool.set_total_memory(500);
    BOOST_REQUIRE_EQUAL(pool.total_memory(), 500);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 500);

    co_return;
}

SEASTAR_TEST_CASE(test_shared_memory_pool_shrink_while_borrowed) {
    shared_memory_pool pool(1000);

    // Shrink with nothing outstanding — exercises ssize_t diff in
    // set_total_memory when new total < old total (diff = -600).
    pool.set_total_memory(400);
    BOOST_REQUIRE_EQUAL(pool.total_memory(), 400);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 400);

    // Grow back
    pool.set_total_memory(1000);
    BOOST_REQUIRE_EQUAL(pool.total_memory(), 1000);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 1000);

    BOOST_REQUIRE(pool.try_consume(800));
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 200);

    // Shrink total while 800 is outstanding — available goes negative
    pool.set_total_memory(500);
    BOOST_REQUIRE_EQUAL(pool.total_memory(), 500);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), -300);

    // try_consume should fail when available is negative
    BOOST_REQUIRE(!pool.try_consume(1));

    // Return borrowed memory
    pool.signal(800);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 500);

    co_return;
}

SEASTAR_TEST_CASE(test_memory_units_return_borrowed_first) {
    // Test that memory_units returns borrowed memory to the shared pool
    // before returning to the semaphore
    shared_memory_pool pool(500);
    semaphore sem(1000);

    // Simulate borrowing 200 from shared pool
    BOOST_REQUIRE(pool.try_consume(200));
    sem.signal(200);

    auto sem_units = co_await get_units(sem, 500);
    service::memory_units mu(std::move(sem_units), &pool, &sem, 200);

    // State: sem=700, pool=300, count=500, borrowed=200
    BOOST_REQUIRE_EQUAL(mu.count(), 500u);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 300);

    // Return 150 units. The borrowed portion (150 of 200) goes back to the
    // shared pool; the semaphore is signaled and then consumed for the borrowed
    // part, so it ends up net: sem += (150 - 150) = 0 change from borrowed,
    // but sem gets 150 signaled first then 150 consumed = net 0.
    // Actually: _sem_units.return_units(150) → sem=850, count=350
    //           sem.consume(150) → sem=700
    //           pool.signal(150) → pool=450
    mu.return_units(150);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 450);
    BOOST_REQUIRE_EQUAL(sem.current(), 700u);
    BOOST_REQUIRE_EQUAL(mu.count(), 350u);

    // Return 150 more. Only 50 borrowed remains.
    // _sem_units.return_units(150) → sem=850, count=200
    // sem.consume(50) → sem=800
    // pool.signal(50) → pool=500
    mu.return_units(150);
    BOOST_REQUIRE_EQUAL(pool.available_memory(), 500);
    BOOST_REQUIRE_EQUAL(sem.current(), 800u);
    BOOST_REQUIRE_EQUAL(mu.count(), 200u);

    // Destroy to release the rest (200 remaining, all from semaphore, no borrowed)
    { auto tmp = std::move(mu); }

    BOOST_REQUIRE_EQUAL(pool.available_memory(), 500); // no change — no more borrowed
    BOOST_REQUIRE_EQUAL(sem.current(), 1000u); // all returned to semaphore

    co_return;
}

SEASTAR_TEST_CASE(test_memory_units_no_pool) {
    // Test memory_units without a shared pool (no borrowing)
    semaphore sem(1000);

    auto sem_units = co_await get_units(sem, 300);
    service::memory_units mu(std::move(sem_units), nullptr, nullptr, 0);

    BOOST_REQUIRE_EQUAL(mu.count(), 300u);
    BOOST_REQUIRE_EQUAL(sem.current(), 700u);

    mu.return_units(100);
    BOOST_REQUIRE_EQUAL(sem.current(), 800u);

    { auto tmp = std::move(mu); }
    BOOST_REQUIRE_EQUAL(sem.current(), 1000u);

    co_return;
}

SEASTAR_TEST_CASE(test_memory_units_move) {
    shared_memory_pool pool(500);
    semaphore sem(1000);

    BOOST_REQUIRE(pool.try_consume(100));
    sem.signal(100);

    auto sem_units = co_await get_units(sem, 400);
    service::memory_units mu1(std::move(sem_units), &pool, &sem, 100);

    // Move construct
    service::memory_units mu2(std::move(mu1));
    BOOST_REQUIRE_EQUAL(mu2.count(), 400u);
    BOOST_REQUIRE_EQUAL(mu1.count(), 0u);

    // Move assign
    service::memory_units mu3;
    mu3 = std::move(mu2);
    BOOST_REQUIRE_EQUAL(mu3.count(), 400u);
    BOOST_REQUIRE_EQUAL(mu2.count(), 0u);

    // Destroy — should return borrowed to pool
    { auto tmp = std::move(mu3); }

    BOOST_REQUIRE_EQUAL(pool.available_memory(), 500); // 400 + 100 returned to pool? No: only 100 was borrowed
    BOOST_REQUIRE_EQUAL(sem.current(), 1000u);

    co_return;
}
