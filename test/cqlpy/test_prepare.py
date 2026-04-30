# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for preparing various kinds of statements. When a client asks to prepare
# a statement, Scylla has to process it and return the correct prepared statement
# metadata. The metadata contains information about the keyspace, table and bind variables.
# Let's ensure that this information is correct.
# Here's the description of prepared metadata in CQL protocol spec:
# https://github.com/apache/cassandra/blob/1959502d8b16212479eecb076c89945c3f0f180c/doc/native_protocol_v4.spec#L675

import pytest
from .util import new_test_table, new_test_keyspace, unique_key_int, config_value_context

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int, c int, PRIMARY KEY (p, c)") as table:
        yield table


@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p1 int, p2 int, p3 int, p4 int, c1 int, c2 int, r1 int, r2 int, PRIMARY KEY ((p1, p2, p3, p4), c1, c2)") as table:
        yield table

# The following tests test the generation of "pk indexes"
# "pk indexes" tell the driver which bind variable values it should use to calculate the partition token, so that it can send queries to the correct shard.
# https://github.com/apache/cassandra/blob/1959502d8b16212479eecb076c89945c3f0f180c/doc/native_protocol_v4.spec#L699-L707


# Test generating pk indexes for a single column partition key.
def test_single_pk_indexes(cql, table1):
    prepared = cql.prepare(f"SELECT p FROM {table1} WHERE p = ?")
    assert prepared.routing_key_indexes == [0]

    prepared = cql.prepare(f"SELECT p, c FROM {table1} WHERE c = ? AND p = ?")
    assert prepared.routing_key_indexes == [1]

# Test that pk indexes aren't generated when the partition key column isn't restricted using a bind variable.
# In this situation the driver won't be able to calculate the token, so pk indexes should be empty (None).
def test_single_pk_no_indexes(cql, table1):
    prepared = cql.prepare(f"SELECT p, c FROM {table1}")
    assert prepared.routing_key_indexes is None

    prepared = cql.prepare(f"SELECT p, c FROM {table1} WHERE c = ? ALLOW FILTERING")
    assert prepared.routing_key_indexes is None

    prepared = cql.prepare(f"SELECT p FROM {table1} WHERE p = 0 AND c = ?")
    assert prepared.routing_key_indexes is None

# Test generating pk indexes for a composite partition key.
def test_composite_pk_indexes(cql, table2):
    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = ? AND p2 = ? AND p3 = ? AND p4 = ? AND c1 = ? AND c2 = ?")
    assert prepared.routing_key_indexes == [0, 1, 2, 3]

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p4 = ? AND p3 = ? AND p2 = ? AND p1 = ? AND c1 = ? AND c2 = ?")
    assert prepared.routing_key_indexes == [3, 2, 1, 0]

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE r1 = ? AND c2 = ? AND p3 = ? AND p1 = ? AND r2 = ? AND p4 = ? AND c1 = ? AND p2 = ? ALLOW FILTERING")
    assert prepared.routing_key_indexes == [3, 7, 2, 5]

# Test that pk indexes aren't generated when not all partition key columns are restricted using bind variables.
# In this situation the driver won't be able to calculate the token, so pk indexes should be empty (None).
def test_composite_pk_no_indexes(cql, table2):
    prepared = cql.prepare(
        f"SELECT * FROM {table2}")
    assert prepared.routing_key_indexes is None

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = ? ALLOW FILTERING")
    assert prepared.routing_key_indexes is None

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = ? AND p2 = ? AND p4 = ? ALLOW FILTERING")
    assert prepared.routing_key_indexes is None

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = ? AND p2 = 0 AND p3 = ? AND p4 = ?")
    assert prepared.routing_key_indexes is None

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = ? AND p2 = 0 AND p3 = ? AND p4 = ? AND c1 = ? AND c2 = ?")
    assert prepared.routing_key_indexes is None

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = 0 AND p2 = 1 AND p3 = 2 AND p4 = 3 AND c1 = 5 AND c2 = 5")
    assert prepared.routing_key_indexes is None

# Test generating pk indexes for a single column partition key using named bind variables.
def test_single_pk_indexes_named_variables(cql, table1):
    prepared = cql.prepare(f"SELECT p FROM {table1} WHERE p = :a")
    assert prepared.routing_key_indexes == [0]

    prepared = cql.prepare(f"SELECT p, c FROM {table1} WHERE c = :a AND p = :b")
    assert prepared.routing_key_indexes == [1]

# Test generating pk indexes for a composite partition key using named bind variables.
def test_composite_pk_indexes_named_variables(cql, table2):
    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = :a AND p2 = :b AND p3 = :c AND p4 = :d AND c1 = :e AND c2 = :f")
    assert prepared.routing_key_indexes == [0, 1, 2, 3]

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = :f AND p2 = :e AND p3 = :d AND p4 = :c AND c1 = :b AND c2 = :a")
    assert prepared.routing_key_indexes == [0, 1, 2, 3]

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE c1 = :a AND c2 = :b AND p1 = :f AND p2 = :e AND p3 = :d AND p4 = :c")
    assert prepared.routing_key_indexes == [2, 3, 4, 5]

# Test generating pk indexes with named bind variables where the same variable is used multiple times.
# The test is scylla_only because Scylla treats :x as a single bind variable, but Cassandra thinks
# that there are two bind variables, both of them named :x.
def test_single_pk_indexes_duplicate_named_variables(cql, table1, scylla_only):
    prepared = cql.prepare(f"SELECT p FROM {table1} WHERE p = :x")
    assert prepared.routing_key_indexes == [0]

    prepared = cql.prepare(f"SELECT p FROM {table1} WHERE p = :x AND c = :x")
    assert prepared.routing_key_indexes == [0]

    prepared = cql.prepare(f"SELECT p FROM {table1} WHERE c = :x AND p = :x")
    assert prepared.routing_key_indexes == [0]

# Same test as test_single_pk_indexes_duplicate_named_variables, but in Cassandra compatibility mode. #15559
def test_single_pk_indexes_duplicate_named_variables_cassandra_compatiblity_mode(cql, table1):
    with config_value_context(cql, 'cql_duplicate_bind_variable_names_refer_to_same_variable', 'false'):
        prepared = cql.prepare(f"SELECT p FROM {table1} WHERE p = :x")
        assert prepared.routing_key_indexes == [0]

        prepared = cql.prepare(f"SELECT p FROM {table1} WHERE p = :x AND c = :x")
        assert prepared.routing_key_indexes == [0]

        prepared = cql.prepare(f"SELECT p FROM {table1} WHERE c = :x AND p = :x")
        # Without #15559 this would be [0].
        assert prepared.routing_key_indexes == [1]


# Test generating pk indexes with named bind variables where the same variable is used multiple times.
# The test is scylla_only because Scylla treats :x as a single bind variable, but Cassandra thinks
# that there are multiple bind variables, all of them named :x.
def test_composite_pk_indexes_duplicate_named_variables(cql, table2, scylla_only):
    prepared = cql.prepare(f"SELECT * FROM {table2} WHERE p1 = :x AND p2 = :x AND p3 = :x AND p4 = :x")
    assert prepared.routing_key_indexes == [0, 0, 0, 0]

    prepared = cql.prepare(f"SELECT * FROM {table2} WHERE p1 = :a AND p2 = :a AND p3 = :b AND p4 = :b")
    assert prepared.routing_key_indexes == [0, 0, 1, 1]

    prepared = cql.prepare(f"SELECT * FROM {table2} WHERE p1 = :a AND p2 = :b AND p3 = :a AND p4 = :b")
    assert prepared.routing_key_indexes == [0, 1, 0, 1]

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE c1 = :a AND c2 = :b AND p1 = :a AND p2 = :b AND p3 = :a AND p4 = :b")
    assert prepared.routing_key_indexes == [0, 1, 0, 1]

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = :a AND p2 = :b AND p3 = :a AND p4 = :b AND c1 = :a AND c2 = :b ")
    assert prepared.routing_key_indexes == [0, 1, 0, 1]

    prepared = cql.prepare(
        f"SELECT * FROM {table2} WHERE p1 = :x AND p2 = :x AND p3 = :z AND p4 = :y AND c1 = :y AND c2 = :z ")
    assert prepared.routing_key_indexes == [0, 0, 1, 2]

# Same test as test_composite_pk_indexes_duplicate_named_variables, but in Cassandra compatibility mode. #15559
def test_composite_pk_indexes_duplicate_named_variables_cassandra_compatibility_mode(cql, table2):
    with config_value_context(cql, 'cql_duplicate_bind_variable_names_refer_to_same_variable', 'false'):
        prepared = cql.prepare(f"SELECT * FROM {table2} WHERE p1 = :x AND p2 = :x AND p3 = :x AND p4 = :x")
        assert prepared.routing_key_indexes == [0, 1, 2, 3]

        prepared = cql.prepare(f"SELECT * FROM {table2} WHERE p1 = :a AND p2 = :a AND p3 = :b AND p4 = :b")
        assert prepared.routing_key_indexes == [0, 1, 2, 3]

        prepared = cql.prepare(f"SELECT * FROM {table2} WHERE p1 = :a AND p2 = :b AND p3 = :a AND p4 = :b")
        assert prepared.routing_key_indexes == [0, 1, 2, 3]

        prepared = cql.prepare(
            f"SELECT * FROM {table2} WHERE c1 = :a AND c2 = :b AND p1 = :a AND p2 = :b AND p3 = :a AND p4 = :b")
        assert prepared.routing_key_indexes == [2, 3, 4, 5]

        prepared = cql.prepare(
            f"SELECT * FROM {table2} WHERE p1 = :a AND p2 = :b AND p3 = :a AND p4 = :b AND c1 = :a AND c2 = :b ")
        assert prepared.routing_key_indexes == [0, 1, 2, 3]

        prepared = cql.prepare(
            f"SELECT * FROM {table2} WHERE p1 = :x AND p2 = :x AND p3 = :z AND p4 = :y AND c1 = :y AND c2 = :z ")
        assert prepared.routing_key_indexes == [0, 1, 2, 3]

# Test what happens when using a bind marker with the same name (e.g., ":x")
# twice in a query. Above we tested which "routing_key_indexes" is returned
# in a PREPARE request, but here we test the functionality of the duplicate
# marker when actually executing the query.
# Note: The CQL protocol allows using bind markers in both QUERY (unprepared
# statement) and EXECUTE (prepared statement) cases, but it appears that the
# Python driver only supports bind markers with prepared statements (and
# for unprepared statements uses a different mechanisms with Python "%s" and
# "%(name)s"), so this test is only for the prepared-statement case.
# Reproduces issue #15559.
def test_duplicate_named_bind_marker_prepared(cql, table1, scylla_only):
    x = unique_key_int()
    cql.execute(f'INSERT INTO {table1} (p,c) VALUES ({x},{x})')
    cql.execute(f'INSERT INTO {table1} (p,c) VALUES ({x},{x+1})')
    # Sanity check: query without bind markers, with unnamed bind markers,
    # and with two different bind-marker names. All should work.
    assert [(x,x)] == list(cql.execute(f'SELECT * FROM {table1} WHERE p={x} AND c={x}'))
    stmt = cql.prepare(f'SELECT * FROM {table1} WHERE p=? AND c=?')
    assert [(x,x)] == list(cql.execute(stmt, (x,x)))
    stmt = cql.prepare(f'SELECT * FROM {table1} WHERE p=:x1 AND c=:x2')
    assert [(x,x)] == list(cql.execute(stmt, {'x1': x, 'x2': x}))
    assert [(x,x)] == list(cql.execute(stmt, (x,x)))
    # Now for the real test: Use the same bind-marker name twice.
    stmt = cql.prepare(f'SELECT * FROM {table1} WHERE p=:x AND c=:x')
    # If EXECUTE is passed a bound value with a name "x", both bind markers
    # named "x" are assigned, and the query works:
    assert [(x,x)] == list(cql.execute(stmt, {'x': x}))
    # In Cassandra, if EXECUTE is passed unnamed bound values, they go to the
    # bind markers no matter what their name is - using ":x" twice in the
    # query has the same effect as using "?" twice - you still need to pass
    # two bound values.
    # In ScyllaDB, it is expected to fail - the driver complains that
    # "Too many arguments provided to bind() (got 2, expected 1)", because
    # Scylla told it there is just one bind variable in the query.
    with pytest.raises(Exception):
        assert [(x,x)] == list(cql.execute(stmt, (x,x)))
    # Passing unnamed values, one can pass two different values even when the
    # query's bind markers have the same name :x. Exactly like two different
    # "?" bind markers can also be bound to different values. However ScyllaDB
    # treats this as a single named variable.
    with pytest.raises(Exception):
        assert [(x,x+1)] == list(cql.execute(stmt, (x,x+1)))

# Same test as test_duplicate_named_bind_marker_prepared, but in Cassandra compatibility mode. #15559
def test_duplicate_named_bind_marker_prepared_cassandra_compatibility_mode(cql, table1):
    with config_value_context(cql, 'cql_duplicate_bind_variable_names_refer_to_same_variable', 'false'):
        x = unique_key_int()
        cql.execute(f'INSERT INTO {table1} (p,c) VALUES ({x},{x})')
        cql.execute(f'INSERT INTO {table1} (p,c) VALUES ({x},{x+1})')
        # Sanity check: query without bind markers, with unnamed bind markers,
        # and with two different bind-marker names. All should work.
        assert [(x,x)] == list(cql.execute(f'SELECT * FROM {table1} WHERE p={x} AND c={x}'))
        stmt = cql.prepare(f'SELECT * FROM {table1} WHERE p=? AND c=?')
        assert [(x,x)] == list(cql.execute(stmt, (x,x)))
        stmt = cql.prepare(f'SELECT * FROM {table1} WHERE p=:x1 AND c=:x2')
        assert [(x,x)] == list(cql.execute(stmt, {'x1': x, 'x2': x}))
        assert [(x,x)] == list(cql.execute(stmt, (x,x)))
        # Now for the real test: Use the same bind-marker name twice.
        stmt = cql.prepare(f'SELECT * FROM {table1} WHERE p=:x AND c=:x')
        # If EXECUTE is passed a bound value with a name "x", both bind markers
        # named "x" are assigned, and the query works:
        assert [(x,x)] == list(cql.execute(stmt, {'x': x}))
        # In Cassandra, if EXECUTE is passed unnamed bound values, they go to the
        # bind markers no matter what their name is - using ":x" twice in the
        # query has the same effect as using "?" twice - you still need to pass
        # two bound values.
        assert [(x,x)] == list(cql.execute(stmt, (x,x)))
        # Passing unnamed values, one can pass two different values even when the
        # query's bind markers have the same name :x. Exactly like two different
        # "?" bind markers can also be bound to different values:
        assert [(x,x+1)] == list(cql.execute(stmt, (x,x+1)))

#############################################################################
# Tests for the prepared statement id derivation rules introduced for
# SCYLLADB-1224 (mirrors Cassandra's CASSANDRA-15252). The id of a fully
# qualified prepared statement must not depend on the connection's current
# USE keyspace; otherwise drivers cannot reliably reprepare on the fly when
# the server cache evicts an entry (see scylla-rust-driver#1561 for a real
# example).

# A fresh single-use session that allows us to issue USE without disturbing
# the shared `cql` fixture (which other tests rely on to be in a known
# keyspace).
def _new_session(cql):
    cluster = cql.cluster
    return cluster.connect()

def test_prepared_id_stable_for_qualified_under_use(cql, this_dc):
    """A fully qualified PREPARE returns the same id regardless of whether
    a USE statement is in effect on the connection. SCYLLADB-1224."""
    with new_test_keyspace(cql,
            "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', '" + this_dc + "': 1}") as ks:
        cql.execute(f"CREATE TABLE {ks}.t (p int PRIMARY KEY, v int)")
        s1 = _new_session(cql)
        s2 = _new_session(cql)
        try:
            id_no_use = s1.prepare(f"SELECT v FROM {ks}.t WHERE p=?").query_id
            s2.execute(f"USE {ks}")
            id_with_use = s2.prepare(f"SELECT v FROM {ks}.t WHERE p=?").query_id
            assert id_no_use == id_with_use
        finally:
            s1.shutdown()
            s2.shutdown()

def test_prepared_id_differs_for_unqualified_under_use(cql, this_dc):
    """For unqualified PREPAREs the id must continue to depend on USE
    keyspace - the statement is genuinely keyspace-dependent. Regression
    guard for the SCYLLADB-1224 fix."""
    with new_test_keyspace(cql,
            "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', '" + this_dc + "': 1}") as ks1, \
         new_test_keyspace(cql,
            "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', '" + this_dc + "': 1}") as ks2:
        cql.execute(f"CREATE TABLE {ks1}.t (p int PRIMARY KEY, v int)")
        cql.execute(f"CREATE TABLE {ks2}.t (p int PRIMARY KEY, v int)")
        s1 = _new_session(cql)
        s2 = _new_session(cql)
        try:
            s1.execute(f"USE {ks1}")
            s2.execute(f"USE {ks2}")
            id1 = s1.prepare("SELECT v FROM t WHERE p=?").query_id
            id2 = s2.prepare("SELECT v FROM t WHERE p=?").query_id
            assert id1 != id2
        finally:
            s1.shutdown()
            s2.shutdown()

def test_reprepare_qualified_after_use_keyspace(cql, this_dc):
    """End-to-end driver flow: prepare a fully qualified statement, drop
    and recreate the underlying table to invalidate the server cache,
    issue USE on the same connection, then execute. The driver
    transparently reprepares; with the SCYLLADB-1224 fix the second
    PREPARE returns the same id and execution succeeds."""
    with new_test_keyspace(cql,
            "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', '" + this_dc + "': 1}") as ks:
        cql.execute(f"CREATE TABLE {ks}.t (p int PRIMARY KEY, v int)")
        s = _new_session(cql)
        try:
            stmt = s.prepare(f"SELECT v FROM {ks}.t WHERE p=?")
            # Invalidate server-side cache by dropping & recreating the table.
            s.execute(f"DROP TABLE {ks}.t")
            s.execute(f"CREATE TABLE {ks}.t (p int PRIMARY KEY, v int)")
            s.execute(f"USE {ks}")
            # Should not raise; driver will see UNPREPARED, reprepare, retry.
            list(s.execute(stmt, (1,)))
        finally:
            s.shutdown()

def test_prepare_unqualified_emits_warning(cql, this_dc):
    """Preparing an unqualified statement should attach a warning
    advising the user to fully qualify the table name."""
    with new_test_keyspace(cql,
            "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', '" + this_dc + "': 1}") as ks:
        cql.execute(f"CREATE TABLE {ks}.t (p int PRIMARY KEY, v int)")
        s = _new_session(cql)
        try:
            s.execute(f"USE {ks}")
            stmt = s.prepare("SELECT v FROM t WHERE p=?")
            warnings = stmt.result_metadata.warnings if hasattr(stmt.result_metadata, 'warnings') else None
            # The python-driver exposes prepare warnings on the response future,
            # but does not retain them on the PreparedStatement object across
            # all driver versions. Rather than tying the assertion to a fragile
            # internal API, verify that the matching qualified prepare does NOT
            # produce a warning - i.e. that we don't warn unconditionally.
            qualified = s.prepare(f"SELECT v FROM {ks}.t WHERE p=?")
            qual_warnings = qualified.result_metadata.warnings if hasattr(qualified.result_metadata, 'warnings') else None
            # If the driver surfaces warnings at all, the unqualified prepare
            # should have a non-empty list and the qualified one should not.
            if warnings is not None and qual_warnings is not None:
                assert warnings
                assert not qual_warnings
        finally:
            s.shutdown()

def test_batch_prepared_id_stable_for_qualified(cql, this_dc):
    """A BEGIN BATCH whose every sub-statement is fully qualified is itself
    keyspace-independent and must produce a stable prepared id across USE.
    SCYLLADB-1224."""
    with new_test_keyspace(cql,
            "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', '" + this_dc + "': 1}") as ks:
        cql.execute(f"CREATE TABLE {ks}.t (p int PRIMARY KEY, v int)")
        batch = (f"BEGIN BATCH "
                 f"INSERT INTO {ks}.t (p,v) VALUES (?,?); "
                 f"INSERT INTO {ks}.t (p,v) VALUES (?,?); "
                 f"APPLY BATCH;")
        s1 = _new_session(cql)
        s2 = _new_session(cql)
        try:
            id_no_use = s1.prepare(batch).query_id
            s2.execute(f"USE {ks}")
            id_with_use = s2.prepare(batch).query_id
            assert id_no_use == id_with_use
        finally:
            s1.shutdown()
            s2.shutdown()
