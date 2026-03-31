# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for DELETE ... ALLOW FILTERING
#
# This feature allows deleting rows matching a WHERE clause that doesn't
# fully specify the primary key. The operation fans out to all nodes/shards
# in parallel (mapreduce-style), scans data, and writes tombstones.

import pytest

from cassandra.protocol import InvalidRequest, SyntaxException

from .util import new_test_table

# ----------------------------------------------------------------
# Not supported syntax
# ----------------------------------------------------------------

def test_delete_allow_filtering_rejects_column_specific(cql, test_keyspace, scylla_only):
    """Column-specific deletions (DELETE v FROM ...) are not supported with
    ALLOW FILTERING. Only whole-row/partition deletion is allowed."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        with pytest.raises(InvalidRequest, match="Column-specific deletions are not supported"):
            cql.execute(f"DELETE v FROM {table} WHERE v = 10 ALLOW FILTERING")


def test_delete_allow_filtering_rejects_if_exists(cql, test_keyspace, scylla_only):
    """Conditional DELETE (IF EXISTS) is not supported with ALLOW FILTERING."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        with pytest.raises(InvalidRequest, match="Conditional DELETE.*IF.*not supported"):
            cql.execute(f"DELETE FROM {table} WHERE v = 10 IF EXISTS ALLOW FILTERING")


def test_delete_allow_filtering_rejects_if_condition(cql, test_keyspace, scylla_only):
    """Conditional DELETE (IF v = ...) is not supported with ALLOW FILTERING."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        with pytest.raises(InvalidRequest, match="Conditional DELETE.*IF.*not supported"):
            cql.execute(f"DELETE FROM {table} WHERE p = 1 IF v = 10 ALLOW FILTERING")


def test_delete_allow_filtering_rejects_pk_or_ck(cql, test_keyspace, scylla_only):
    """OR combining partition key and clustering key is not supported."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        with pytest.raises(SyntaxException):
            cql.execute(f"DELETE FROM {table} WHERE p = 0 OR c = 3 ALLOW FILTERING")


# ----------------------------------------------------------------
# Regular column predicates (read-before-delete)
# ----------------------------------------------------------------

def test_delete_allow_filtering_by_regular_column(cql, test_keyspace, scylla_only):
    """Delete rows where a regular column matches a value."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        for p in range(3):
            for c in range(3):
                cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, {c}, {c})")

        # Delete all rows where v = 1
        cql.execute(f"DELETE FROM {table} WHERE v = 1 ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p, c, v FROM {table}"))
        # v=1 rows were (0,1,1), (1,1,1), (2,1,1) -- now deleted
        expected = sorted([(p, c, c) for p in range(3) for c in range(3) if c != 1])
        assert remaining == expected


def test_delete_allow_filtering_by_regular_column_inequality(cql, test_keyspace, scylla_only):
    """Delete rows where a regular column satisfies an inequality."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        for p in range(3):
            for c in range(3):
                cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, {c}, {p * 10 + c})")

        # Delete all rows where v >= 20 (i.e., p=2: values 20,21,22)
        cql.execute(f"DELETE FROM {table} WHERE v >= 20 ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p, c, v FROM {table}"))
        expected = sorted([(p, c, p * 10 + c) for p in range(3) for c in range(3) if p * 10 + c < 20])
        assert remaining == expected


def test_delete_allow_filtering_no_matching_rows(cql, test_keyspace, scylla_only):
    """Delete with a predicate that matches nothing should be a no-op."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 1, 10)")
        cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (2, 2, 20)")

        cql.execute(f"DELETE FROM {table} WHERE v = 999 ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p, c, v FROM {table}"))
        assert remaining == [(1, 1, 10), (2, 2, 20)]


def test_delete_allow_filtering_all_rows(cql, test_keyspace, scylla_only):
    """Delete all rows by matching a predicate that covers everything."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        for i in range(5):
            cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({i}, 0, 1)")

        cql.execute(f"DELETE FROM {table} WHERE v = 1 ALLOW FILTERING")

        remaining = list(cql.execute(f"SELECT * FROM {table}"))
        assert remaining == []


# ----------------------------------------------------------------
# Partition-key-only predicates
# ----------------------------------------------------------------

def test_delete_allow_filtering_partition_key_range(cql, test_keyspace, scylla_only):
    """Delete partitions where partition key satisfies an inequality."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        for p in range(5):
            for c in range(2):
                cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, {c}, {p})")

        # Delete where p >= 3
        cql.execute(f"DELETE FROM {table} WHERE p >= 3 ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p, c, v FROM {table}"))
        expected = sorted([(p, c, p) for p in range(3) for c in range(2)])
        assert remaining == expected


# ----------------------------------------------------------------
# Clustering column predicates
# ----------------------------------------------------------------

def test_delete_allow_filtering_clustering_range(cql, test_keyspace, scylla_only):
    """Delete rows with clustering key range (no regular column predicate)."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        for p in range(3):
            for c in range(5):
                cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, {c}, {c})")

        # Delete all rows where c >= 3 across all partitions
        cql.execute(f"DELETE FROM {table} WHERE c >= 3 ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p, c, v FROM {table}"))
        expected = sorted([(p, c, c) for p in range(3) for c in range(3)])
        assert remaining == expected


def test_delete_allow_filtering_pk_and_ck_restriction(cql, test_keyspace, scylla_only):
    """Delete with both partition key and clustering key restrictions."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        for p in range(4):
            for c in range(4):
                cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, {c}, {p * 10 + c})")

        # Delete rows where p < 2 AND c > 1
        cql.execute(f"DELETE FROM {table} WHERE p < 2 AND c > 1 ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p, c, v FROM {table}"))
        expected = sorted([(p, c, p * 10 + c) for p in range(4) for c in range(4)
                           if not (p < 2 and c > 1)])
        assert remaining == expected


# ----------------------------------------------------------------
# Mixed predicates: partition key + clustering key + regular column
# ----------------------------------------------------------------

def test_delete_allow_filtering_mixed_predicates(cql, test_keyspace, scylla_only):
    """Delete with partition key, clustering key, and regular
    column restrictions combined."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        for p in range(3):
            for c in range(3):
                cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, {c}, {(p + c) % 3})")

        # Delete where p > 0 AND c < 2 AND v = 1
        cql.execute(f"DELETE FROM {table} WHERE p > 0 AND c < 2 AND v = 1 ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p, c, v FROM {table}"))
        expected = sorted([(p, c, (p + c) % 3)
                           for p in range(3) for c in range(3)
                           if not (p > 0 and c < 2 and (p + c) % 3 == 1)])
        assert remaining == expected


# ----------------------------------------------------------------
# Edge cases
# ----------------------------------------------------------------

def test_delete_allow_filtering_empty_table(cql, test_keyspace, scylla_only):
    """Delete from an empty table should succeed without error."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        cql.execute(f"DELETE FROM {table} WHERE v = 1 ALLOW FILTERING")
        remaining = list(cql.execute(f"SELECT * FROM {table}"))
        assert remaining == []


def test_delete_allow_filtering_single_partition_key(cql, test_keyspace, scylla_only):
    """When partition key IS fully specified, ALLOW FILTERING should still
    work (even if it's not needed for partition lookup)."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        for c in range(5):
            cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, {c}, {c})")

        # p=1 fully specifies partition, but we also filter on v
        cql.execute(f"DELETE FROM {table} WHERE p = 1 AND v >= 3 ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p, c, v FROM {table}"))
        expected = sorted([(1, c, c) for c in range(3)])
        assert remaining == expected


def test_delete_allow_filtering_composite_partition_key(cql, test_keyspace, scylla_only):
    """Test with composite partition key where only part of it is specified."""
    with new_test_table(cql, test_keyspace,
                        "p1 int, p2 int, c int, v int, PRIMARY KEY ((p1, p2), c)") as table:
        for p1 in range(3):
            for p2 in range(3):
                cql.execute(f"INSERT INTO {table} (p1, p2, c, v) VALUES ({p1}, {p2}, 0, {p1 + p2})")

        # Only specify p1, missing p2 -- requires ALLOW FILTERING
        cql.execute(f"DELETE FROM {table} WHERE p1 = 1 ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p1, p2, c, v FROM {table}"))
        expected = sorted([(p1, p2, 0, p1 + p2) for p1 in range(3) for p2 in range(3) if p1 != 1])
        assert remaining == expected


def test_delete_allow_filtering_composite_ck(cql, test_keyspace, scylla_only):
    """Test with composite clustering key."""
    with new_test_table(cql, test_keyspace,
                        "p int, c1 int, c2 int, v int, PRIMARY KEY (p, c1, c2)") as table:
        for p in range(2):
            for c1 in range(3):
                for c2 in range(3):
                    cql.execute(f"INSERT INTO {table} (p, c1, c2, v) VALUES ({p}, {c1}, {c2}, {c1 * 10 + c2})")

        # Delete where c2 = 0 across all partitions (skipping c1)
        cql.execute(f"DELETE FROM {table} WHERE c2 = 0 ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p, c1, c2, v FROM {table}"))
        expected = sorted([(p, c1, c2, c1 * 10 + c2)
                           for p in range(2) for c1 in range(3) for c2 in range(3)
                           if c2 != 0])
        assert remaining == expected


def test_delete_allow_filtering_with_text_column(cql, test_keyspace, scylla_only):
    """Test with text column types."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, name text, PRIMARY KEY (p, c)") as table:
        cql.execute(f"INSERT INTO {table} (p, c, name) VALUES (1, 1, 'alice')")
        cql.execute(f"INSERT INTO {table} (p, c, name) VALUES (1, 2, 'bob')")
        cql.execute(f"INSERT INTO {table} (p, c, name) VALUES (2, 1, 'alice')")
        cql.execute(f"INSERT INTO {table} (p, c, name) VALUES (2, 2, 'charlie')")

        cql.execute(f"DELETE FROM {table} WHERE name = 'alice' ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p, c, name FROM {table}"))
        assert remaining == [(1, 2, 'bob'), (2, 2, 'charlie')]


def test_delete_allow_filtering_with_prepared_statement(cql, test_keyspace, scylla_only):
    """Test using a prepared statement with bind variables."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        for p in range(3):
            cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, 0, {p})")

        stmt = cql.prepare(f"DELETE FROM {table} WHERE v = ? ALLOW FILTERING")
        cql.execute(stmt, [1])

        remaining = sorted(cql.execute(f"SELECT p, c, v FROM {table}"))
        assert remaining == [(0, 0, 0), (2, 0, 2)]


def test_delete_allow_filtering_idempotent(cql, test_keyspace, scylla_only):
    """Running the same DELETE ALLOW FILTERING twice should be idempotent."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (1, 1, 10)")
        cql.execute(f"INSERT INTO {table} (p, c, v) VALUES (2, 2, 20)")

        cql.execute(f"DELETE FROM {table} WHERE v = 10 ALLOW FILTERING")
        cql.execute(f"DELETE FROM {table} WHERE v = 10 ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p, c, v FROM {table}"))
        assert remaining == [(2, 2, 20)]


def test_delete_allow_filtering_partition_key_and_regular(cql, test_keyspace, scylla_only):
    """Delete with both partition key and regular column restrictions."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        for p in range(3):
            for c in range(3):
                cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, {c}, {c * 10})")

        # Delete rows where p >= 1 and v = 20
        cql.execute(f"DELETE FROM {table} WHERE p >= 1 AND v = 20 ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p, c, v FROM {table}"))
        expected = sorted([(p, c, c * 10) for p in range(3) for c in range(3)
                           if not (p >= 1 and c * 10 == 20)])
        assert remaining == expected


def test_delete_allow_filtering_static_column(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace,
                        "p int, c int, s int static, v int, PRIMARY KEY (p, c)") as table:
        for p in range(5):
            cql.execute(f"INSERT INTO {table} (p, s) VALUES ({p}, {p * 10})")
            for c in range(3):
                cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, {c}, {c})")

        cql.execute(f"DELETE FROM {table} WHERE s = 20 ALLOW FILTERING")

        rows = list(cql.execute(f"SELECT p, c, s, v FROM {table} WHERE p = 2"))
        assert len(rows) == 0

        for p in range(5):
            if p == 2:
                continue
            rows = list(cql.execute(f"SELECT p, c, s, v FROM {table} WHERE p = {p}"))
            assert len(rows) == 3


def test_delete_allow_filtering_static_and_regular_column(cql, test_keyspace, scylla_only):
    with new_test_table(cql, test_keyspace,
                        "p int, c int, s int static, v int, PRIMARY KEY (p, c)") as table:
        for p in range(5):
            cql.execute(f"INSERT INTO {table} (p, s) VALUES ({p}, {p * 10})")
            for c in range(3):
                cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, {c}, {c})")

        cql.execute(f"DELETE FROM {table} WHERE s = 20 AND v IN (0, 1, 2) ALLOW FILTERING")

        rows = list(cql.execute(f"SELECT p, c, s, v FROM {table} WHERE p = 2"))
        assert len(rows) == 1
        assert rows[0].c is None and rows[0].v is None
        assert rows[0].s == 20

        for p in range(5):
            if p == 2:
                continue
            rows = list(cql.execute(f"SELECT p, c, s, v FROM {table} WHERE p = {p}"))
            assert len(rows) == 3


# ----------------------------------------------------------------
# IN predicates
# ----------------------------------------------------------------

def test_delete_allow_filtering_pk_in(cql, test_keyspace, scylla_only):
    """Delete rows where partition key is IN a set of values."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        for p in range(5):
            for c in range(3):
                cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, {c}, {p})")

        cql.execute(f"DELETE FROM {table} WHERE p IN (1, 3) ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p, c, v FROM {table}"))
        expected = sorted([(p, c, p) for p in range(5) for c in range(3)
                           if p not in (1, 3)])
        assert remaining == expected


def test_delete_allow_filtering_ck_in(cql, test_keyspace, scylla_only):
    """Delete rows where clustering key is IN a set of values."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        for p in range(3):
            for c in range(5):
                cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, {c}, {c})")

        cql.execute(f"DELETE FROM {table} WHERE c IN (0, 2, 4) ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p, c, v FROM {table}"))
        expected = sorted([(p, c, c) for p in range(3) for c in range(5)
                           if c not in (0, 2, 4)])
        assert remaining == expected


def test_delete_allow_filtering_regular_in(cql, test_keyspace, scylla_only):
    """Delete rows where a regular column is IN a set of values."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        for p in range(3):
            for c in range(4):
                cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, {c}, {c})")

        cql.execute(f"DELETE FROM {table} WHERE v IN (1, 3) ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p, c, v FROM {table}"))
        expected = sorted([(p, c, c) for p in range(3) for c in range(4)
                           if c not in (1, 3)])
        assert remaining == expected


# ----------------------------------------------------------------
# token() predicates
# ----------------------------------------------------------------

def test_delete_allow_filtering_token_range(cql, test_keyspace, scylla_only):
    """Delete by token range restriction."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        for p in range(10):
            cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, 0, {p})")

        all_before = sorted(cql.execute(f"SELECT p, c, v FROM {table}"))

        # Delete using a token range that may cover some partitions
        # We pick an arbitrary range; the exact partitions deleted depend on
        # token ordering. We just verify correctness by checking that after
        # the delete, the remaining rows are a strict subset of the originals
        # and that the deleted rows are in the specified token range.
        # Use a token range that should catch at least some partitions:
        cql.execute(f"DELETE FROM {table} WHERE token(p) < 0 ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p, c, v FROM {table}"))
        assert len(remaining) <= len(all_before)
        # All remaining rows should have token(p) >= 0
        for row in remaining:
            token = list(cql.execute(f"SELECT token(p) FROM {table} WHERE p = {row.p}"))[0][0]
            assert token >= 0


def test_delete_allow_filtering_token_bounded_range(cql, test_keyspace, scylla_only):
    """Delete partitions whose token falls within a bounded range."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        for p in range(20):
            cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, 0, {p})")

        # Retrieve the actual token ordering so we can pick a meaningful range
        token_rows = list(cql.execute(f"SELECT token(p), p FROM {table}"))
        token_rows.sort()
        # Pick a range that covers the middle third of partitions
        lo = token_rows[len(token_rows) // 3][0]
        hi = token_rows[2 * len(token_rows) // 3][0]
        expected_deleted = {r[1] for r in token_rows if lo <= r[0] < hi}

        all_before = {row.p for row in cql.execute(f"SELECT p FROM {table}")}

        cql.execute(f"DELETE FROM {table} WHERE token(p) >= {lo} AND token(p) < {hi} ALLOW FILTERING")

        remaining = {row.p for row in cql.execute(f"SELECT p FROM {table}")}
        assert remaining == all_before - expected_deleted


def test_delete_allow_filtering_token_composite_pk(cql, test_keyspace, scylla_only):
    """Delete by token range on a composite partition key."""
    with new_test_table(cql, test_keyspace,
                        "p1 int, p2 int, c int, v int, PRIMARY KEY ((p1, p2), c)") as table:
        for p1 in range(4):
            for p2 in range(4):
                cql.execute(f"INSERT INTO {table} (p1, p2, c, v) VALUES ({p1}, {p2}, 0, {p1 + p2})")

        token_rows = list(cql.execute(f"SELECT token(p1, p2), p1, p2 FROM {table}"))
        token_rows.sort()
        # Delete bottom half by token
        mid = token_rows[len(token_rows) // 2][0]
        expected_deleted = {(r[1], r[2]) for r in token_rows if r[0] < mid}

        cql.execute(f"DELETE FROM {table} WHERE token(p1, p2) < {mid} ALLOW FILTERING")

        remaining = {(row.p1, row.p2) for row in cql.execute(f"SELECT p1, p2 FROM {table}")}
        all_pks = {(p1, p2) for p1 in range(4) for p2 in range(4)}
        assert remaining == all_pks - expected_deleted


def test_delete_allow_filtering_token_with_clustering(cql, test_keyspace, scylla_only):
    """Delete by combining a token range with a clustering key restriction."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        for p in range(10):
            for c in range(4):
                cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, {c}, {p * 10 + c})")

        token_rows = list(cql.execute(f"SELECT token(p), p FROM {table}"))
        # Get unique (token, p) pairs
        token_map = {}
        for tok, p in token_rows:
            token_map[p] = tok
        sorted_tokens = sorted(token_map.items(), key=lambda x: x[1])
        # Pick a midpoint token
        mid = sorted_tokens[len(sorted_tokens) // 2][1]
        partitions_in_range = {p for p, tok in token_map.items() if tok >= mid}

        # Delete rows where token(p) >= mid AND c < 2
        cql.execute(f"DELETE FROM {table} WHERE token(p) >= {mid} AND c < 2 ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p, c, v FROM {table}"))
        expected = sorted([(p, c, p * 10 + c)
                           for p in range(10) for c in range(4)
                           if not (p in partitions_in_range and c < 2)])
        assert remaining == expected


def test_delete_allow_filtering_token_with_regular_column(cql, test_keyspace, scylla_only):
    """Delete by combining a token range with a regular column restriction."""
    with new_test_table(cql, test_keyspace,
                        "p int, c int, v int, PRIMARY KEY (p, c)") as table:
        for p in range(10):
            for c in range(3):
                cql.execute(f"INSERT INTO {table} (p, c, v) VALUES ({p}, {c}, {c})")

        token_rows = list(cql.execute(f"SELECT token(p), p FROM {table}"))
        token_map = {}
        for tok, p in token_rows:
            token_map[p] = tok
        sorted_tokens = sorted(token_map.items(), key=lambda x: x[1])
        mid = sorted_tokens[len(sorted_tokens) // 2][1]
        partitions_in_range = {p for p, tok in token_map.items() if tok < mid}

        # Delete rows where token(p) < mid AND v = 1
        cql.execute(f"DELETE FROM {table} WHERE token(p) < {mid} AND v = 1 ALLOW FILTERING")

        remaining = sorted(cql.execute(f"SELECT p, c, v FROM {table}"))
        expected = sorted([(p, c, c)
                           for p in range(10) for c in range(3)
                           if not (p in partitions_in_range and c == 1)])
        assert remaining == expected

