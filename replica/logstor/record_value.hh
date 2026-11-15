/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#pragma once

#include "replica/logstor/types.hh"
#include "schema/schema_fwd.hh"

class frozen_mutation;
class mutation;

namespace replica::logstor {

struct single_row_partition;

// The functions that know what the bytes of a record_value mean. Everything else in logstor
// treats a value as bytes, so changing the encoding is a change to this file alone.
//
// The value holds the partition of the mutation and nothing the record_header already
// carries, which is why decoding takes the header: the key and the table of the record come
// from it. The layout of the value is described in replica/logstor/record_value.cc, next to
// the encoder.

// Returns the timestamp of the record that stores m: the timestamp of the row marker, or of the
// partition tombstone if there is no marker. Throws if m has neither. The two overloads must give
// the same result.
api::timestamp_type record_timestamp(const mutation& m);
api::timestamp_type record_timestamp(const frozen_mutation& m);

// Encodes the partition of m as the value of a record whose header is built from m.
record_value encode_record_value(const mutation& m);

// Encodes the partition of m as the value of a record whose header is built from m. s is the
// schema that m was frozen with.
record_value encode_record_value(const frozen_mutation& m, const schema& s);

// TEST-ONLY candidate fix: a key that orders two records of the same timestamp `ts` as Scylla's
// reconciliation orders their contents, as far as 32 bits allow; the larger key wins. A record
// whose partition or row is deleted at `ts` or later (a partition tombstone, a row tombstone or a
// dead row marker) gets the largest key, since a deletion wins over a live write of the same
// timestamp. Any other record gets the first 4 bytes, big-endian and zero-padded, of the value of
// its first atomic cell (by column id), capped one below the largest key: between two live cells
// of the same timestamp the larger value wins (compare_atomic_cell_for_merge). The value is read
// without a schema, from the column ids of the encoded partition, so the write path and recovery
// compute the same key.
uint32_t record_tiebreak(const record_value& v, api::timestamp_type ts);
uint32_t record_tiebreak(bytes_view v, api::timestamp_type ts);

// Decodes the value of a record with header h as a partition of the schema s.
single_row_partition decode_record_value(const record_value& v, const schema& s, const record_header& h);

} // namespace replica::logstor
