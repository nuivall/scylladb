/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "schema/schema_fwd.hh"
#include "replica/database_fwd.hh"
#include "bytes_ostream.hh"
#include <iosfwd>

// Immutable mutation form which can be read using any schema version of the same table.
// Safe to access from other shards via const&.
// Safe to pass serialized across nodes.
class canonical_mutation {
    bytes_ostream _data;
public:
    explicit canonical_mutation(bytes_ostream);
    explicit canonical_mutation(const mutation&);

    canonical_mutation(canonical_mutation&&) = default;
    canonical_mutation(const canonical_mutation&) = default;
    canonical_mutation& operator=(const canonical_mutation&) = default;
    canonical_mutation& operator=(canonical_mutation&&) = default;

    // Create a mutation object interpreting this canonical mutation using
    // given schema.
    //
    // Data which is not representable in the target schema is dropped. If this
    // is not intended, user should sync the schema first.
    //
    // Use ignore_cf_id_mismatch when in rare cases mutation from one table
    // should be deserialized as mutation of another table (useful for data copy).
    using ignore_cf_id_mismatch = bool_class<class ignore_cf_id_mismatch_tag>;
    mutation to_mutation(schema_ptr, ignore_cf_id_mismatch = ignore_cf_id_mismatch::no) const;

    table_id column_family_id() const;

    const bytes_ostream& representation() const { return _data; }

    friend std::ostream& operator<<(std::ostream& os, const canonical_mutation& cm);
};
