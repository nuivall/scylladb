/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "dht/i_partitioner_fwd.hh"

#include "idl/uuid.idl.hh"
#include "idl/consistency_level.idl.hh"

namespace query {
struct filtering_delete_request {
    enum class tier : uint8_t {
        partition_only,
        clustering,
        regular_column
    };

    table_id schema_id;
    table_schema_version schema_version;
    sstring where_clause;
    dht::partition_range_vector pr;

    db::consistency_level cl;
    lowres_system_clock::time_point timeout;
    api::timestamp_type timestamp;
    query::filtering_delete_request::tier optimization_tier;
    std::optional<shard_id> shard_id_hint;
};

struct filtering_delete_result {
    uint64_t rows_deleted;
};

verb [[cancellable]] filtering_delete_request(query::filtering_delete_request req [[ref]], std::optional<tracing::trace_info> trace_info [[ref]]) -> query::filtering_delete_result;
}
